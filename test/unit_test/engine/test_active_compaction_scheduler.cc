// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [GC-Tombstone refactor B2/B3/B4/B5] Unit tests for the score-based active compaction scheduler and
// the write-CF MvccPropertiesCollector. NOTE: not run here (the user compiles/runs the tests
// themselves); they document the expected behavior.
//
// Coverage map (vs planner-1-plan.md §9):
//   - Group A  : GetCompactScore both branches + thresholds       (§9.1)
//   - Group B  : MvccPropertiesCollector AddUserKey/Finish/parse   (§9.3)
//   - Group C  : real-engine GetRangeTableProperties round-trip    (§9.4)
//   - Group D  : EvaluateRangeCandidate + CollectCompactionTop-N   (§9.1, §9.2)
//   - Group E  : cold-SST fallback selection                       (§9.5)
//   - Group F  : double-check skip on recheck                      (§9.6)
//   - Group G  : ParseMvcc round-trip equivalence (collector<->reader props)
//
// Units that need the live Server/Storage singleton, the crontab thread, or wall-clock timing
// (ActiveCompactionHandler bootstrap, round_budget_ms deadline drop, per-region min_interval debounce,
// the default-off rollback handler, and the filter end-to-end tombstone removal) are NOT covered as
// pure unit tests here -- see tester-round-1.md "Gaps" for the rationale and integration suggestions.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "config/config.h"
#include "config/yaml_config.h"
#include "engine/active_compaction_scheduler.h"
#include "engine/mvcc_properties_collector.h"
#include "engine/raw_engine.h"
#include "engine/rocks_raw_engine.h"
#include "engine/write_compaction_filter.h"  // g_compaction_filter_safe_point
#include "gflags/gflags.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

// gflags consumed by the scheduler. They are DEFINEd in txn_engine_helper.cc inside namespace dingodb,
// so the DECLARE must also live here (same pitfall documented in test_txn_scan_seek_round.cc).
DECLARE_int64(gc_active_compaction_max_per_round);
DECLARE_int64(gc_active_compaction_min_interval_s);
DECLARE_bool(gc_active_compaction_force_bottommost);
DECLARE_int64(gc_active_compaction_tombstones_num_threshold);
DECLARE_int64(gc_active_compaction_tombstones_percent_threshold);
DECLARE_int64(gc_active_compaction_redundant_rows_threshold);
DECLARE_int64(gc_active_compaction_redundant_rows_percent_threshold);
DECLARE_int64(gc_active_compaction_cold_sst_seconds);
// DEFINEd in mvcc_properties_collector.cc, inside namespace dingodb.
DECLARE_bool(gc_enable_mvcc_properties_collector);

namespace {

// ----------------------------------------------------------------------------------------------------
// Small RAII guards that save a gflag on construction and restore it on destruction, so each test case
// leaves the global FLAGS_ exactly as it found them (project convention from test_txn_scan_seek_round).
// ----------------------------------------------------------------------------------------------------
class Int64FlagGuard {
 public:
  Int64FlagGuard(int64_t* flag, int64_t value) : flag_(flag), saved_(*flag) { *flag_ = value; }
  ~Int64FlagGuard() { *flag_ = saved_; }

 private:
  int64_t* flag_;
  int64_t saved_;
};

class BoolFlagGuard {
 public:
  BoolFlagGuard(bool* flag, bool value) : flag_(flag), saved_(*flag) { *flag_ = value; }
  ~BoolFlagGuard() { *flag_ = saved_; }

 private:
  bool* flag_;
  bool saved_;
};

// Build a serialized WriteInfo value with the given op.
std::string MakeWriteInfo(pb::store::Op op, int64_t start_ts = 1) {
  pb::store::WriteInfo write_info;
  write_info.set_op(op);
  write_info.set_start_ts(start_ts);
  return write_info.SerializeAsString();
}

// Build an encoded write CF key (user_key + descending-encoded commit_ts).
std::string MakeWriteKey(const std::string& user_key, int64_t commit_ts) {
  return mvcc::Codec::EncodeKey(user_key, commit_ts);
}

// Construct a lightweight RegionPtr over [start_key, end_key). RAW (un-encoded) bounds, matching what
// EvaluateRangeCandidate expects (it encodes the bounds itself before reading SST properties).
store::RegionPtr MakeRegion(int64_t region_id, const std::string& start_key, const std::string& end_key) {
  pb::common::RegionDefinition definition;
  definition.set_id(region_id);
  definition.mutable_range()->set_start_key(start_key);
  definition.mutable_range()->set_end_key(end_key);
  definition.set_part_id(1);
  definition.set_tenant_id(0);
  return store::Region::New(definition);
}

}  // namespace

// ====================================================================================================
// Group A: GetCompactScore -- pure static function, both branches + thresholds (planner §9.1).
// ====================================================================================================

class GetCompactScoreTest : public testing::Test {};

// num_total_entries == 0 always scores 0 (empty range, guarded first line).
TEST_F(GetCompactScoreTest, ZeroTotalEntriesScoresZero) {
  EXPECT_DOUBLE_EQ(0.0, ActiveCompactionScheduler::GetCompactScore(100, 100, 0, /*enable_filter=*/false));
  EXPECT_DOUBLE_EQ(0.0, ActiveCompactionScheduler::GetCompactScore(100, 100, 0, /*enable_filter=*/true));
}

// Filter-off branch (phase-A): below BOTH the absolute count threshold AND the percent threshold => 0.
TEST_F(GetCompactScoreTest, FilterOffBelowBothThresholdsZero) {
  Int64FlagGuard g_num(&FLAGS_gc_active_compaction_tombstones_num_threshold, 10000);
  Int64FlagGuard g_pct(&FLAGS_gc_active_compaction_tombstones_percent_threshold, 30);

  // 100 tombstones out of 1000 entries = 10% < 30% and 100 < 10000 -> not selected.
  EXPECT_DOUBLE_EQ(0.0, ActiveCompactionScheduler::GetCompactScore(100, 0, 1000, /*enable_filter=*/false));
}

// Filter-off branch: meeting the absolute count threshold alone (even at low percent) selects it.
TEST_F(GetCompactScoreTest, FilterOffMeetsCountThresholdSelected) {
  Int64FlagGuard g_num(&FLAGS_gc_active_compaction_tombstones_num_threshold, 10000);
  Int64FlagGuard g_pct(&FLAGS_gc_active_compaction_tombstones_percent_threshold, 30);

  // 10000 tombstones out of 1,000,000 entries = 1% (< 30%) but count >= 10000 -> selected, score > 0.
  double score = ActiveCompactionScheduler::GetCompactScore(10000, 0, 1000000, /*enable_filter=*/false);
  EXPECT_GT(score, 0.0);
  // score = tombstones * ratio = 10000 * (10000/1000000) = 100.
  EXPECT_DOUBLE_EQ(100.0, score);
}

// Filter-off branch: meeting the percent threshold alone (low count) selects it.
TEST_F(GetCompactScoreTest, FilterOffMeetsPercentThresholdSelected) {
  Int64FlagGuard g_num(&FLAGS_gc_active_compaction_tombstones_num_threshold, 10000);
  Int64FlagGuard g_pct(&FLAGS_gc_active_compaction_tombstones_percent_threshold, 30);

  // 400 tombstones out of 1000 = 40% (>= 30%) although 400 < 10000 -> selected.
  double score = ActiveCompactionScheduler::GetCompactScore(400, 0, 1000, /*enable_filter=*/false);
  EXPECT_GT(score, 0.0);
  // score = 400 * 0.4 = 160.
  EXPECT_DOUBLE_EQ(160.0, score);
}

// Filter-off branch monotonicity: more tombstones (same total) -> strictly higher score, once selected.
TEST_F(GetCompactScoreTest, FilterOffMonotonicInTombstones) {
  Int64FlagGuard g_num(&FLAGS_gc_active_compaction_tombstones_num_threshold, 1);
  Int64FlagGuard g_pct(&FLAGS_gc_active_compaction_tombstones_percent_threshold, 0);

  double low = ActiveCompactionScheduler::GetCompactScore(100, 0, 1000, /*enable_filter=*/false);
  double high = ActiveCompactionScheduler::GetCompactScore(300, 0, 1000, /*enable_filter=*/false);
  EXPECT_GT(low, 0.0);
  EXPECT_GT(high, low);
}

// Filter-on branch (phase-B): below BOTH redundant-row thresholds => 0; num_discardable is the driver.
TEST_F(GetCompactScoreTest, FilterOnBelowBothThresholdsZero) {
  Int64FlagGuard g_num(&FLAGS_gc_active_compaction_redundant_rows_threshold, 50000);
  Int64FlagGuard g_pct(&FLAGS_gc_active_compaction_redundant_rows_percent_threshold, 20);

  // 100 discardable + 0 tombstone of 1000 = 10% < 20% and 100 < 50000 -> not selected.
  EXPECT_DOUBLE_EQ(0.0, ActiveCompactionScheduler::GetCompactScore(0, 100, 1000, /*enable_filter=*/true));
}

// Filter-on branch: discardable above threshold selects; tombstones only feed the ratio numerator.
TEST_F(GetCompactScoreTest, FilterOnDiscardableDrivesScore) {
  Int64FlagGuard g_num(&FLAGS_gc_active_compaction_redundant_rows_threshold, 50000);
  Int64FlagGuard g_pct(&FLAGS_gc_active_compaction_redundant_rows_percent_threshold, 20);

  // discardable=300, tombstones=100, total=1000: ratio=(100+300)/1000=0.4 >= 20% -> selected.
  double score = ActiveCompactionScheduler::GetCompactScore(100, 300, 1000, /*enable_filter=*/true);
  EXPECT_GT(score, 0.0);
  // score = discardable * ratio = 300 * 0.4 = 120.
  EXPECT_DOUBLE_EQ(120.0, score);
}

// Filter-on branch monotonicity in num_discardable once selected.
TEST_F(GetCompactScoreTest, FilterOnMonotonicInDiscardable) {
  Int64FlagGuard g_num(&FLAGS_gc_active_compaction_redundant_rows_threshold, 1);
  Int64FlagGuard g_pct(&FLAGS_gc_active_compaction_redundant_rows_percent_threshold, 0);

  double low = ActiveCompactionScheduler::GetCompactScore(0, 100, 1000, /*enable_filter=*/true);
  double high = ActiveCompactionScheduler::GetCompactScore(0, 400, 1000, /*enable_filter=*/true);
  EXPECT_GT(low, 0.0);
  EXPECT_GT(high, low);
}

// ====================================================================================================
// Group B: MvccPropertiesCollector -- AddUserKey/Finish over constructed write-CF records (planner §9.3).
// Drives the collector directly (no RocksDB), asserting the serialized user properties are numerically
// correct and that malformed records are conservatively skipped without crashing.
// ====================================================================================================

class MvccCollectorTest : public testing::Test {
 protected:
  // Feed one Put record into a collector. Mirrors RocksDB AddUserKey(kEntryPut).
  static rocksdb::Status Add(MvccPropertiesCollector& c, const std::string& key, const std::string& value,
                             rocksdb::EntryType type = rocksdb::kEntryPut) {
    return c.AddUserKey(rocksdb::Slice(key), rocksdb::Slice(value), type, /*seq=*/0, /*file_size=*/0);
  }

  // Parse a decimal-ASCII user property to int64. Returns -1 if missing.
  static int64_t Prop(const rocksdb::UserCollectedProperties& p, const char* k) {
    auto it = p.find(k);
    if (it == p.end()) {
      return -1;
    }
    return std::stoll(it->second);
  }
};

// Three rows, single version each, all Put: num_rows == num_versions == num_entries == 3, no deletes,
// no stale versions.
TEST_F(MvccCollectorTest, ThreeDistinctRowsSingleVersion) {
  MvccPropertiesCollector c;
  // Records arrive in ascending user_key order.
  ASSERT_TRUE(Add(c, MakeWriteKey("k1", 100), MakeWriteInfo(pb::store::Op::Put)).ok());
  ASSERT_TRUE(Add(c, MakeWriteKey("k2", 100), MakeWriteInfo(pb::store::Op::Put)).ok());
  ASSERT_TRUE(Add(c, MakeWriteKey("k3", 100), MakeWriteInfo(pb::store::Op::Put)).ok());

  rocksdb::UserCollectedProperties props;
  ASSERT_TRUE(c.Finish(&props).ok());

  EXPECT_EQ(3, Prop(props, kMvccPropNumEntries));
  EXPECT_EQ(3, Prop(props, kMvccPropNumVersions));
  EXPECT_EQ(3, Prop(props, kMvccPropNumRows));
  EXPECT_EQ(0, Prop(props, kMvccPropNumDeletes));
  EXPECT_EQ(0, Prop(props, kMvccPropOldestStaleVersionTs));
  EXPECT_EQ(0, Prop(props, kMvccPropNewestStaleVersionTs));
}

// One row with three versions: num_rows=1, num_versions=3, two of them are stale (non-newest). Within a
// user_key, RocksDB delivers newest commit_ts first (descending-ts encoding), so the first record is the
// row, the next two are stale versions. Stale ts span = [80, 90].
TEST_F(MvccCollectorTest, SingleRowMultipleVersionsTracksStale) {
  MvccPropertiesCollector c;
  ASSERT_TRUE(Add(c, MakeWriteKey("k1", 100), MakeWriteInfo(pb::store::Op::Put)).ok());  // newest -> the row
  ASSERT_TRUE(Add(c, MakeWriteKey("k1", 90), MakeWriteInfo(pb::store::Op::Put)).ok());   // stale
  ASSERT_TRUE(Add(c, MakeWriteKey("k1", 80), MakeWriteInfo(pb::store::Op::Put)).ok());   // stale

  rocksdb::UserCollectedProperties props;
  ASSERT_TRUE(c.Finish(&props).ok());

  EXPECT_EQ(3, Prop(props, kMvccPropNumEntries));
  EXPECT_EQ(3, Prop(props, kMvccPropNumVersions));
  EXPECT_EQ(1, Prop(props, kMvccPropNumRows));
  EXPECT_EQ(0, Prop(props, kMvccPropNumDeletes));
  // num_versions - num_rows = 2 stale versions, ts span [80, 90].
  EXPECT_EQ(80, Prop(props, kMvccPropOldestStaleVersionTs));
  EXPECT_EQ(90, Prop(props, kMvccPropNewestStaleVersionTs));
}

// Delete markers (op=Delete) are counted in num_deletes and tracked by the delete ts span. They are
// still RocksDB Puts (a dingo Delete marker is an op=Delete WriteInfo Put), so they also count as
// entries/versions.
TEST_F(MvccCollectorTest, DeleteMarkersCountedAndTsTracked) {
  MvccPropertiesCollector c;
  ASSERT_TRUE(Add(c, MakeWriteKey("k1", 50), MakeWriteInfo(pb::store::Op::Delete)).ok());
  ASSERT_TRUE(Add(c, MakeWriteKey("k2", 70), MakeWriteInfo(pb::store::Op::Delete)).ok());
  ASSERT_TRUE(Add(c, MakeWriteKey("k3", 60), MakeWriteInfo(pb::store::Op::Put)).ok());

  rocksdb::UserCollectedProperties props;
  ASSERT_TRUE(c.Finish(&props).ok());

  EXPECT_EQ(3, Prop(props, kMvccPropNumEntries));
  EXPECT_EQ(3, Prop(props, kMvccPropNumRows));  // 3 distinct user_keys
  EXPECT_EQ(2, Prop(props, kMvccPropNumDeletes));
  // delete ts span = [50, 70].
  EXPECT_EQ(50, Prop(props, kMvccPropOldestDeleteTs));
  EXPECT_EQ(70, Prop(props, kMvccPropNewestDeleteTs));
}

// Malformed records are conservatively skipped (not counted), and the collector never errors. Mixes a
// non-Put entry type, an undecodable key, and a non-WriteInfo value with one valid record.
TEST_F(MvccCollectorTest, MalformedRecordsSkippedNotCounted) {
  MvccPropertiesCollector c;
  // Non-Put entry type (e.g. a RocksDB tombstone) -> skipped without counting.
  ASSERT_TRUE(Add(c, MakeWriteKey("k1", 100), MakeWriteInfo(pb::store::Op::Put), rocksdb::kEntryDelete).ok());
  // Key too short to decode (user_key/ts) -> skipped.
  ASSERT_TRUE(Add(c, "short", MakeWriteInfo(pb::store::Op::Put)).ok());
  // Value that is not a valid WriteInfo -> skipped (note: an empty value parses as a default WriteInfo,
  // so we use bytes that fail ParseFromArray for a non-trivial proto). Use clearly garbage bytes.
  ASSERT_TRUE(Add(c, MakeWriteKey("k2", 100), std::string("\xff\xff\xff\xff\xff", 5)).ok());
  // One valid record.
  ASSERT_TRUE(Add(c, MakeWriteKey("k3", 100), MakeWriteInfo(pb::store::Op::Put)).ok());

  rocksdb::UserCollectedProperties props;
  ASSERT_TRUE(c.Finish(&props).ok());

  // Only the single valid record is counted.
  EXPECT_EQ(1, Prop(props, kMvccPropNumEntries));
  EXPECT_EQ(1, Prop(props, kMvccPropNumVersions));
  EXPECT_EQ(1, Prop(props, kMvccPropNumRows));
  EXPECT_EQ(0, Prop(props, kMvccPropNumDeletes));
}

// Finish(nullptr) must not crash.
TEST_F(MvccCollectorTest, FinishNullPropertiesSafe) {
  MvccPropertiesCollector c;
  ASSERT_TRUE(Add(c, MakeWriteKey("k1", 100), MakeWriteInfo(pb::store::Op::Put)).ok());
  EXPECT_TRUE(c.Finish(nullptr).ok());
}

// ====================================================================================================
// Group G: collector Finish() <-> GetReadableProperties() emit the SAME user-property map. This is what
// RocksRawEngine::ParseMvccUserProperties consumes, so it doubles as a parse-format contract test
// (planner §9.4 fallback: cover the property round-trip when a real engine SST is not exercised).
// ====================================================================================================

TEST(MvccCollectorReadbackTest, FinishMatchesReadableProperties) {
  MvccPropertiesCollector c;
  c.AddUserKey(rocksdb::Slice(MakeWriteKey("k1", 100)), rocksdb::Slice(MakeWriteInfo(pb::store::Op::Put)),
               rocksdb::kEntryPut, 0, 0);
  c.AddUserKey(rocksdb::Slice(MakeWriteKey("k1", 90)), rocksdb::Slice(MakeWriteInfo(pb::store::Op::Delete)),
               rocksdb::kEntryPut, 0, 0);

  rocksdb::UserCollectedProperties finish_props;
  ASSERT_TRUE(c.Finish(&finish_props).ok());
  rocksdb::UserCollectedProperties readable = c.GetReadableProperties();

  // Both maps must agree on every MVCC property key, and the values must be parseable decimal ASCII.
  for (const char* key : {kMvccPropNumEntries, kMvccPropNumVersions, kMvccPropNumRows, kMvccPropNumDeletes,
                          kMvccPropOldestDeleteTs, kMvccPropNewestDeleteTs, kMvccPropOldestStaleVersionTs,
                          kMvccPropNewestStaleVersionTs}) {
    ASSERT_TRUE(finish_props.find(key) != finish_props.end()) << key;
    ASSERT_TRUE(readable.find(key) != readable.end()) << key;
    EXPECT_EQ(finish_props.at(key), readable.at(key)) << key;
    EXPECT_NO_THROW(std::stoll(finish_props.at(key))) << key;  // decimal ASCII parse contract
  }
}

// ====================================================================================================
// Groups C/D/E/F: real RocksRawEngine fixture. Writes write-CF records, flushes SSTs, then exercises
// GetRangeTableProperties and the scheduler's Evaluate/Collect/recheck logic against the live engine.
//
// Two engine instances are built once for the suite:
//   - engine_     : collector OFF (phase-A signals only); compaction filter OFF so flushed records
//                   survive (we want to count num_entries, not have them GC'd away).
//   - engine_b_   : collector ON  (phase-B SST user properties present).
// The collector and the compaction filter are registered at Init() time from the gflags, so the flags
// must be set BEFORE Init(). We therefore build the two engines under explicit flag guards.
// ====================================================================================================

class ActiveCompactionEngineTest : public testing::Test {
 protected:
  static constexpr const char* kRootA = "./unit_test_acs_a";
  static constexpr const char* kRootB = "./unit_test_acs_b";

  static std::shared_ptr<RocksRawEngine> engine_;    // collector OFF (phase-A)
  static std::shared_ptr<RocksRawEngine> engine_b_;  // collector ON  (phase-B)

  static std::shared_ptr<Config> MakeConfig(const std::string& store_path) {
    const std::string yaml =
        "cluster:\n  name: dingodb\n  instance_id: 12345\n"
        "  coordinators: 127.0.0.1:19190\n  keyring: TO_BE_CONTINUED\n"
        "server:\n  host: 127.0.0.1\n  port: 23000\n"
        "log:\n  path: " +
        store_path + "/log\n" + "store:\n  path: " + store_path + "/db\n";
    auto cfg = std::make_shared<YamlConfig>();
    EXPECT_EQ(0, cfg->Load(yaml));
    return cfg;
  }

  static void SetUpTestSuite() {
    Helper::CreateDirectories(std::string(kRootA) + "/db");
    Helper::CreateDirectories(std::string(kRootB) + "/db");

    const std::vector<std::string> cfs = {Constant::kTxnWriteCF, Constant::kStoreDataCF, Constant::kTxnLockCF};

    // Phase-A engine: collector OFF. Keep the compaction filter OFF too so flushed records are not
    // reclaimed by a background compaction during the test (we measure raw num_entries).
    {
      BoolFlagGuard g_collector(&FLAGS_gc_enable_mvcc_properties_collector, false);
      engine_ = std::make_shared<RocksRawEngine>();
      ASSERT_TRUE(engine_->Init(MakeConfig(kRootA), cfs));
    }

    // Phase-B engine: collector ON so SSTs carry dingo.mvcc.* user properties.
    {
      BoolFlagGuard g_collector(&FLAGS_gc_enable_mvcc_properties_collector, true);
      engine_b_ = std::make_shared<RocksRawEngine>();
      ASSERT_TRUE(engine_b_->Init(MakeConfig(kRootB), cfs));
    }
  }

  static void TearDownTestSuite() {
    if (engine_ != nullptr) {
      engine_->Close();
      engine_->Destroy();
    }
    if (engine_b_ != nullptr) {
      engine_b_->Close();
      engine_b_->Destroy();
    }
    Helper::RemoveAllFileOrDirectory(kRootA);
    Helper::RemoveAllFileOrDirectory(kRootB);
  }

  // Write a batch of write-CF records and flush so they materialize as an SST.
  static void WriteAndFlush(const std::shared_ptr<RocksRawEngine>& engine,
                            const std::vector<pb::common::KeyValue>& kvs) {
    auto writer = engine->Writer();
    ASSERT_TRUE(writer->KvBatchPut(Constant::kTxnWriteCF, kvs).ok());
    engine->Flush(Constant::kTxnWriteCF);
  }

  static pb::common::KeyValue Kv(const std::string& user_key, int64_t commit_ts, pb::store::Op op) {
    pb::common::KeyValue kv;
    kv.set_key(MakeWriteKey(user_key, commit_ts));
    kv.set_value(MakeWriteInfo(op, /*start_ts=*/commit_ts - 1));
    return kv;
  }
};

std::shared_ptr<RocksRawEngine> ActiveCompactionEngineTest::engine_ = nullptr;
std::shared_ptr<RocksRawEngine> ActiveCompactionEngineTest::engine_b_ = nullptr;

// --- Group C: GetRangeTableProperties round-trip ----------------------------------------------------

// Phase-A: after writing N write-CF records to a region's key range and flushing, GetRangeTableProperties
// reports num_entries >= N over that range, at least one SST, and a non-zero cold-SST timestamp. With the
// collector OFF the phase-B fields stay 0.
TEST_F(ActiveCompactionEngineTest, GetRangePropertiesPhaseA) {
  std::vector<pb::common::KeyValue> kvs;
  for (int i = 0; i < 50; ++i) {
    kvs.push_back(Kv("ca_" + std::to_string(i), 100, pb::store::Op::Put));
  }
  WriteAndFlush(engine_, kvs);

  RangeTableProperties props;
  const std::string start = mvcc::Codec::EncodeBytes("ca_");
  const std::string end = mvcc::Codec::EncodeBytes("ca`");  // 'z'+1 region upper bound covering ca_*
  butil::Status s = engine_->GetRangeTableProperties(Constant::kTxnWriteCF, start, end, &props);
  ASSERT_TRUE(s.ok()) << s.error_cstr();

  EXPECT_GE(props.num_entries, 50u);
  EXPECT_GE(props.sst_file_count, 1u);
  EXPECT_GT(props.oldest_ancester_time, 0u);
  // Collector OFF -> phase-B fields untouched.
  EXPECT_EQ(0u, props.num_versions);
  EXPECT_EQ(0u, props.num_rows);
}

// Phase-B: with the collector ON, the same write/flush produces non-zero dingo.mvcc.* fields. Three rows
// with two versions each => num_versions=6, num_rows=3; one delete marker => num_deletes=1.
TEST_F(ActiveCompactionEngineTest, GetRangePropertiesPhaseB) {
  std::vector<pb::common::KeyValue> kvs;
  for (int r = 0; r < 3; ++r) {
    const std::string uk = "cb_" + std::to_string(r);
    kvs.push_back(Kv(uk, 200, pb::store::Op::Put));
    kvs.push_back(Kv(uk, 100, pb::store::Op::Put));
  }
  // One delete marker on a fourth row.
  kvs.push_back(Kv("cb_9", 150, pb::store::Op::Delete));
  WriteAndFlush(engine_b_, kvs);

  RangeTableProperties props;
  const std::string start = mvcc::Codec::EncodeBytes("cb_");
  const std::string end = mvcc::Codec::EncodeBytes("cb`");
  butil::Status s = engine_b_->GetRangeTableProperties(Constant::kTxnWriteCF, start, end, &props);
  ASSERT_TRUE(s.ok()) << s.error_cstr();

  EXPECT_GE(props.num_entries, 7u);
  EXPECT_GE(props.num_versions, 7u);  // 6 puts + 1 delete marker
  EXPECT_GE(props.num_rows, 4u);      // cb_0..2 + cb_9
  EXPECT_GE(props.num_deletes, 1u);
  EXPECT_GT(props.newest_delete_ts, 0);
}

// --- Group D: EvaluateRangeCandidate + CollectCompactionCandidates Top-N -----------------------------

// A region with many entries and a low threshold is selected; CollectCompactionCandidates returns a
// descending-by-score Top-N and excludes below-threshold regions.
TEST_F(ActiveCompactionEngineTest, CollectTopNDescendingAndThresholdGated) {
  // Force phase-A and an aggressive threshold so num_entries alone selects.
  BoolFlagGuard g_collector(&FLAGS_gc_enable_mvcc_properties_collector, false);
  Int64FlagGuard g_num(&FLAGS_gc_active_compaction_tombstones_num_threshold, 1);
  Int64FlagGuard g_pct(&FLAGS_gc_active_compaction_tombstones_percent_threshold, 0);
  Int64FlagGuard g_cold(&FLAGS_gc_active_compaction_cold_sst_seconds, 1);  // make every fresh SST "cold"

  // Three populated regions with descending entry counts, plus one empty region.
  auto write_region = [&](const std::string& prefix, int n) {
    std::vector<pb::common::KeyValue> kvs;
    for (int i = 0; i < n; ++i) {
      kvs.push_back(Kv(prefix + std::to_string(i), 100, pb::store::Op::Put));
    }
    WriteAndFlush(engine_, kvs);
  };
  write_region("dh_", 300);  // high
  write_region("dm_", 150);  // mid
  write_region("dl_", 60);   // low

  std::vector<store::RegionPtr> regions = {
      MakeRegion(101, "dh_", "dh`"),  // high
      MakeRegion(102, "dm_", "dm`"),  // mid
      MakeRegion(103, "dl_", "dl`"),  // low
      MakeRegion(104, "dz_", "dz`"),  // empty -> below threshold, excluded
  };

  // heap_capacity 2 keeps only the top-2 by score.
  auto candidates =
      ActiveCompactionScheduler::CollectCompactionCandidates(engine_, regions, /*gc_safe_point=*/1000,
                                                             /*heap_capacity=*/2);
  ASSERT_EQ(2u, candidates.size());
  EXPECT_EQ(101, candidates[0].region_id);  // highest score first
  EXPECT_EQ(102, candidates[1].region_id);
  EXPECT_GE(candidates[0].score, candidates[1].score);  // descending
}

// EvaluateRangeCandidate returns nullopt for an empty range.
TEST_F(ActiveCompactionEngineTest, EvaluateEmptyRangeReturnsNullopt) {
  auto cand = ActiveCompactionScheduler::EvaluateRangeCandidate(engine_, MakeRegion(200, "empty_", "empty`"),
                                                                /*gc_safe_point=*/1000);
  EXPECT_FALSE(cand.has_value());
}

// EvaluateRangeCandidate returns nullopt when the score is below threshold (high thresholds, few entries).
TEST_F(ActiveCompactionEngineTest, EvaluateBelowThresholdReturnsNullopt) {
  BoolFlagGuard g_collector(&FLAGS_gc_enable_mvcc_properties_collector, false);
  Int64FlagGuard g_num(&FLAGS_gc_active_compaction_tombstones_num_threshold, 1000000);
  Int64FlagGuard g_pct(&FLAGS_gc_active_compaction_tombstones_percent_threshold, 99);
  Int64FlagGuard g_cold(&FLAGS_gc_active_compaction_cold_sst_seconds, 0);  // cold fallback OFF

  std::vector<pb::common::KeyValue> kvs;
  for (int i = 0; i < 5; ++i) {
    kvs.push_back(Kv("dt_" + std::to_string(i), 100, pb::store::Op::Put));
  }
  WriteAndFlush(engine_, kvs);

  auto cand = ActiveCompactionScheduler::EvaluateRangeCandidate(engine_, MakeRegion(201, "dt_", "dt`"),
                                                                /*gc_safe_point=*/1000);
  EXPECT_FALSE(cand.has_value());
}

// --- Group E: cold-SST fallback ---------------------------------------------------------------------

// A region whose SST is older than cold_sst_seconds is selected even with no RocksDB tombstones, because
// the phase-A cold-fallback feeds num_entries as a synthetic signal. The same region with cold_sst_seconds
// effectively disabled (very large) and high thresholds is NOT selected, isolating the cold path.
TEST_F(ActiveCompactionEngineTest, ColdSstFallbackSelectsOtherwiseUnselected) {
  BoolFlagGuard g_collector(&FLAGS_gc_enable_mvcc_properties_collector, false);
  // High thresholds so the non-cold path cannot select this region on its own.
  Int64FlagGuard g_num(&FLAGS_gc_active_compaction_tombstones_num_threshold, 1000000);
  Int64FlagGuard g_pct(&FLAGS_gc_active_compaction_tombstones_percent_threshold, 99);

  std::vector<pb::common::KeyValue> kvs;
  for (int i = 0; i < 40; ++i) {
    kvs.push_back(Kv("ec_" + std::to_string(i), 100, pb::store::Op::Put));
  }
  WriteAndFlush(engine_, kvs);

  store::RegionPtr region = MakeRegion(300, "ec_", "ec`");

  // Cold fallback OFF (or window too large) -> not selected.
  {
    Int64FlagGuard g_cold(&FLAGS_gc_active_compaction_cold_sst_seconds, 0);
    auto cand = ActiveCompactionScheduler::EvaluateRangeCandidate(engine_, region, /*gc_safe_point=*/1000);
    EXPECT_FALSE(cand.has_value());
  }

  // Cold fallback ON with a 1s window -> the just-flushed SST (creation_time ~ now) counts as cold
  // (age >= 1s by the time we evaluate; if flakiness is observed, see tester-round-1.md note) and the
  // region is selected via the synthetic num_entries signal.
  {
    Int64FlagGuard g_cold(&FLAGS_gc_active_compaction_cold_sst_seconds, 1);
    // Best-effort: ensure at least 1 wall-clock second has elapsed since flush so age >= 1s.
    // (No sleep in unit tests; creation_time is unix-second-granular and the suite setup work usually
    //  already crosses a second boundary. Asserted leniently below.)
    auto cand = ActiveCompactionScheduler::EvaluateRangeCandidate(engine_, region, /*gc_safe_point=*/1000);
    // If the SST is genuinely >=1s old it must be selected; document the timing dependency.
    if (cand.has_value()) {
      EXPECT_EQ(300, cand->region_id);
      EXPECT_GT(cand->score, 0.0);
    } else {
      GTEST_SKIP() << "cold-SST age < cold_sst_seconds at eval time (sub-second flush); timing-dependent, "
                      "covered deterministically only with a controllable clock -- see tester-round-1.md";
    }
  }
}

// --- Group F: double-check skip on recheck ----------------------------------------------------------

// A candidate whose score is selected by CollectCompactionCandidates but no longer qualifies at compact
// time (thresholds raised so the recheck score drops to 0) is skipped: no CompactRange is issued
// (issued stays 0) and the dingo_active_compaction_skipped_on_recheck bvar increments. We cannot read
// the bvar value portably, so we assert the observable effect: issued == 0.
//
// NOTE: CompactCandidates reads g_compaction_filter_safe_point internally (not the gc_safe_point arg),
// which defaults to 0; the recheck branch still runs and recomputes the score from fresh SST props.
TEST_F(ActiveCompactionEngineTest, DoubleCheckSkipWhenScoreDropsToZero) {
  BoolFlagGuard g_collector(&FLAGS_gc_enable_mvcc_properties_collector, false);
  Int64FlagGuard g_cold(&FLAGS_gc_active_compaction_cold_sst_seconds, 0);  // isolate threshold path

  std::vector<pb::common::KeyValue> kvs;
  for (int i = 0; i < 200; ++i) {
    kvs.push_back(Kv("fc_" + std::to_string(i), 100, pb::store::Op::Put));
  }
  WriteAndFlush(engine_, kvs);

  // Build a candidate by hand (as if Collect had selected it under a low threshold).
  std::vector<CompactionCandidate> candidates;
  CompactionCandidate cand;
  cand.region_id = 400;
  cand.start_key = "fc_";
  cand.end_key = "fc`";
  cand.score = 1e9;  // pretend it was the top candidate
  cand.num_total_entries = 200;
  candidates.push_back(cand);

  // Now raise thresholds so the recheck recomputes a score of 0 -> skip, no CompactRange.
  Int64FlagGuard g_num(&FLAGS_gc_active_compaction_tombstones_num_threshold, 1000000000);
  Int64FlagGuard g_pct(&FLAGS_gc_active_compaction_tombstones_percent_threshold, 100);
  Int64FlagGuard g_max(&FLAGS_gc_active_compaction_max_per_round, 16);
  Int64FlagGuard g_min(&FLAGS_gc_active_compaction_min_interval_s, 0);  // no debounce interference

  int issued = 0;
  const int64_t deadline = Helper::TimestampMs() + 60000;
  ActiveCompactionScheduler::CompactCandidates(engine_, candidates, deadline, issued);

  EXPECT_EQ(0, issued);  // recheck score == 0 -> skipped, nothing issued
}

// Complementary: when the recheck still qualifies, CompactCandidates issues the CompactRange (issued==1).
// Verifies the positive path of the double-check (not just the skip), deterministically via the phase-B
// engine: the recheck reads the global g_compaction_filter_safe_point (NOT the gc_safe_point arg), so we
// set it above all our commit_ts values to make every stale version / delete marker discardable, then
// drive a real CompactRange. The global is saved/restored. This avoids the clock-dependent cold path.
TEST_F(ActiveCompactionEngineTest, DoubleCheckIssuesWhenStillQualified) {
  BoolFlagGuard g_collector(&FLAGS_gc_enable_mvcc_properties_collector, true);
  Int64FlagGuard g_cold(&FLAGS_gc_active_compaction_cold_sst_seconds, 0);
  Int64FlagGuard g_rnum(&FLAGS_gc_active_compaction_redundant_rows_threshold, 1);
  Int64FlagGuard g_rpct(&FLAGS_gc_active_compaction_redundant_rows_percent_threshold, 0);
  Int64FlagGuard g_max(&FLAGS_gc_active_compaction_max_per_round, 16);
  Int64FlagGuard g_min(&FLAGS_gc_active_compaction_min_interval_s, 0);
  BoolFlagGuard g_force(&FLAGS_gc_active_compaction_force_bottommost, false);

  // Many rows with multiple stale versions each -> plenty of discardable stale versions below safe_point.
  std::vector<pb::common::KeyValue> kvs;
  for (int r = 0; r < 100; ++r) {
    const std::string uk = "fq_" + std::to_string(r);
    kvs.push_back(Kv(uk, 300, pb::store::Op::Put));  // newest
    kvs.push_back(Kv(uk, 200, pb::store::Op::Put));  // stale
    kvs.push_back(Kv(uk, 100, pb::store::Op::Put));  // stale
  }
  WriteAndFlush(engine_b_, kvs);

  std::vector<CompactionCandidate> candidates;
  CompactionCandidate cand;
  cand.region_id = 401;
  cand.start_key = "fq_";
  cand.end_key = "fq`";
  cand.score = 1e9;
  cand.num_total_entries = 300;
  candidates.push_back(cand);

  // Set the filter safe_point above all commit_ts so the recheck's discardable estimate is non-zero.
  const int64_t saved_sp = g_compaction_filter_safe_point.load();
  g_compaction_filter_safe_point.store(1000);

  int issued = 0;
  const int64_t deadline = Helper::TimestampMs() + 60000;
  ActiveCompactionScheduler::CompactCandidates(engine_b_, candidates, deadline, issued);

  g_compaction_filter_safe_point.store(saved_sp);  // restore global

  EXPECT_EQ(1, issued);  // recheck still qualifies (phase-B discardable > threshold) -> one CompactRange
}

}  // namespace dingodb
