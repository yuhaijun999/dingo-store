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

// Unit tests for the txn scan "seek round" rework (architect-1-design.md, F1-F4):
//   F1 GetUserValueInWriteIter version-locating loop adaptive seek fallback (SI-only).
//   F2 SI correctness: adaptive-seek result is byte-for-byte identical to the pure-Next() path.
//   F3 RC semantics: target_ts == seek_ts(kMaxVer) -> never skips, reads latest committed.
//   F4 FLAGS_txn_scan_rc_skip_lock: RC without lock collection skips the lock CF entirely.
//
// All FLAGS_ touched here are restored to their defaults at the end of every test (FlagGuard) so
// these cases never pollute one another or other suites.

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "bvar/bvar.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/stream.h"
#include "common/tracker.h"
#include "config/config.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "engine/txn_engine_helper.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

// FLAGS under test (defined inside namespace dingodb in txn_engine_helper.cc, so the DECLARE must
// live in the same namespace or the linker looks for ::FLAGS_* and fails to resolve).
DECLARE_bool(txn_scan_adaptive_seek);
DECLARE_int64(txn_scan_seek_bound);
DECLARE_bool(txn_scan_rc_skip_lock);

// bvar incremented by GetUserValueInWriteIter's seek fallback (defined in txn_engine_helper.cc, not
// exported in the header). Declared extern here so the test can read its accumulated value.
extern bvar::Adder<int64_t> g_txn_scan_over_seek_bound_version;

namespace {

const std::string kRootPath = "./unit_test_txn_scan_seek_round";
const std::string kLogPath = kRootPath + "/log";
const std::string kStorePath = kRootPath + "/db";

const std::string kYamlConfigContent =
    "cluster:\n"
    "  name: dingodb\n"
    "  instance_id: 12345\n"
    "  coordinators: 127.0.0.1:19190,127.0.0.1:19191,127.0.0.1:19192\n"
    "  keyring: TO_BE_CONTINUED\n"
    "server:\n"
    "  host: 127.0.0.1\n"
    "  port: 23000\n"
    "log:\n"
    "  path: " +
    kLogPath +
    "\n"
    "store:\n"
    "  path: " +
    kStorePath + "\n";

// RAII guard that snapshots all seek-round FLAGS on construction and restores them on destruction,
// so each test leaves the process global flags exactly as it found them.
struct FlagGuard {
  bool adaptive_seek;
  int64_t seek_bound;
  bool rc_skip_lock;

  FlagGuard()
      : adaptive_seek(FLAGS_txn_scan_adaptive_seek),
        seek_bound(FLAGS_txn_scan_seek_bound),
        rc_skip_lock(FLAGS_txn_scan_rc_skip_lock) {}

  ~FlagGuard() {
    FLAGS_txn_scan_adaptive_seek = adaptive_seek;
    FLAGS_txn_scan_seek_bound = seek_bound;
    FLAGS_txn_scan_rc_skip_lock = rc_skip_lock;
  }
};

struct ScanOutput {
  butil::Status status;
  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::common::KeyValue> kvs;
  std::vector<pb::store::TxnScanEntry> entries;
  bool has_more{false};
  std::string end_key;
  StreamPtr stream;
};

}  // namespace

class TxnScanSeekRoundTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    Helper::CreateDirectories(kStorePath);

    std::shared_ptr<Config> config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, kAllCFs));
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
    Helper::RemoveAllFileOrDirectory(kRootPath);
  }

  static inline std::shared_ptr<RocksRawEngine> engine;

 private:
  static const std::vector<std::string> kAllCFs;
};

const std::vector<std::string> TxnScanSeekRoundTest::kAllCFs = {
    Constant::kTxnWriteCF,
    Constant::kTxnDataCF,
    Constant::kTxnLockCF,
    "default",
};

// ----------------------------------------------------------------------------
// Helpers (mirror test_txn_scan_lock_collection.cc style).
// ----------------------------------------------------------------------------

static void ClearTxnPrefix(const std::shared_ptr<RocksRawEngine> &engine, const std::string &prefix) {
  pb::common::Range range;
  range.set_start_key(mvcc::Codec::EncodeKey(prefix, Constant::kMaxVer));
  range.set_end_key(mvcc::Codec::EncodeKey(Helper::PrefixNext(prefix), 0));

  auto writer = engine->Writer();
  ASSERT_EQ(writer->KvDeleteRange(Constant::kTxnWriteCF, range).error_code(), pb::error::OK);
  ASSERT_EQ(writer->KvDeleteRange(Constant::kTxnDataCF, range).error_code(), pb::error::OK);
  ASSERT_EQ(writer->KvDeleteRange(Constant::kTxnLockCF, range).error_code(), pb::error::OK);
}

static std::string TestKey(const std::string &prefix, const std::string &suffix) { return prefix + suffix; }

// Write one committed Put version. short_value != "" stores the value inline in the write CF
// (short-value path); otherwise the value goes to the data CF (long-value path).
static void PutVersion(const std::shared_ptr<RocksRawEngine> &engine, const std::string &key, const std::string &value,
                       int64_t start_ts, int64_t commit_ts, bool short_value) {
  pb::store::WriteInfo write_info;
  write_info.set_start_ts(start_ts);
  write_info.set_op(pb::store::Op::Put);
  if (short_value) {
    write_info.set_short_value(value);
  }

  pb::common::KeyValue write_kv;
  write_kv.set_key(mvcc::Codec::EncodeKey(key, commit_ts));
  write_kv.set_value(write_info.SerializeAsString());
  ASSERT_EQ(engine->Writer()->KvPut(Constant::kTxnWriteCF, write_kv).error_code(), pb::error::OK);

  if (!short_value) {
    pb::common::KeyValue data_kv;
    data_kv.set_key(mvcc::Codec::EncodeKey(key, start_ts));
    data_kv.set_value(value);
    ASSERT_EQ(engine->Writer()->KvPut(Constant::kTxnDataCF, data_kv).error_code(), pb::error::OK);
  }
}

// Convenience: committed Put backed by the data CF (long-value path).
static void PutCommitted(const std::shared_ptr<RocksRawEngine> &engine, const std::string &key,
                         const std::string &value, int64_t start_ts, int64_t commit_ts) {
  PutVersion(engine, key, value, start_ts, commit_ts, /*short_value=*/false);
}

// Write a committed write CF record carrying an arbitrary Op (Delete / Rollback / ...). No data CF.
static void PutWriteOp(const std::shared_ptr<RocksRawEngine> &engine, const std::string &key, pb::store::Op op,
                       int64_t start_ts, int64_t commit_ts) {
  pb::store::WriteInfo write_info;
  write_info.set_start_ts(start_ts);
  write_info.set_op(op);

  pb::common::KeyValue write_kv;
  write_kv.set_key(mvcc::Codec::EncodeKey(key, commit_ts));
  write_kv.set_value(write_info.SerializeAsString());
  ASSERT_EQ(engine->Writer()->KvPut(Constant::kTxnWriteCF, write_kv).error_code(), pb::error::OK);
}

static void PutLock(const std::shared_ptr<RocksRawEngine> &engine, const std::string &key, int64_t lock_ts,
                    pb::store::Op lock_type = pb::store::Op::Put, int64_t for_update_ts = 0) {
  pb::store::LockInfo lock_info;
  lock_info.set_primary_lock(key);
  lock_info.set_lock_ts(lock_ts);
  lock_info.set_key(key);
  lock_info.set_lock_ttl(1000);
  lock_info.set_lock_type(lock_type);
  lock_info.set_for_update_ts(for_update_ts);

  pb::common::KeyValue lock_kv;
  lock_kv.set_key(mvcc::Codec::EncodeKey(key, Constant::kLockVer));
  lock_kv.set_value(lock_info.SerializeAsString());
  ASSERT_EQ(engine->Writer()->KvPut(Constant::kTxnLockCF, lock_kv).error_code(), pb::error::OK);
}

// Drive a full TxnEngineHelper::Scan over [prefix, PrefixNext(prefix)). enable_lock_collection=false
// so values land in `entries` as kv entries (the normal read path that exercises
// GetUserValueInWriteIter). Returns kv pairs decoded to (user_key, value).
static ScanOutput RunScan(const std::shared_ptr<RocksRawEngine> &engine, const std::string &prefix, int64_t scan_ts,
                          pb::store::IsolationLevel isolation, bool enable_lock_collection = false,
                          const std::set<int64_t> &resolved_locks = {}, uint32_t stream_limit = 100000) {
  ScanOutput output;
  output.stream = Stream::New(stream_limit);

  auto ctx = std::make_shared<Context>();
  ctx->SetTracker(Tracker::New(pb::common::RequestInfo{}));

  pb::common::Range range;
  range.set_start_key(prefix);
  range.set_end_key(Helper::PrefixNext(prefix));

  pb::common::CoprocessorV2 coprocessor;
  output.status = TxnEngineHelper::Scan(ctx, output.stream, engine, isolation, scan_ts, range, stream_limit,
                                        /*key_only=*/false, /*is_reverse=*/false, resolved_locks,
                                        /*disable_coprocessor=*/true, coprocessor, enable_lock_collection,
                                        output.txn_result_info, output.kvs, output.entries, output.has_more,
                                        output.end_key);
  return output;
}

// Flatten scan results to (user_key, value) for byte-for-byte comparison.
// With lock_collection disabled (the default read path) values land in out.kvs; with lock_collection
// enabled they land in out.entries as kv entries (locked entries are skipped here).
static std::vector<std::pair<std::string, std::string>> CollectKvs(const ScanOutput &out) {
  std::vector<std::pair<std::string, std::string>> result;
  for (const auto &kv : out.kvs) {
    result.emplace_back(kv.key(), kv.value());
  }
  for (const auto &e : out.entries) {
    if (e.has_kv()) {
      result.emplace_back(e.kv().key(), e.kv().value());
    }
  }
  return result;
}

// ============================================================================
// 5. Encoding property tests (architect §8.5 / §7.2 risk 5). These lock the seek-landing
//    assumptions the whole F1 fallback relies on. No engine needed.
// ============================================================================

TEST_F(TxnScanSeekRoundTest, EncodeKeyUserKeyDimensionOrdersBeforeTs) {
  // A < B  =>  EncodeKey(A, anyTs1) < EncodeKey(B, anyTs2) for ALL ts (user_key dimension wins).
  const std::string a = "aaa";
  const std::string b = "aab";  // a < b
  const std::vector<int64_t> tss = {0, 1, 100, 1000, Constant::kMaxVer};

  for (int64_t ts_a : tss) {
    for (int64_t ts_b : tss) {
      const std::string ea = mvcc::Codec::EncodeKey(a, ts_a);
      const std::string eb = mvcc::Codec::EncodeKey(b, ts_b);
      EXPECT_LT(ea, eb) << "user_key ordering must dominate ts; a_ts=" << ts_a << " b_ts=" << ts_b;
    }
  }
}

TEST_F(TxnScanSeekRoundTest, EncodeKeyDescendingTsWithinSameUserKey) {
  // Within the same user_key, a LARGER ts encodes to a SMALLER key (descending ts order), so
  // Seek(EncodeKey(k, target_ts)) lands on the first version with commit_ts <= target_ts.
  const std::string k = "user_key";
  EXPECT_LT(mvcc::Codec::EncodeKey(k, 100), mvcc::Codec::EncodeKey(k, 50));
  EXPECT_LT(mvcc::Codec::EncodeKey(k, Constant::kMaxVer), mvcc::Codec::EncodeKey(k, 1));
  EXPECT_LT(mvcc::Codec::EncodeKey(k, 10), mvcc::Codec::EncodeKey(k, 0));

  // The whole chain of one user_key sorts strictly below the chain head of the next user_key:
  // this is why a seek that overshoots a user_key lands on the next user_key's LATEST version.
  const std::string next = Helper::PrefixNext(k);
  EXPECT_LT(mvcc::Codec::EncodeKey(k, 0), mvcc::Codec::EncodeKey(next, Constant::kMaxVer));
}

// ============================================================================
// 1. F1/F2 equivalence: adaptive_seek off vs on must yield byte-for-byte identical kvs.
//    Covers SI at min / mid / max start_ts, and RC.
// ============================================================================

// Build N committed versions for each of several user_keys. Version i (1-based) of user_key uses
// commit_ts = i*10, start_ts = i*10 - 1, value = "<key>_v<i>". Returns the user_keys created.
static std::vector<std::string> BuildVersionChains(const std::shared_ptr<RocksRawEngine> &engine,
                                                   const std::string &prefix, int num_keys, int versions_per_key) {
  std::vector<std::string> keys;
  for (int ki = 0; ki < num_keys; ++ki) {
    char suffix[8];
    snprintf(suffix, sizeof(suffix), "%03d", ki);
    std::string key = TestKey(prefix, suffix);
    keys.push_back(key);
    for (int v = 1; v <= versions_per_key; ++v) {
      int64_t commit_ts = static_cast<int64_t>(v) * 10;
      int64_t start_ts = commit_ts - 1;
      PutCommitted(engine, key, key + "_v" + std::to_string(v), start_ts, commit_ts);
    }
  }
  return keys;
}

TEST_F(TxnScanSeekRoundTest, AdaptiveSeekEquivalenceSiAndRc) {
  FlagGuard guard;
  const std::string prefix = "txn_sr_equiv_";
  ClearTxnPrefix(engine, prefix);

  const int num_keys = 4;
  const int versions = 20;  // deep enough (> default seek_bound 8) to trigger the fallback
  BuildVersionChains(engine, prefix, num_keys, versions);

  FLAGS_txn_scan_seek_bound = 8;

  // SI snapshots: min (below all -> empty), several mid points, max (above all -> latest visible).
  // Plus RC (reads latest committed, fallback must be a no-op).
  struct Case {
    pb::store::IsolationLevel iso;
    int64_t ts;
  };
  std::vector<Case> cases = {
      {pb::store::IsolationLevel::SnapshotIsolation, 5},    // below v1 (commit_ts 10) -> empty
      {pb::store::IsolationLevel::SnapshotIsolation, 55},   // mid: visible up to v5 (commit_ts 50)
      {pb::store::IsolationLevel::SnapshotIsolation, 125},  // mid: visible up to v12 (commit_ts 120)
      {pb::store::IsolationLevel::SnapshotIsolation, 100000},  // above all -> latest version v20
      {pb::store::IsolationLevel::ReadCommitted, 55},          // RC: latest committed regardless of ts
      {pb::store::IsolationLevel::ReadCommitted, 100000},
  };

  for (const auto &c : cases) {
    FLAGS_txn_scan_adaptive_seek = false;
    auto baseline = RunScan(engine, prefix, c.ts, c.iso);
    ASSERT_TRUE(baseline.status.ok()) << baseline.status.error_str();

    FLAGS_txn_scan_adaptive_seek = true;
    auto optimized = RunScan(engine, prefix, c.ts, c.iso);
    ASSERT_TRUE(optimized.status.ok()) << optimized.status.error_str();

    // Byte-for-byte identical kv list (key, value, order) and has_more.
    EXPECT_EQ(baseline.has_more, optimized.has_more) << "iso=" << c.iso << " ts=" << c.ts;
    EXPECT_EQ(CollectKvs(baseline), CollectKvs(optimized))
        << "kv mismatch between adaptive off/on, iso=" << c.iso << " ts=" << c.ts;
  }
}

// ============================================================================
// 2. Trigger the fallback and assert the bvar counter advances.
// ============================================================================

TEST_F(TxnScanSeekRoundTest, FallbackTriggeredCounterIncrementsAndResultCorrect) {
  FlagGuard guard;
  const std::string prefix = "txn_sr_fallback_";
  ClearTxnPrefix(engine, prefix);

  const int num_keys = 3;
  const int versions = 20;  // > seek_bound, start_ts mid-chain forces > seek_bound skips per key
  auto keys = BuildVersionChains(engine, prefix, num_keys, versions);

  FLAGS_txn_scan_seek_bound = 8;
  FLAGS_txn_scan_adaptive_seek = true;

  // SI start_ts mid-chain: v15 visible (commit_ts 150), versions v16..v20 (5) are newer... not yet
  // over the bound. Use a lower mid point so > 8 newer versions get skipped: ts=55 -> v5 visible,
  // v6..v20 = 15 newer versions per key, well over seek_bound 8.
  const int64_t mid_ts = 55;

  int64_t before = g_txn_scan_over_seek_bound_version.get_value();
  auto out = RunScan(engine, prefix, mid_ts, pb::store::IsolationLevel::SnapshotIsolation);
  ASSERT_TRUE(out.status.ok()) << out.status.error_str();
  int64_t after = g_txn_scan_over_seek_bound_version.get_value();

  // Fallback must have fired at least once per user_key.
  EXPECT_GT(after - before, 0) << "expected the version-loop seek fallback to fire";

  // Result correctness: each user_key resolves to its v5 value (commit_ts 50 <= 55).
  auto kvs = CollectKvs(out);
  ASSERT_EQ(kvs.size(), static_cast<size_t>(num_keys));
  for (size_t i = 0; i < keys.size(); ++i) {
    EXPECT_EQ(kvs[i].first, keys[i]);
    EXPECT_EQ(kvs[i].second, keys[i] + "_v5");
  }

  // Cross-check against the pure-Next path: same result.
  FLAGS_txn_scan_adaptive_seek = false;
  auto baseline = RunScan(engine, prefix, mid_ts, pb::store::IsolationLevel::SnapshotIsolation);
  ASSERT_TRUE(baseline.status.ok());
  EXPECT_EQ(CollectKvs(baseline), kvs);
}

// ============================================================================
// 3. Op boundary matrix (adaptive seek ON, target version is each Op in turn). Each user_key has
//    many newer versions above the target so the fallback engages, then the target Op is handled.
// ============================================================================

TEST_F(TxnScanSeekRoundTest, OpBoundaryMatrixUnderFallback) {
  FlagGuard guard;
  const std::string prefix = "txn_sr_opmatrix_";
  ClearTxnPrefix(engine, prefix);

  FLAGS_txn_scan_seek_bound = 8;
  FLAGS_txn_scan_adaptive_seek = true;

  // SI snapshot ts. Target version commit_ts must be <= si_ts; "newer" decoy versions use commit_ts
  // > si_ts (and there are > seek_bound of them to force a fallback).
  const int64_t si_ts = 1000;
  const int64_t target_commit = 100;  // <= si_ts -> visible target
  const int64_t target_start = 99;

  // Helper to stack >seek_bound newer (commit_ts > si_ts) decoy Put versions above a key.
  auto add_newer_decoys = [&](const std::string &key) {
    for (int i = 0; i < 12; ++i) {  // 12 > seek_bound 8
      int64_t cts = si_ts + 100 + i * 10;
      PutCommitted(engine, key, "newer", cts - 1, cts);
    }
  };

  // k1: target = Put short_value
  const std::string k1 = TestKey(prefix, "001");
  PutVersion(engine, k1, "short_v1", target_start, target_commit, /*short_value=*/true);
  add_newer_decoys(k1);

  // k2: target = Put long value (data CF)
  const std::string k2 = TestKey(prefix, "002");
  PutVersion(engine, k2, "long_value_in_data_cf", target_start, target_commit, /*short_value=*/false);
  add_newer_decoys(k2);

  // k3: target = Delete -> no value, key absent from results
  const std::string k3 = TestKey(prefix, "003");
  PutWriteOp(engine, k3, pb::store::Op::Delete, target_start, target_commit);
  add_newer_decoys(k3);

  // k4: a Rollback record sits ABOVE the real Put target (but still <= si_ts). The loop must skip
  // the Rollback and continue to the Put.
  const std::string k4 = TestKey(prefix, "004");
  PutCommitted(engine, k4, "k4_real", target_start - 10, target_commit - 10);  // commit_ts 90
  PutWriteOp(engine, k4, pb::store::Op::Rollback, target_start, target_commit);  // commit_ts 100 (<= si_ts)
  add_newer_decoys(k4);

  // k5: all versions are NEWER than si_ts (no version <= si_ts) -> key has no value at this snapshot.
  const std::string k5 = TestKey(prefix, "005");
  add_newer_decoys(k5);

  auto out = RunScan(engine, prefix, si_ts, pb::store::IsolationLevel::SnapshotIsolation);
  ASSERT_TRUE(out.status.ok()) << out.status.error_str();
  auto kvs = CollectKvs(out);

  // Expect k1 (short), k2 (long); k3 deleted, k4 put (rollback skipped), k5 absent.
  ASSERT_EQ(kvs.size(), 3u) << "expected k1,k2,k4 only";
  EXPECT_EQ(kvs[0].first, k1);
  EXPECT_EQ(kvs[0].second, "short_v1");
  EXPECT_EQ(kvs[1].first, k2);
  EXPECT_EQ(kvs[1].second, "long_value_in_data_cf");
  EXPECT_EQ(kvs[2].first, k4);
  EXPECT_EQ(kvs[2].second, "k4_real");

  // Equivalence cross-check with pure-Next path.
  FLAGS_txn_scan_adaptive_seek = false;
  auto baseline = RunScan(engine, prefix, si_ts, pb::store::IsolationLevel::SnapshotIsolation);
  ASSERT_TRUE(baseline.status.ok());
  EXPECT_EQ(CollectKvs(baseline), kvs);
}

// ============================================================================
// 4. Cross-user_key boundary (architect §8.4 / §7.2 risk 5). Several consecutive user_keys whose
//    versions are all newer than the snapshot, with one visible key in the middle.
// ============================================================================

TEST_F(TxnScanSeekRoundTest, CrossUserKeyAllNewerWithOneVisible) {
  FlagGuard guard;
  const std::string prefix = "txn_sr_crosskey_";
  ClearTxnPrefix(engine, prefix);

  FLAGS_txn_scan_seek_bound = 8;

  const int64_t si_ts = 1000;

  // k1,k2: all versions newer than si_ts -> no value.
  // k3: has one version <= si_ts -> visible.
  // k4,k5: all newer -> no value.
  auto stack_newer = [&](const std::string &key) {
    for (int i = 0; i < 12; ++i) {  // > seek_bound
      int64_t cts = si_ts + 100 + i * 10;
      PutCommitted(engine, key, "newer", cts - 1, cts);
    }
  };

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  const std::string k3 = TestKey(prefix, "003");
  const std::string k4 = TestKey(prefix, "004");
  const std::string k5 = TestKey(prefix, "005");
  stack_newer(k1);
  stack_newer(k2);
  stack_newer(k3);
  PutCommitted(engine, k3, "k3_visible", 99, 100);  // commit_ts 100 <= si_ts
  stack_newer(k4);
  stack_newer(k5);

  // adaptive off then on -> identical, and only k3 present.
  FLAGS_txn_scan_adaptive_seek = false;
  auto baseline = RunScan(engine, prefix, si_ts, pb::store::IsolationLevel::SnapshotIsolation);
  ASSERT_TRUE(baseline.status.ok());

  FLAGS_txn_scan_adaptive_seek = true;
  auto optimized = RunScan(engine, prefix, si_ts, pb::store::IsolationLevel::SnapshotIsolation);
  ASSERT_TRUE(optimized.status.ok());

  EXPECT_EQ(CollectKvs(baseline), CollectKvs(optimized));

  auto kvs = CollectKvs(optimized);
  ASSERT_EQ(kvs.size(), 1u);
  EXPECT_EQ(kvs[0].first, k3);
  EXPECT_EQ(kvs[0].second, "k3_visible");
}

TEST_F(TxnScanSeekRoundTest, CrossUserKeyAllNewerReturnsEmpty) {
  FlagGuard guard;
  const std::string prefix = "txn_sr_crosskey_empty_";
  ClearTxnPrefix(engine, prefix);

  FLAGS_txn_scan_seek_bound = 8;
  const int64_t si_ts = 1000;

  for (int ki = 0; ki < 3; ++ki) {
    char suffix[8];
    snprintf(suffix, sizeof(suffix), "%03d", ki);
    std::string key = TestKey(prefix, suffix);
    for (int i = 0; i < 12; ++i) {
      int64_t cts = si_ts + 100 + i * 10;  // all > si_ts
      PutCommitted(engine, key, "newer", cts - 1, cts);
    }
  }

  FLAGS_txn_scan_adaptive_seek = false;
  auto baseline = RunScan(engine, prefix, si_ts, pb::store::IsolationLevel::SnapshotIsolation);
  ASSERT_TRUE(baseline.status.ok());

  FLAGS_txn_scan_adaptive_seek = true;
  auto optimized = RunScan(engine, prefix, si_ts, pb::store::IsolationLevel::SnapshotIsolation);
  ASSERT_TRUE(optimized.status.ok());

  EXPECT_TRUE(CollectKvs(baseline).empty());
  EXPECT_TRUE(CollectKvs(optimized).empty());
}

// ============================================================================
// 6. F4: FLAGS_txn_scan_rc_skip_lock.
// ============================================================================

// With rc_skip_lock ON and no lock collection: an RC scan ignores an uncommitted lock and reads the
// latest committed version below it (no conflict). The same data under SI must report a lock conflict.
TEST_F(TxnScanSeekRoundTest, RcSkipLockReadsLatestCommittedWhileSiConflicts) {
  FlagGuard guard;
  const std::string prefix = "txn_sr_rc_skip_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  PutCommitted(engine, k1, "v1", 10, 20);
  PutCommitted(engine, k2, "committed_v2", 11, 21);
  // Optimistic prewrite lock (no for_update_ts) with lock_ts < start_ts on k2.
  PutLock(engine, k2, /*lock_ts=*/50);

  FLAGS_txn_scan_rc_skip_lock = true;

  // RC: lock skipped, read committed_v2 on k2, no conflict.
  auto rc = RunScan(engine, prefix, /*scan_ts=*/100, pb::store::IsolationLevel::ReadCommitted);
  ASSERT_TRUE(rc.status.ok()) << rc.status.error_str();
  EXPECT_FALSE(rc.has_more);
  EXPECT_EQ(rc.txn_result_info.ByteSizeLong(), 0) << "RC must not report a lock conflict";
  auto rc_kvs = CollectKvs(rc);
  ASSERT_EQ(rc_kvs.size(), 2u);
  EXPECT_EQ(rc_kvs[0].first, k1);
  EXPECT_EQ(rc_kvs[0].second, "v1");
  EXPECT_EQ(rc_kvs[1].first, k2);
  EXPECT_EQ(rc_kvs[1].second, "committed_v2");

  // SI over the same data: optimistic lock with lock_ts(50) <= start_ts(100) -> conflict reported
  // via txn_result_info.locked + has_more (no error status, matching the non-lock-collection path).
  auto si = RunScan(engine, prefix, /*scan_ts=*/100, pb::store::IsolationLevel::SnapshotIsolation);
  ASSERT_TRUE(si.status.ok()) << si.status.error_str();
  EXPECT_TRUE(si.has_more);
  ASSERT_TRUE(si.txn_result_info.has_locked()) << si.txn_result_info.ShortDebugString();
  EXPECT_EQ(si.txn_result_info.locked().key(), k2);
  EXPECT_EQ(si.txn_result_info.locked().lock_ts(), 50);
}

// With rc_skip_lock OFF (default), RC behaves exactly as before: the optimistic lock is ignored by
// RC's existing weakened conflict check, so RC still reads the committed value. (Behavior parity is
// the key assertion -- the result is the same as flag-on for this non-conflicting lock.)
TEST_F(TxnScanSeekRoundTest, RcSkipLockOffPreservesPriorRcBehavior) {
  FlagGuard guard;
  const std::string prefix = "txn_sr_rc_off_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  PutCommitted(engine, k1, "v1", 10, 20);
  PutCommitted(engine, k2, "committed_v2", 11, 21);
  PutLock(engine, k2, /*lock_ts=*/50);  // optimistic lock

  FLAGS_txn_scan_rc_skip_lock = false;  // default

  auto rc = RunScan(engine, prefix, /*scan_ts=*/100, pb::store::IsolationLevel::ReadCommitted);
  ASSERT_TRUE(rc.status.ok()) << rc.status.error_str();
  EXPECT_EQ(rc.txn_result_info.ByteSizeLong(), 0);
  auto rc_kvs = CollectKvs(rc);
  ASSERT_EQ(rc_kvs.size(), 2u);
  EXPECT_EQ(rc_kvs[0].second, "v1");
  EXPECT_EQ(rc_kvs[1].second, "committed_v2");
}

// RC + rc_skip_lock on, empty range: must not crash and returns empty.
TEST_F(TxnScanSeekRoundTest, RcSkipLockEmptyRange) {
  FlagGuard guard;
  const std::string prefix = "txn_sr_rc_empty_";
  ClearTxnPrefix(engine, prefix);

  FLAGS_txn_scan_rc_skip_lock = true;

  auto rc = RunScan(engine, prefix, /*scan_ts=*/100, pb::store::IsolationLevel::ReadCommitted);
  ASSERT_TRUE(rc.status.ok()) << rc.status.error_str();
  EXPECT_FALSE(rc.has_more);
  EXPECT_TRUE(CollectKvs(rc).empty());
  EXPECT_EQ(rc.txn_result_info.ByteSizeLong(), 0);
}

// RC + rc_skip_lock on, single committed key (no lock): must not crash and returns the value.
TEST_F(TxnScanSeekRoundTest, RcSkipLockSingleKey) {
  FlagGuard guard;
  const std::string prefix = "txn_sr_rc_single_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  PutCommitted(engine, k1, "only_v", 10, 20);

  FLAGS_txn_scan_rc_skip_lock = true;

  auto rc = RunScan(engine, prefix, /*scan_ts=*/100, pb::store::IsolationLevel::ReadCommitted);
  ASSERT_TRUE(rc.status.ok()) << rc.status.error_str();
  auto kvs = CollectKvs(rc);
  ASSERT_EQ(kvs.size(), 1u);
  EXPECT_EQ(kvs[0].first, k1);
  EXPECT_EQ(kvs[0].second, "only_v");
}

// RC + rc_skip_lock on, with lock collection requested: lock cursor must still be built (need_check
// _locks_ stays true because lock_collection_enabled_), so the lock is collected as a locked entry.
TEST_F(TxnScanSeekRoundTest, RcSkipLockWithLockCollectionStillCollects) {
  FlagGuard guard;
  const std::string prefix = "txn_sr_rc_lc_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  PutCommitted(engine, k1, "v1", 10, 20);
  PutCommitted(engine, k2, "committed_v2", 11, 21);
  PutLock(engine, k2, /*lock_ts=*/50, pb::store::Op::Put, /*for_update_ts=*/30);  // pessimistic, conflicts in RC

  FLAGS_txn_scan_rc_skip_lock = true;

  auto rc = RunScan(engine, prefix, /*scan_ts=*/100, pb::store::IsolationLevel::ReadCommitted,
                    /*enable_lock_collection=*/true);
  ASSERT_TRUE(rc.status.ok()) << rc.status.error_str();
  // Lock collection forces the lock cursor; the conflicting pessimistic lock is collected.
  ASSERT_EQ(rc.entries.size(), 2u);
  ASSERT_TRUE(rc.entries[0].has_kv());
  EXPECT_EQ(rc.entries[0].kv().key(), k1);
  ASSERT_TRUE(rc.entries[1].has_locked()) << rc.entries[1].ShortDebugString();
  EXPECT_EQ(rc.entries[1].locked().key(), k2);
  EXPECT_EQ(rc.entries[1].locked().lock_ts(), 50);
}

}  // namespace dingodb
