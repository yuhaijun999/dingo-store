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

// [GC-Tombstone] Unit tests for WriteCompactionFilter decision logic. These tests exercise the
// FilterV2 remove_older_ state machine, the tombstone safety gate (is_full_compaction +
// tombstone_require_full_compaction), and skip_until construction. NOTE: not run here (the user
// compiles/runs the tests themselves), they only document the expected behavior.
//
// Constructor signature under test:
//   WriteCompactionFilter(int64_t safe_point, bool is_full_compaction, bool tombstone_require_full_compaction)
//
// The tombstone policy is a CONSTRUCTOR snapshot, so most tests drive behavior purely through the two
// constructor bools and never touch the global FLAGS_. The factory-level interaction with
// FLAGS_gc_compaction_filter_tombstone_require_full_compaction is covered by a dedicated guard below.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "engine/write_compaction_filter.h"
#include "mvcc/codec.h"
#include "proto/store.pb.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"

namespace dingodb {

using Decision = rocksdb::CompactionFilter::Decision;
using ValueType = rocksdb::CompactionFilter::ValueType;

class WriteCompactionFilterTest : public testing::Test {
 protected:
  // Build an encoded write CF key for (user_key, commit_ts).
  static std::string MakeKey(const std::string& user_key, int64_t commit_ts) {
    return mvcc::Codec::EncodeKey(user_key, commit_ts);
  }

  // Build a serialized WriteInfo value with the given op.
  static std::string MakeValue(pb::store::Op op, int64_t start_ts = 1) {
    pb::store::WriteInfo write_info;
    write_info.set_op(op);
    write_info.set_start_ts(start_ts);
    return write_info.SerializeAsString();
  }

  // Drive one FilterV2 call.
  static Decision Filter(const WriteCompactionFilter& filter, const std::string& key, const std::string& value,
                         std::string* skip_until, ValueType value_type = ValueType::kValue) {
    std::string new_value;
    skip_until->clear();
    return filter.FilterV2(0, rocksdb::Slice(key), value_type, rocksdb::Slice(value), &new_value, skip_until);
  }
};

// ===========================================================================================
// Group A: existing decision-logic tests, updated to the new 3-arg constructor.
// Unless a test specifically targets the tombstone gate, it uses is_full_compaction=true and
// tombstone_require_full_compaction=true (the production default), which keeps the original
// assertion intent (FirstDeleteRemoved expects the marker to be removed -> requires full=true).
// ===========================================================================================

// Non-Value entries (blob index / merge operand) are always kept.
TEST_F(WriteCompactionFilterTest, NonValueKept) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 50), MakeValue(pb::store::Op::Put), &skip_until, ValueType::kBlobIndex),
            Decision::kKeep);
}

// Versions newer than safe_point are always kept.
TEST_F(WriteCompactionFilterTest, AboveSafePointKept) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 200), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
}

// Single key, no version above safe_point: the newest Put at/below safe_point is kept, older ones are
// removed via kRemoveAndSkipUntil, and skip_until == EncodeKey(user_key, commit_ts-1) (same user_key).
TEST_F(WriteCompactionFilterTest, FirstPutKeptOlderRemoved) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;

  // Newest Put at/below safe_point: kept (protected first put).
  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);

  // Older Put: removed; skip_until must equal EncodeKey("k1", 79) and be > the current key.
  std::string key80 = MakeKey("k1", 80);
  EXPECT_EQ(Filter(filter, key80, MakeValue(pb::store::Op::Put), &skip_until), Decision::kRemoveAndSkipUntil);
  EXPECT_EQ(skip_until, MakeKey("k1", 79));
  EXPECT_GT(skip_until, key80);  // descending ts encoding: commit_ts-1 encodes to larger bytes

  // skip_until must stay within the same user_key (not cross the boundary).
  std::string decoded_user_key;
  int64_t decoded_ts = 0;
  ASSERT_TRUE(mvcc::Codec::DecodeKey(skip_until, decoded_user_key, decoded_ts));
  EXPECT_EQ(decoded_user_key, "k1");
  EXPECT_EQ(decoded_ts, 79);
}

// A version above safe_point disables the "first put" protection: the newest Put at/below safe_point
// is then removable.
TEST_F(WriteCompactionFilterTest, NewerVersionAboveSafePointDisablesProtection) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;

  // Put above safe_point: kept, records that a newer Put/Delete exists.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 200), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);

  // Newest Put at/below safe_point is no longer protected -> removed.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Put), &skip_until), Decision::kRemoveAndSkipUntil);
  EXPECT_EQ(skip_until, MakeKey("k1", 89));
}

// The first Delete at/below safe_point is itself removed (the marker is dropped) -- ONLY under a full
// compaction with the safety gate on, which is exactly this configuration.
TEST_F(WriteCompactionFilterTest, FirstDeleteRemoved) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Delete), &skip_until),
            Decision::kRemoveAndSkipUntil);
  EXPECT_EQ(skip_until, MakeKey("k1", 89));
}

// A standalone Rollback at/below safe_point is removed via kRemove.
TEST_F(WriteCompactionFilterTest, RollbackRemoved) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Rollback), &skip_until), Decision::kRemove);
}

// Lock is NOT deleted (aligns with DoGcCoreTxn, which only logs and keeps it).
TEST_F(WriteCompactionFilterTest, LockKept) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Lock), &skip_until), Decision::kKeep);
}

// A key that fails to decode is conservatively kept (the filter must never crash a compaction thread).
TEST_F(WriteCompactionFilterTest, DecodeFailureKept) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;
  std::string bad_key = "short";  // shorter than the minimum encoded key length
  EXPECT_EQ(Filter(filter, bad_key, MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
}

// commit_ts == 1 boundary: skip_until = EncodeKey(user_key, 0), which must be valid and not carry over.
TEST_F(WriteCompactionFilterTest, CommitTsOneBoundary) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;

  // Make ts=1 a removable older version by first consuming the protected first put at a higher ts.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);

  std::string key1 = MakeKey("k1", 1);
  EXPECT_EQ(Filter(filter, key1, MakeValue(pb::store::Op::Put), &skip_until), Decision::kRemoveAndSkipUntil);
  EXPECT_EQ(skip_until, MakeKey("k1", 0));
  EXPECT_GT(skip_until, key1);

  std::string decoded_user_key;
  int64_t decoded_ts = 0;
  ASSERT_TRUE(mvcc::Codec::DecodeKey(skip_until, decoded_user_key, decoded_ts));
  EXPECT_EQ(decoded_user_key, "k1");
  EXPECT_EQ(decoded_ts, 0);
}

// Crossing a user_key boundary resets the per-key state (a new key's first put is protected again).
TEST_F(WriteCompactionFilterTest, UserKeyBoundaryResets) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;

  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
  EXPECT_EQ(Filter(filter, MakeKey("k1", 80), MakeValue(pb::store::Op::Put), &skip_until),
            Decision::kRemoveAndSkipUntil);

  // New user_key: its newest put at/below safe_point is protected again.
  EXPECT_EQ(Filter(filter, MakeKey("k2", 90), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
}

// ===========================================================================================
// Group B: tombstone safety gate (the core new behavior).
// First Delete at/below safe_point under the three (is_full_compaction, require_full) combos.
// ===========================================================================================

// full=true, require_full=true -> safe to physically remove the marker.
TEST_F(WriteCompactionFilterTest, FirstDeleteFullCompactionRequireFullRemoved) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 50), MakeValue(pb::store::Op::Delete), &skip_until),
            Decision::kRemoveAndSkipUntil);
  EXPECT_EQ(skip_until, MakeKey("k1", 49));
}

// full=false, require_full=true (the safe default on a partial compaction) -> the marker MUST be kept
// to keep shadowing any lower-level old version (prevents resurrection). This is the core fix.
TEST_F(WriteCompactionFilterTest, FirstDeletePartialCompactionRequireFullKept) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/false,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 50), MakeValue(pb::store::Op::Delete), &skip_until), Decision::kKeep);
  EXPECT_TRUE(skip_until.empty());  // kKeep never sets skip_until
}

// full=false, require_full=false (the old DANGEROUS behavior, A/B only) -> marker removed even on a
// partial compaction.
TEST_F(WriteCompactionFilterTest, FirstDeletePartialCompactionNoRequireFullRemoved) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/false,
                               /*tombstone_require_full_compaction=*/false);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 50), MakeValue(pb::store::Op::Delete), &skip_until),
            Decision::kRemoveAndSkipUntil);
  EXPECT_EQ(skip_until, MakeKey("k1", 49));
}

// ===========================================================================================
// Group C: state-machine interactions between the kept tombstone, remove_older_, and Rollback.
// ===========================================================================================

// On a partial compaction the first Delete is KEPT (kKeep) but it STILL sets remove_older_, so a
// lower (older) Put in this compaction's view is reclaimed via kRemoveAndSkipUntil. This proves the
// "keep the marker, still reclaim what's below it" claim -> non-full compactions still get zero-
// tombstone reclaim of the old data shadowed by the marker.
TEST_F(WriteCompactionFilterTest, KeptTombstoneStillRemovesOlderBelow) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/false,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;

  // First effective version is a Delete@50 -> kept (gate), remove_older_ now true.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 50), MakeValue(pb::store::Op::Delete), &skip_until), Decision::kKeep);

  // Older Put@30 below it -> redundant, dropped.
  std::string key30 = MakeKey("k1", 30);
  EXPECT_EQ(Filter(filter, key30, MakeValue(pb::store::Op::Put), &skip_until), Decision::kRemoveAndSkipUntil);
  EXPECT_EQ(skip_until, MakeKey("k1", 29));
}

// remove_older_ set by a kept FIRST Put: a still-older Delete marker below it is dropped via
// kRemoveAndSkipUntil even on a partial compaction (the gate only protects the FIRST tombstone; an
// older shadowed Delete is redundant because the newer Put already shadows it).
TEST_F(WriteCompactionFilterTest, OlderDeleteBelowKeptPutRemoved) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/false,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;

  // First effective version Put@50 -> kept (no newer above), remove_older_ now true.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 50), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);

  // Older Delete@30 -> dropped via the remove_older_ branch (NOT gated; it is shadowed).
  std::string key30 = MakeKey("k1", 30);
  EXPECT_EQ(Filter(filter, key30, MakeValue(pb::store::Op::Delete), &skip_until), Decision::kRemoveAndSkipUntil);
  EXPECT_EQ(skip_until, MakeKey("k1", 29));
}

// Rollback must NOT set remove_older_: a Rollback@50 (noise, kRemove) above a real Put@30 must leave
// the Put treated as the FIRST effective version (kept), not as a redundant older version.
TEST_F(WriteCompactionFilterTest, RollbackDoesNotShadowLowerPut) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;

  // Rollback@50 -> removed self only (kRemove), remove_older_ stays false.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 50), MakeValue(pb::store::Op::Rollback), &skip_until), Decision::kRemove);

  // Real Put@30 -> still the first effective version, kept (no Put/Delete above safe_point).
  EXPECT_EQ(Filter(filter, MakeKey("k1", 30), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
  EXPECT_TRUE(skip_until.empty());
}

// Rollback above a Put that IS shadowed by a newer Put above safe_point: the Rollback still does not
// set remove_older_, but the first Put@30 is dropped because seen_put_or_delete_above_sp_ is true.
// This confirms Rollback is transparent to BOTH the remove_older_ and the seen_above logic.
TEST_F(WriteCompactionFilterTest, RollbackTransparentWithNewerAbove) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;

  // Newer Put above safe_point -> kept, sets seen_put_or_delete_above_sp_.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 200), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
  // Rollback@90 -> kRemove, does not set remove_older_.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Rollback), &skip_until), Decision::kRemove);
  // First Put@30 below safe_point but shadowed by the above Put -> dropped to stay subset of DoGcCoreTxn.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 30), MakeValue(pb::store::Op::Put), &skip_until),
            Decision::kRemoveAndSkipUntil);
  EXPECT_EQ(skip_until, MakeKey("k1", 29));
}

// ===========================================================================================
// Group D: equivalence and defensive checks.
// ===========================================================================================

// State-machine equivalence: with is_full_compaction=true the tombstone gate is satisfied regardless
// of require_full, so a (full=true, require_full=false) filter must make the SAME decisions as the
// production (full=true, require_full=true) filter for a Delete-led sequence.
TEST_F(WriteCompactionFilterTest, FullCompactionEquivalentAcrossFlag) {
  std::string skip_a;
  std::string skip_b;
  WriteCompactionFilter filter_a(/*safe_point=*/100, /*is_full_compaction=*/true,
                                 /*tombstone_require_full_compaction=*/true);
  WriteCompactionFilter filter_b(/*safe_point=*/100, /*is_full_compaction=*/true,
                                 /*tombstone_require_full_compaction=*/false);

  EXPECT_EQ(Filter(filter_a, MakeKey("k1", 50), MakeValue(pb::store::Op::Delete), &skip_a),
            Filter(filter_b, MakeKey("k1", 50), MakeValue(pb::store::Op::Delete), &skip_b));
  EXPECT_EQ(skip_a, skip_b);

  EXPECT_EQ(Filter(filter_a, MakeKey("k1", 30), MakeValue(pb::store::Op::Put), &skip_a),
            Filter(filter_b, MakeKey("k1", 30), MakeValue(pb::store::Op::Put), &skip_b));
  EXPECT_EQ(skip_a, skip_b);
}

// commit_ts <= 0 defense: an anomalous ts=0 entry at/below safe_point must be kept (never crash the
// compaction thread by feeding a negative ts to the encoder).
TEST_F(WriteCompactionFilterTest, CommitTsZeroDefensiveKeep) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/true,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 0), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
  EXPECT_TRUE(skip_until.empty());
}

// User_key boundary fully resets remove_older_ AND seen_put_or_delete_above_sp_: after k1 reaches
// remove_older_, k2's first Delete must still be evaluated by the gate (kept on a partial compaction),
// proving the per-key state is reset rather than carried over.
TEST_F(WriteCompactionFilterTest, UserKeyBoundaryResetsTombstoneState) {
  WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/false,
                               /*tombstone_require_full_compaction=*/true);
  std::string skip_until;

  // k1: Put@50 kept -> remove_older_=true; Put@40 dropped.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 50), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
  EXPECT_EQ(Filter(filter, MakeKey("k1", 40), MakeValue(pb::store::Op::Put), &skip_until),
            Decision::kRemoveAndSkipUntil);

  // k2 boundary: state reset, first Delete must hit the gate -> kept on this partial compaction.
  EXPECT_EQ(Filter(filter, MakeKey("k2", 50), MakeValue(pb::store::Op::Delete), &skip_until), Decision::kKeep);
}

// ===========================================================================================
// Group E: factory-level flag snapshot.
// The constructor takes the policy as a value snapshot, but the factory reads
// FLAGS_gc_compaction_filter_tombstone_require_full_compaction at creation time. This test documents
// (and restores) that flag. DECLARE must live inside namespace dingodb (the flag is DEFINEd there).
// ===========================================================================================

DECLARE_bool(gc_compaction_filter_tombstone_require_full_compaction);

// Flipping the global flag then constructing a filter with the snapshotted value must reproduce the
// gate behavior. We restore the flag afterwards (FlagGuard-style).
TEST_F(WriteCompactionFilterTest, FactoryFlagSnapshotReproducesGate) {
  const bool saved = FLAGS_gc_compaction_filter_tombstone_require_full_compaction;

  // Simulate the factory snapshotting flag=true on a partial compaction -> marker kept.
  FLAGS_gc_compaction_filter_tombstone_require_full_compaction = true;
  {
    WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/false,
                                 FLAGS_gc_compaction_filter_tombstone_require_full_compaction);
    std::string skip_until;
    EXPECT_EQ(Filter(filter, MakeKey("k1", 50), MakeValue(pb::store::Op::Delete), &skip_until), Decision::kKeep);
  }

  // Simulate the factory snapshotting flag=false on a partial compaction -> marker removed (DANGEROUS).
  FLAGS_gc_compaction_filter_tombstone_require_full_compaction = false;
  {
    WriteCompactionFilter filter(/*safe_point=*/100, /*is_full_compaction=*/false,
                                 FLAGS_gc_compaction_filter_tombstone_require_full_compaction);
    std::string skip_until;
    EXPECT_EQ(Filter(filter, MakeKey("k1", 50), MakeValue(pb::store::Op::Delete), &skip_until),
              Decision::kRemoveAndSkipUntil);
  }

  // Restore the global flag to its pre-test value.
  FLAGS_gc_compaction_filter_tombstone_require_full_compaction = saved;
}

}  // namespace dingodb
