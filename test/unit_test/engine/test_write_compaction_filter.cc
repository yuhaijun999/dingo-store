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

// [GC-Tombstone baseline step-2] Unit tests for WriteCompactionFilter decision logic. These tests
// exercise the FilterV2 state machine and skip_until construction. NOTE: not run here (the user
// compiles/runs the tests themselves), they only document the expected behavior.

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

// Non-Value entries (blob index / merge operand) are always kept.
TEST_F(WriteCompactionFilterTest, NonValueKept) {
  WriteCompactionFilter filter(100);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 50), MakeValue(pb::store::Op::Put), &skip_until, ValueType::kBlobIndex),
            Decision::kKeep);
}

// Versions newer than safe_point are always kept.
TEST_F(WriteCompactionFilterTest, AboveSafePointKept) {
  WriteCompactionFilter filter(100);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 200), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
}

// Single key, no version above safe_point: the newest Put at/below safe_point is kept, older ones are
// removed via kRemoveAndSkipUntil, and skip_until == EncodeKey(user_key, commit_ts-1) (same user_key).
TEST_F(WriteCompactionFilterTest, FirstPutKeptOlderRemoved) {
  WriteCompactionFilter filter(100);
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
  WriteCompactionFilter filter(100);
  std::string skip_until;

  // Put above safe_point: kept, records that a newer Put/Delete exists.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 200), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);

  // Newest Put at/below safe_point is no longer protected -> removed.
  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Put), &skip_until), Decision::kRemoveAndSkipUntil);
  EXPECT_EQ(skip_until, MakeKey("k1", 89));
}

// The first Delete at/below safe_point is itself removed (the marker is dropped).
TEST_F(WriteCompactionFilterTest, FirstDeleteRemoved) {
  WriteCompactionFilter filter(100);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Delete), &skip_until),
            Decision::kRemoveAndSkipUntil);
  EXPECT_EQ(skip_until, MakeKey("k1", 89));
}

// A standalone Rollback at/below safe_point is removed via kRemove.
TEST_F(WriteCompactionFilterTest, RollbackRemoved) {
  WriteCompactionFilter filter(100);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Rollback), &skip_until), Decision::kRemove);
}

// Lock is NOT deleted (aligns with DoGcCoreTxn, which only logs and keeps it).
TEST_F(WriteCompactionFilterTest, LockKept) {
  WriteCompactionFilter filter(100);
  std::string skip_until;
  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Lock), &skip_until), Decision::kKeep);
}

// A key that fails to decode is conservatively kept (the filter must never crash a compaction thread).
TEST_F(WriteCompactionFilterTest, DecodeFailureKept) {
  WriteCompactionFilter filter(100);
  std::string skip_until;
  std::string bad_key = "short";  // shorter than the minimum encoded key length
  EXPECT_EQ(Filter(filter, bad_key, MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
}

// commit_ts == 1 boundary: skip_until = EncodeKey(user_key, 0), which must be valid and not carry over.
TEST_F(WriteCompactionFilterTest, CommitTsOneBoundary) {
  WriteCompactionFilter filter(100);
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
  WriteCompactionFilter filter(100);
  std::string skip_until;

  EXPECT_EQ(Filter(filter, MakeKey("k1", 90), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
  EXPECT_EQ(Filter(filter, MakeKey("k1", 80), MakeValue(pb::store::Op::Put), &skip_until),
            Decision::kRemoveAndSkipUntil);

  // New user_key: its newest put at/below safe_point is protected again.
  EXPECT_EQ(Filter(filter, MakeKey("k2", 90), MakeValue(pb::store::Op::Put), &skip_until), Decision::kKeep);
}

}  // namespace dingodb
