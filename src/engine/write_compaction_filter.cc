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

#include "engine/write_compaction_filter.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "gflags/gflags.h"
#include "mvcc/codec.h"
#include "proto/store.pb.h"

namespace dingodb {

// Master switch for the write CF compaction filter GC. Default OFF: when false, the factory installs
// no filter, so the write CF behaves exactly as before. When ON, the filter physically drops expired
// MVCC versions during compaction (DANGEROUS: it deletes data). Baseline is test-cluster only.
DEFINE_bool(gc_enable_compaction_filter, false,
            "enable write CF compaction filter GC (DANGEROUS, deletes expired MVCC versions during "
            "compaction); baseline is for test clusters only");

// Tombstone safety gate. Default true (safe). See header for the full rationale: RocksDB 10.5.1 C++
// has no is_bottommost_level(), so we gate physical removal of a Delete marker on is_full_compaction
// (the only reliable "complete view" signal) to prevent resurrecting a lower-level old version. Set to
// false to restore the old unconditional marker removal (DANGEROUS, partial-compaction resurrection;
// for A/B comparison and debugging only).
DEFINE_bool(gc_compaction_filter_tombstone_require_full_compaction, true,
            "only physically remove a Delete tombstone during a full compaction (default true, safe); "
            "false restores the old unconditional removal (DANGEROUS, can resurrect deleted data)");

std::atomic<int64_t> g_compaction_filter_safe_point{0};
std::atomic<bool> g_compaction_filter_gc_stop{false};

rocksdb::CompactionFilter::Decision WriteCompactionFilter::FilterV2(int /*level*/, const rocksdb::Slice& key,
                                                                    rocksdb::CompactionFilter::ValueType value_type,
                                                                    const rocksdb::Slice& existing_value,
                                                                    std::string* /*new_value*/,
                                                                    std::string* skip_until) const {
  using Decision = rocksdb::CompactionFilter::Decision;

  // Only plain values participate. Blob index / merge operands / wide columns are kept untouched.
  if (value_type != rocksdb::CompactionFilter::ValueType::kValue) {
    return Decision::kKeep;
  }

  // Decode the write CF key into (user_key, commit_ts). NOTE: never FATAL inside a compaction thread
  // (DoGcCoreTxn FATALs on decode failure, but here that would crash the whole DB). On failure we
  // conservatively keep the entry.
  std::string user_key;
  int64_t commit_ts = 0;
  if (!mvcc::Codec::DecodeKey(std::string_view(key.data(), key.size()), user_key, commit_ts)) {
    return Decision::kKeep;
  }

  // Parse the WriteInfo value once. Conservatively keep on parse failure.
  pb::store::WriteInfo write_info;
  if (!write_info.ParseFromArray(existing_value.data(), existing_value.size())) {
    return Decision::kKeep;
  }
  const pb::store::Op op = write_info.op();

  // User_key boundary: reset the per-key decision state. Iteration order is "newest -> oldest" within a
  // user_key (commit_ts is descending-encoded), so a version above safe_point is always seen before any
  // version at/below it.
  if (user_key != mvcc_key_prefix_) {
    mvcc_key_prefix_ = user_key;
    remove_older_ = false;
    seen_put_or_delete_above_sp_ = false;
  }

  // Versions newer than safe_point are never dropped. Just remember whether a Put/Delete exists above
  // safe_point for this user_key (mirrors is_exist_put_or_delete_key_if_write_ts_gt_safe_point_ts).
  if (commit_ts > safe_point_) {
    if (op == pb::store::Op::Put || op == pb::store::Op::Delete) {
      seen_put_or_delete_above_sp_ = true;
    }
    return Decision::kKeep;
  }

  // Defensive: skip_until below uses EncodeKey(user_key, commit_ts - 1), and the ts encoder requires a
  // non-negative ts (WriteLongWithNegation CHECKs value >= 0). A real write CF commit_ts is always > 0,
  // but guard against an anomalous 0 to avoid crashing the compaction thread. Keeping is always safe.
  if (commit_ts <= 0) {
    return Decision::kKeep;
  }

  // commit_ts <= safe_point_ : candidate for GC. The decision mirrors TiKV's do_filter remove_older
  // state machine, constrained to stay a SUBSET of what DoGcCoreTxn would delete at the same safe_point.

  // remove_older_ == true: an effective version (Put, or a Delete marker) at/below safe_point has already
  // been decided for this user_key, so EVERY older version here is redundant historical data. It is safe
  // to drop it via kRemoveAndSkipUntil at ANY compaction level (full or not): the kept newer version
  // shadows it, so dropping it (and any lower-level copy) can never resurrect anything.
  if (remove_older_) {
    switch (op) {
      case pb::store::Put:
      case pb::store::Delete:
        // kRemoveAndSkipUntil drops WITHOUT leaving a tombstone (kRemove would leave an internal
        // tombstone at non-bottommost levels). skip_until points to the next-older version boundary of
        // the same user_key; correctness relies on descending ts encoding: commit_ts-1 encodes to larger
        // bytes than commit_ts, so skip_until > key and stays within the same user_key (the fixed-width
        // 8-byte ts segment never carries into the user_key bytes).
        *skip_until = mvcc::Codec::EncodeKey(std::string_view(user_key), commit_ts - 1);
        return Decision::kRemoveAndSkipUntil;
      case pb::store::Rollback:
        // Noise record. Drop self only (kRemove); it does not extend the skip range.
        return Decision::kRemove;
      default:
        // Lock / PutIfAbsent / None / unknown: DoGcCoreTxn does NOT delete these. Keep to stay a subset.
        return Decision::kKeep;
    }
  }

  // remove_older_ == false: this is the first record at/below safe_point pending a decision.
  switch (op) {
    case pb::store::Put: {
      // The first effective version is consumed: older versions become redundant from here on.
      remove_older_ = true;
      // Keep the first Put at/below safe_point (so a read at safe_point still sees it), but ONLY when
      // there is no newer Put/Delete above safe_point. Otherwise this Put is redundant (a newer version
      // above takes over visibility), and DoGcCoreTxn drops it too -- so we drop it to stay in lockstep.
      if (!seen_put_or_delete_above_sp_) {
        return Decision::kKeep;
      }
      *skip_until = mvcc::Codec::EncodeKey(std::string_view(user_key), commit_ts - 1);
      return Decision::kRemoveAndSkipUntil;
    }
    case pb::store::Delete: {
      // The first effective version is consumed: older versions become redundant from here on.
      remove_older_ = true;
      // Tombstone safety: physically removing the Delete marker is only safe when this compaction's view
      // covers ALL versions of this user_key (is_full_compaction_). On a partial (e.g. leveled) compaction
      // an older Put may live in a lower level NOT in this compaction's input; removing the marker would
      // un-shadow it and resurrect deleted data. C++ RocksDB 10.5.1 has no is_bottommost_level(), so
      // is_full_compaction is the equivalent "complete view" signal. When the flag is off the marker is
      // removed unconditionally (DANGEROUS, old behavior). NOTE: only removing the marker ITSELF needs the
      // gate -- older versions below it are still reclaimed via the remove_older_ branch above (the kept
      // marker shadows them), so non-full compactions still get the zero-tombstone reclaim of old data.
      const bool may_remove_tombstone = is_full_compaction_ || !tombstone_require_full_compaction_;
      if (may_remove_tombstone) {
        *skip_until = mvcc::Codec::EncodeKey(std::string_view(user_key), commit_ts - 1);
        return Decision::kRemoveAndSkipUntil;
      }
      // Partial compaction: keep the marker to shadow any lower-level old version. Its in-view older
      // versions are dropped by the remove_older_ branch on subsequent callbacks.
      return Decision::kKeep;
    }
    case pb::store::Rollback: {
      // Standalone rollback record: pure noise, no data value. Drop self (kRemove) but do NOT set
      // remove_older_ -- otherwise the real Put/Delete below it would be wrongly treated as redundant.
      return Decision::kRemove;
    }
    default: {
      // Lock / PutIfAbsent / None / unknown: DoGcCoreTxn does NOT delete these (it only logs an error).
      // Keep them (and do NOT set remove_older_) so the filter's delete set stays a subset of raft GC.
      return Decision::kKeep;
    }
  }
}

std::unique_ptr<rocksdb::CompactionFilter> WriteCompactionFilterFactory::CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context) {
  // Feature off -> no GC this compaction (equivalent to all-Keep).
  if (!FLAGS_gc_enable_compaction_filter) {
    return nullptr;
  }
  // GC stopped (e.g. backup holding GC) -> do not reclaim.
  if (g_compaction_filter_gc_stop.load(std::memory_order_acquire)) {
    return nullptr;
  }
  // safe_point not ready -> do not GC.
  const int64_t safe_point = g_compaction_filter_safe_point.load(std::memory_order_acquire);
  if (safe_point <= 0) {
    return nullptr;
  }
  // Snapshot the tombstone policy flag once per compaction so all versions of a user_key in this
  // compaction use a consistent policy even if the flag is flipped mid-run. is_full_compaction tells the
  // filter whether this compaction's view is complete (safe to remove Delete markers). is_manual_compaction
  // and input_start_level are intentionally unused: neither implies a complete view of all MVCC versions.
  return std::make_unique<WriteCompactionFilter>(safe_point, context.is_full_compaction,
                                                 FLAGS_gc_compaction_filter_tombstone_require_full_compaction);
}

}  // namespace dingodb
