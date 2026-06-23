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

// [GC-Tombstone baseline step-2] Write CF compaction filter.
//
// Goal: drop expired MVCC versions (commit_ts <= safe_point) directly during RocksDB compaction,
// using Decision::kRemoveAndSkipUntil so that NO tombstone is produced (unlike kRemove, which would
// leave an internal tombstone at non-bottommost levels). This is the core mechanism that turns the
// current "GC writes N point tombstones" into "0 tombstone".
//
// The deletion decision MUST be a subset of what the existing raft-based GC (DoGcCoreTxn) would delete
// at the same safe_point, so the filter never deletes anything the raft GC would keep.
//
// Decision model (refactored after TiKV's do_filter remove_older state machine; see
// TiKV-WriteCompactionFilter-三类Write处理.md). For each user_key, versions are visited newest->oldest:
//   - Put  (first effective version at/below safe_point): kept (unless a newer Put/Delete exists above
//           safe_point, then dropped to match DoGcCoreTxn), then remove_older_ = true.
//   - Delete (first effective version): see the tombstone gate below; remove_older_ = true.
//   - Rollback / Lock / unknown: noise / not-deleted-by-DoGcCoreTxn; do NOT set remove_older_.
//   - any version once remove_older_ == true: dropped via kRemoveAndSkipUntil (zero tombstone).
//
// Tombstone safety (the key difference from a naive port): a Delete marker is only physically removed
// when the compaction's view is COMPLETE (is_full_compaction), because C++ RocksDB 10.5.1 has no
// is_bottommost_level(). On a partial compaction the marker is kept to shadow any lower-level old
// version (preventing resurrection); its in-view older versions are still reclaimed. Gated by
// FLAGS_gc_compaction_filter_tombstone_require_full_compaction (default true). See its DECLARE comment.
//
// Out of scope (still handled only by the retained raft GC backup):
//   1. safe_point is taken from tenant 0 only (no cross-tenant min aggregation).
//   2. Orphan data CF cleanup (orphan large values reclaimed by raft GC). The filter only touches write CF.
//   3. lock CF and GcKeys delegation to a GC worker are not implemented.

#ifndef DINGODB_ENGINE_WRITE_COMPACTION_FILTER_H_  // NOLINT
#define DINGODB_ENGINE_WRITE_COMPACTION_FILTER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "gflags/gflags.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"

namespace dingodb {

// Master switch (default off). When false, the factory registers no filter (CF behaves exactly as
// before). DANGEROUS: when on, the filter physically drops expired MVCC versions during compaction.
DECLARE_bool(gc_enable_compaction_filter);

// Tombstone safety gate (default true = safe). RocksDB 10.5.1's C++ CompactionFilter::Context has no
// is_bottommost_level(), so we cannot replicate TiKV's "keep the Delete marker at non-bottommost,
// delete it only at bottommost" scheme directly. Instead we use is_full_compaction as the equivalent
// "complete view" signal: a full compaction includes ALL table files for the key range, so every MVCC
// version of a user_key is in this compaction's input -> deleting the Delete marker cannot resurrect a
// lower-level version. When true (default), the filter only physically removes a Delete marker during a
// full compaction; otherwise it keeps the marker (still shadowing any lower-level old version) and only
// reclaims the versions BELOW it that are in this compaction's view. When false, the marker is removed
// unconditionally (the old, DANGEROUS behavior that can resurrect deleted data on partial compactions;
// kept only for A/B comparison and debugging).
DECLARE_bool(gc_compaction_filter_tombstone_require_full_compaction);

// Process-level safe_point shared with the filter. Refreshed every GC poll round from tenant 0's
// safe_point (RegularDoGcHandler), monotonically increasing. The filter only drops versions whose
// commit_ts <= this value. 0 means "not ready / do not GC".
extern std::atomic<int64_t> g_compaction_filter_safe_point;

// Process-level gc_stop flag for tenant 0. When true (e.g. a backup is holding GC), the factory
// returns no filter so compaction stops reclaiming. Mirrors the existing raft GC's gc_stop semantics.
extern std::atomic<bool> g_compaction_filter_gc_stop;

// Compaction filter that runs the GC decision while RocksDB rewrites write CF SST files.
//
// One instance is created per compaction (single-threaded use), so the per-user-key state members
// are plain mutable fields (FilterV2 is const in the RocksDB API, hence `mutable`). All members are
// initialized in-place to avoid UB.
class WriteCompactionFilter : public rocksdb::CompactionFilter {
 public:
  // is_full_compaction comes from the compaction Context (see factory). tombstone_require_full_compaction
  // is a snapshot of FLAGS_gc_compaction_filter_tombstone_require_full_compaction taken at construction,
  // so a mid-compaction flag flip cannot make different versions of the same user_key use inconsistent
  // tombstone policy.
  WriteCompactionFilter(int64_t safe_point, bool is_full_compaction, bool tombstone_require_full_compaction)
      : safe_point_(safe_point),
        is_full_compaction_(is_full_compaction),
        tombstone_require_full_compaction_(tombstone_require_full_compaction) {}

  const char* Name() const override { return "DingoWriteCompactionFilter"; }

  rocksdb::CompactionFilter::Decision FilterV2(int level, const rocksdb::Slice& key,
                                               rocksdb::CompactionFilter::ValueType value_type,
                                               const rocksdb::Slice& existing_value, std::string* new_value,
                                               std::string* skip_until) const override;

 private:
  // safe_point captured when this filter instance was created (see factory). Versions with
  // commit_ts > safe_point_ are always kept.
  const int64_t safe_point_;

  // Whether this compaction includes ALL table files for the key range (Context.is_full_compaction).
  // Only then is it safe to physically remove a Delete marker (no lower-level version can resurrect).
  const bool is_full_compaction_;

  // Snapshot of the tombstone safety flag (see header DECLARE comment). When true, a Delete marker is
  // removed only if is_full_compaction_; when false, removed unconditionally (DANGEROUS, old behavior).
  const bool tombstone_require_full_compaction_;

  // Current user_key being processed; used to detect a user_key boundary and reset the per-key state.
  mutable std::string mvcc_key_prefix_;

  // remove_older_: TiKV-style state machine bit. Set true once the first effective version (Put, or a
  // Delete marker) at/below safe_point has been decided for this user_key; every OLDER version is then a
  // redundant historical version and is dropped via kRemoveAndSkipUntil. Replaces the old
  // first_below_sp_unconsumed_ flag (remove_older_ == true means "first effective version consumed").
  mutable bool remove_older_ = false;

  // seen_put_or_delete_above_sp_ <-> DoGcCoreTxn's is_exist_put_or_delete_key_if_write_ts_gt_safe_point_ts.
  // Kept to preserve dingo's "drop the first Put at/below safe_point when a newer Put/Delete exists above
  // safe_point" semantics (keeps the filter's delete set a subset of DoGcCoreTxn's).
  mutable bool seen_put_or_delete_above_sp_ = false;
};

// Stateless factory. Created once and shared by all write CF compactions. CreateCompactionFilter is
// called by each compaction thread; it returns nullptr (no GC this compaction) when the feature is
// off, safe_point is not ready, or GC is stopped.
class WriteCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  WriteCompactionFilterFactory() = default;

  const char* Name() const override { return "DingoWriteCompactionFilterFactory"; }

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_WRITE_COMPACTION_FILTER_H_  // NOLINT
