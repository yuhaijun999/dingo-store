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
// at the same safe_point, so the filter never deletes anything the raft GC would keep. We therefore
// replicate DoGcCoreTxn's decision logic exactly (see write_compaction_filter.cc).
//
// Baseline simplifications (full version restores them later):
//   1. safe_point is taken from tenant 0 only (no cross-tenant min aggregation).
//   2. Orphan data CF cleanup is NOT done here; orphan large values are reclaimed by the retained
//      raft GC backup. The filter only evaporates write CF versions.
//   3. RocksDB snapshot / read-path consistency (IgnoreSnapshots side effects) is shelved; baseline
//      is for performance measurement on a test cluster only, not for production rollout.

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
  explicit WriteCompactionFilter(int64_t safe_point) : safe_point_(safe_point) {}

  const char* Name() const override { return "DingoWriteCompactionFilter"; }

  rocksdb::CompactionFilter::Decision FilterV2(int level, const rocksdb::Slice& key,
                                               rocksdb::CompactionFilter::ValueType value_type,
                                               const rocksdb::Slice& existing_value, std::string* new_value,
                                               std::string* skip_until) const override;

 private:
  // safe_point captured when this filter instance was created (see factory). Versions with
  // commit_ts > safe_point_ are always kept.
  const int64_t safe_point_;

  // Current user_key being processed; used to detect a user_key boundary and reset the per-key state.
  mutable std::string mvcc_key_prefix_;

  // The two state flags below mirror DoGcCoreTxn exactly (txn_engine_helper.cc):
  //   seen_put_or_delete_above_sp_  <-> is_exist_put_or_delete_key_if_write_ts_gt_safe_point_ts
  //   first_below_sp_unconsumed_    <-> is_first_put_key_if_write_ts_le_safe_point_ts
  // Together they implement the "the first valid version at/below safe_point must be kept" rule, but
  // only when there is NO newer Put/Delete above safe_point.
  mutable bool seen_put_or_delete_above_sp_ = false;
  mutable bool first_below_sp_unconsumed_ = true;
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
