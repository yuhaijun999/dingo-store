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

// [GC-Tombstone refactor B2/B3/B5] Score-based active compaction scheduler (write CF only).
//
// This replaces the old "lexicographic walk + compact whatever just got raft-GC'd" with a TiKV-style
// two-phase scheduler:
//   1. CollectCompactionCandidates: evaluate every region (ALL replicas on this node, not leader-only --
//      see tikv-compaction 4.7) by SST garbage, keep the Top-N in a min-heap
//      (capacity = gc_active_compaction_max_per_round), return them descending by score.
//   2. CompactCandidates: walk descending, drop on deadline, per-region debounce, re-evaluate just
//      before issuing (double-check), then a synchronous CompactRange on the write CF.
//
// The scheduler is decoupled from raft GC: a candidate is selected purely by its SST garbage estimate,
// so cold regions (no recent raft-GC / safe_point not advanced) are still picked up when their SSTs
// hold enough reclaimable garbage. It runs on its own crontab task (ActiveCompactionHandler) gated by
// gc_active_compaction_score_based; when that flag is off the old inline path in RegularDoGcHandler is
// used instead (one-flag hot rollback).
//
// Scope: write CF (Constant::kTxnWriteCF) only. default/lock CF active compaction and SST boundary
// alignment (CompactionGuard) are out of scope -> see TODOs.

#ifndef DINGODB_ENGINE_ACTIVE_COMPACTION_SCHEDULER_H_
#define DINGODB_ENGINE_ACTIVE_COMPACTION_SCHEDULER_H_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "engine/raw_engine.h"
#include "meta/store_meta_manager.h"  // store::Region / store::RegionPtr

namespace dingodb {

// One scored region candidate for active compaction.
struct CompactionCandidate {
  double score = 0.0;                 // GetCompactScore output; > 0 means worth compacting
  uint64_t num_total_entries = 0;     // total entries across the region's write CF SSTs
  uint64_t num_tombstones = 0;        // RocksDB tombstones (phase-A add-on signal)
  uint64_t num_discardable = 0;       // estimated discardable MVCC records (phase-B main signal)
  int64_t region_id = 0;
  std::string start_key;  // RAW region start bound ("" = open); encoding happens at compact time
  std::string end_key;    // RAW region end bound ("" = open)
};

class ActiveCompactionScheduler {
 public:
  // Score formula mirroring TiKV get_compact_score, branching on the compaction filter:
  //   filter off: signal = num_tombstones; gate by tombstones_num/percent thresholds; score = tombstones*ratio
  //   filter on : signal = num_discardable; gate by redundant_rows_num/percent thresholds; score = discardable*ratio
  // Phase-A (no collector data) feeds num_discardable=0 and the filter-off branch is forced via
  // enable_filter=false; phase-B (collector on) passes real num_discardable and enable_filter=true. The
  // same formula serves both, so A->B is smooth.
  static double GetCompactScore(uint64_t num_tombstones, uint64_t num_discardable, uint64_t num_total_entries,
                                bool enable_filter);

  // Evaluate one region's write CF garbage. Returns nullopt when the engine does not support property
  // reads (xdprocks ENOT_SUPPORT), the read fails, or the score is <= 0 (below threshold). Never FATAL.
  static std::optional<CompactionCandidate> EvaluateRangeCandidate(const RawEnginePtr& raw_engine,
                                                                   const store::RegionPtr& region,
                                                                   int64_t gc_safe_point);

  // Phase one: evaluate all given regions (all replicas on this node), keep Top-N (heap_capacity) by
  // score, return descending by score.
  static std::vector<CompactionCandidate> CollectCompactionCandidates(
      const RawEnginePtr& raw_engine, const std::vector<store::RegionPtr>& regions, int64_t gc_safe_point,
      size_t heap_capacity);

  // Phase two: compact candidates descending by score. Stops once the wall clock passes round_deadline_ms
  // (remaining candidates dropped, recorded in bvar). Per-region debounce via min interval. Re-evaluates
  // each candidate with the latest safe_point just before issuing; if the score has dropped to 0 it is
  // skipped (recorded in bvar). issued counts CompactRange calls actually issued this round.
  static void CompactCandidates(const RawEnginePtr& raw_engine, std::vector<CompactionCandidate>& candidates,
                                int64_t round_deadline_ms, int& issued);
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ACTIVE_COMPACTION_SCHEDULER_H_
