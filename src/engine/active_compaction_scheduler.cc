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

#include "engine/active_compaction_scheduler.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "bvar/bvar.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "engine/mvcc_properties_collector.h"  // DECLARE_bool(gc_enable_mvcc_properties_collector)
#include "engine/write_compaction_filter.h"    // g_compaction_filter_safe_point
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "mvcc/codec.h"

namespace dingodb {

// ---- gflags owned by txn_engine_helper.cc (DEFINE there to share the same gflag block / validators) ----
DECLARE_bool(gc_active_compaction_force_bottommost);
DECLARE_int64(gc_active_compaction_max_per_round);
DECLARE_int64(gc_active_compaction_min_interval_s);
DECLARE_int64(gc_active_compaction_tombstones_num_threshold);
DECLARE_int64(gc_active_compaction_tombstones_percent_threshold);
DECLARE_int64(gc_active_compaction_redundant_rows_threshold);
DECLARE_int64(gc_active_compaction_redundant_rows_percent_threshold);
DECLARE_int64(gc_active_compaction_cold_sst_seconds);
DECLARE_bool(gc_enable_mvcc_properties_collector);
DECLARE_bool(dingo_log_switch_txn_gc_detail);

namespace {

// [B3 bvar] Active-compaction observability (mirrors the dingo_txn_scan_* bvar style). All counters are
// process-wide adders; the round duration / blocked time are latency recorders.
bvar::Adder<int64_t> g_active_compaction_pending_candidates("dingo_active_compaction_pending_candidates");
bvar::Adder<int64_t> g_active_compaction_skipped_on_recheck("dingo_active_compaction_skipped_on_recheck");
bvar::Adder<int64_t> g_active_compaction_issued("dingo_active_compaction_issued");
bvar::Adder<int64_t> g_active_compaction_force_bottommost_issued("dingo_active_compaction_force_bottommost_issued");
bvar::Adder<int64_t> g_active_compaction_dropped_on_deadline("dingo_active_compaction_dropped_on_deadline");
bvar::LatencyRecorder g_active_compaction_round_duration_ms("dingo_active_compaction_round_duration_ms");
bvar::LatencyRecorder g_active_compaction_compact_blocked_ms("dingo_active_compaction_compact_blocked_ms");

// Per-region debounce state. CompactCandidates runs single-instance (ActiveCompactionHandler guard),
// but EvaluateRangeCandidate / state access can race with the old inline path in theory, so guard with
// a mutex. The map is small (one entry per ever-compacted region).
struct ActiveCompactionState {
  int64_t last_compaction_time_ms = 0;     // when CompactRange was last issued for this region
  int64_t last_compaction_safe_point = 0;  // safe_point used at that compaction (debounce add-on)
};
std::mutex g_active_compaction_state_mutex;
std::map<int64_t, ActiveCompactionState> g_active_compaction_state;  // region_id -> state

// Estimate how many of `count` records with commit_ts in [oldest_ts, newest_ts] fall below gc_safe_point
// (and are therefore discardable). Mirrors TiKV get_estimated_discardable_entries: assume the timestamps
// are uniformly distributed across [oldest, newest] and linearly interpolate the fraction below the
// safe_point. Conservative edge handling: 0 records / no ts -> 0; whole range below safe_point -> all.
uint64_t GetEstimatedDiscardableEntries(uint64_t count, int64_t oldest_ts, int64_t newest_ts,
                                        int64_t gc_safe_point) {
  if (count == 0 || oldest_ts <= 0 || newest_ts <= 0 || gc_safe_point <= 0) {
    return 0;
  }
  if (newest_ts <= gc_safe_point) {
    return count;  // every record is older than the safe_point
  }
  if (oldest_ts > gc_safe_point) {
    return 0;  // none discardable yet
  }
  if (newest_ts <= oldest_ts) {
    // [M1] Degenerate / inverted range: TiKV get_estimated_discardable_entries returns 0 here
    // (compaction_runner.rs:685 `oldest_ts >= newest_ts -> return 0`). A degenerate span carries no
    // distribution information, so be conservative and count nothing rather than over-estimating
    // discardable entries (which would inflate the score and cause needless compactions). Even though
    // the earlier safe_point guards usually cover this case, we align the direction with TiKV.
    return 0;
  }
  // Linear interpolation: fraction of the [oldest, newest] span that lies at/below the safe_point.
  const double fraction =
      static_cast<double>(gc_safe_point - oldest_ts) / static_cast<double>(newest_ts - oldest_ts);
  double est = static_cast<double>(count) * fraction;
  if (est < 0) {
    est = 0;
  }
  if (est > static_cast<double>(count)) {
    est = static_cast<double>(count);
  }
  return static_cast<uint64_t>(est);
}

// Encode raw region bounds into physical keys for CompactRange / GetRangeTableProperties. An empty bound
// (first / last region) must stay empty (open bound); never EncodeBytes an empty key (it returns a
// non-empty 9-byte sentinel).
std::string EncodeBoundOrEmpty(const std::string& raw_bound) {
  return raw_bound.empty() ? std::string() : mvcc::Codec::EncodeBytes(raw_bound);
}

}  // namespace

double ActiveCompactionScheduler::GetCompactScore(uint64_t num_tombstones, uint64_t num_discardable,
                                                  uint64_t num_total_entries, bool enable_filter) {
  if (num_total_entries == 0) {
    return 0.0;
  }

  if (!enable_filter) {
    // Phase-A / filter-off branch: RocksDB tombstones are the main signal. NOTE (reviewer MF1): dingo
    // Delete markers are op=Delete WriteInfo Puts, NOT RocksDB tombstones, so num_tombstones is only an
    // add-on here; the phase-A main driver (num_entries + cold-SST time) is applied in
    // EvaluateRangeCandidate, which also feeds a synthetic num_tombstones for cold/large regions.
    // [M2] No ratio>1 risk here: num_tombstones is either props.num_deletions (built-in, same source as
    // num_total_entries, thus <= it) or the cold-region synthetic signal max(num_deletions, num_entries),
    // which is capped at num_entries by the caller. So ratio <= 1.0 already; no clamp needed.
    const double ratio = static_cast<double>(num_tombstones) / static_cast<double>(num_total_entries);
    const double percent = ratio * 100.0;
    if (num_tombstones < static_cast<uint64_t>(FLAGS_gc_active_compaction_tombstones_num_threshold) &&
        percent < static_cast<double>(FLAGS_gc_active_compaction_tombstones_percent_threshold)) {
      return 0.0;  // below threshold -> not selected
    }
    return static_cast<double>(num_tombstones) * ratio;
  }

  // Phase-B / filter-on branch: discardable redundant versions are the main signal; tombstones count
  // only toward the ratio numerator (mirrors TiKV).
  // [M2] num_tombstones (built-in TableProperties::num_deletions) and num_discardable (derived from the
  // MvccPropertiesCollector's num_versions/num_deletes) come from DIFFERENT sources than num_total_entries
  // (built-in num_entries). Their sum can therefore exceed num_total_entries, which would make ratio > 1
  // and inflate the score (needless compactions). TiKV guards this in get_compact_score
  // (compaction_runner.rs:714 `num_total_entries < num_discardable -> return 0.0`). Here we clamp both the
  // ratio numerator and the discardable count used for the score to <= num_total_entries so ratio <= 1.0,
  // while keeping the threshold check on the raw num_discardable unchanged.
  const uint64_t clamped_numerator =
      std::min<uint64_t>(num_tombstones + num_discardable, num_total_entries);
  const uint64_t clamped_discardable = std::min<uint64_t>(num_discardable, num_total_entries);
  const double ratio = static_cast<double>(clamped_numerator) / static_cast<double>(num_total_entries);
  const double percent = ratio * 100.0;
  if (num_discardable < static_cast<uint64_t>(FLAGS_gc_active_compaction_redundant_rows_threshold) &&
      percent < static_cast<double>(FLAGS_gc_active_compaction_redundant_rows_percent_threshold)) {
    return 0.0;  // below threshold -> not selected
  }
  return static_cast<double>(clamped_discardable) * ratio;
}

std::optional<CompactionCandidate> ActiveCompactionScheduler::EvaluateRangeCandidate(
    const RawEnginePtr& raw_engine, const store::RegionPtr& region, int64_t gc_safe_point) {
  if (raw_engine == nullptr || region == nullptr) {
    return std::nullopt;
  }

  const int64_t region_id = region->Id();
  const std::string raw_start = region->Range().start_key();
  const std::string raw_end = region->Range().end_key();
  const std::string enc_start = EncodeBoundOrEmpty(raw_start);
  const std::string enc_end = EncodeBoundOrEmpty(raw_end);

  RangeTableProperties props;
  butil::Status status = raw_engine->GetRangeTableProperties(Constant::kTxnWriteCF, enc_start, enc_end, &props);
  if (!status.ok()) {
    // ENOT_SUPPORT on xdprocks, or a transient read failure -> conservatively skip this region. Never
    // FATAL on the compaction path.
    DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
        "[active_compaction][region({})] GetRangeTableProperties failed/unsupported: {}, skip", region_id,
        status.error_cstr());
    return std::nullopt;
  }

  if (props.num_entries == 0) {
    return std::nullopt;  // empty range, nothing to do
  }

  // Decide which signal branch to use. Phase-B requires both the collector gflag on AND collector data
  // actually present in the SSTs (num_versions > 0). Otherwise fall back to phase-A.
  const bool phase_b = FLAGS_gc_enable_mvcc_properties_collector && props.num_versions > 0;

  CompactionCandidate cand;
  cand.region_id = region_id;
  cand.start_key = raw_start;
  cand.end_key = raw_end;
  cand.num_total_entries = props.num_entries;
  cand.num_tombstones = props.num_deletions;

  double score = 0.0;
  if (phase_b) {
    // ---- Phase-B (B5): estimate discardable = discardable Delete markers + discardable stale versions,
    // both bounded by the safe_point. Uses the same safe_point the compaction filter uses, so the score
    // tracks what a CompactRange would actually reclaim. ----
    uint64_t discardable = 0;
    discardable += GetEstimatedDiscardableEntries(props.num_deletes, props.oldest_delete_ts,
                                                  props.newest_delete_ts, gc_safe_point);
    const uint64_t stale_versions = props.num_versions > props.num_rows ? props.num_versions - props.num_rows : 0;
    discardable += GetEstimatedDiscardableEntries(stale_versions, props.oldest_stale_version_ts,
                                                  props.newest_stale_version_ts, gc_safe_point);
    cand.num_discardable = discardable;
    score = GetCompactScore(props.num_deletions, discardable, props.num_entries, /*enable_filter=*/true);
  } else {
    // ---- Phase-A (B2): no per-record MVCC stats. Main driver = num_entries + "SST not full-compacted
    // for a long time" (creation_time exposed as oldest_ancester_time). dingo Delete markers are Puts and
    // invisible to num_deletions, so we synthesize a num_tombstones-equivalent signal for the filter-off
    // formula: feed num_entries as the "garbage" when the region is large AND its SSTs are cold. This
    // lets the skeleton select large/cold regions even though pure num_deletions would be ~0. ----
    cand.num_discardable = 0;

    bool cold = false;
    if (FLAGS_gc_active_compaction_cold_sst_seconds > 0 && props.oldest_ancester_time > 0) {
      const int64_t now_s = Helper::Timestamp();  // unix seconds
      const int64_t age_s = now_s - static_cast<int64_t>(props.oldest_ancester_time);
      cold = age_s >= FLAGS_gc_active_compaction_cold_sst_seconds;
    }

    // Phase-A synthetic signal fed into the filter-off formula: real RocksDB tombstones (Rollback noise /
    // residual point deletes) plus, for cold regions, the whole entry count as a fallback so large cold
    // regions are picked up periodically. This is intentionally generous on cold regions and is bounded
    // by per-region min_interval + the cold_sst_seconds gate.
    uint64_t signal_tombstones = props.num_deletions;
    if (cold) {
      signal_tombstones = std::max(signal_tombstones, props.num_entries);
    }
    score = GetCompactScore(signal_tombstones, /*num_discardable=*/0, props.num_entries, /*enable_filter=*/false);
  }

  if (score <= 0.0) {
    return std::nullopt;  // below threshold
  }
  cand.score = score;
  return cand;
}

std::vector<CompactionCandidate> ActiveCompactionScheduler::CollectCompactionCandidates(
    const RawEnginePtr& raw_engine, const std::vector<store::RegionPtr>& regions, int64_t gc_safe_point,
    size_t heap_capacity) {
  std::vector<CompactionCandidate> heap;  // min-heap by score (smallest on top), capacity = heap_capacity
  if (heap_capacity == 0) {
    return {};
  }
  heap.reserve(heap_capacity);

  // Min-heap comparator: parent has the SMALLEST score so the weakest candidate is the one popped.
  const auto cmp = [](const CompactionCandidate& a, const CompactionCandidate& b) { return a.score > b.score; };

  for (const auto& region : regions) {
    auto cand_opt = EvaluateRangeCandidate(raw_engine, region, gc_safe_point);
    if (!cand_opt.has_value()) {
      continue;  // unsupported / failed / below threshold
    }
    const CompactionCandidate& cand = cand_opt.value();
    if (cand.score <= 0.0) {
      continue;
    }

    if (heap.size() < heap_capacity) {
      heap.push_back(cand);
      std::push_heap(heap.begin(), heap.end(), cmp);
    } else if (cand.score > heap.front().score) {
      // Replace the current weakest candidate.
      std::pop_heap(heap.begin(), heap.end(), cmp);
      heap.back() = cand;
      std::push_heap(heap.begin(), heap.end(), cmp);
    }
  }

  // Sort descending by score (best first) for the compact phase.
  std::sort(heap.begin(), heap.end(),
            [](const CompactionCandidate& a, const CompactionCandidate& b) { return a.score > b.score; });
  g_active_compaction_pending_candidates << static_cast<int64_t>(heap.size());
  return heap;
}

void ActiveCompactionScheduler::CompactCandidates(const RawEnginePtr& raw_engine,
                                                  std::vector<CompactionCandidate>& candidates,
                                                  int64_t round_deadline_ms, int& issued) {
  if (raw_engine == nullptr) {
    return;
  }

  const int64_t gc_safe_point = g_compaction_filter_safe_point.load(std::memory_order_acquire);

  for (const auto& cand : candidates) {
    if (issued >= FLAGS_gc_active_compaction_max_per_round) {
      break;  // per-round issue cap reached
    }
    // Per-round time budget: cannot interrupt the CompactRange currently blocking, only stop issuing the
    // NEXT one. Remaining candidates are dropped this round and re-scored next round.
    if (Helper::TimestampMs() >= round_deadline_ms) {
      g_active_compaction_dropped_on_deadline << 1;
      DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail)
          << "[active_compaction] round deadline reached, dropping remaining candidates";
      break;
    }

    const int64_t now_ms = Helper::TimestampMs();
    {
      std::lock_guard<std::mutex> lk(g_active_compaction_state_mutex);
      auto& state = g_active_compaction_state[cand.region_id];  // default {0,0}
      // Per-region minimum interval (debounce).
      if (state.last_compaction_time_ms != 0 &&
          now_ms - state.last_compaction_time_ms < FLAGS_gc_active_compaction_min_interval_s * 1000) {
        continue;
      }
    }

    // ---- Pre-compaction double-check: re-evaluate with the latest safe_point. If another compaction /
    // natural RocksDB compaction already cleared the garbage, the score drops to 0 -> skip. ----
    // Look up the region again is not needed: re-read its SST properties via a fresh EvaluateRangeCandidate
    // by rebuilding a temporary region lookup is heavier; instead recompute the score from fresh props.
    // We re-fetch properties through the same helper by constructing the encoded bounds directly.
    {
      const std::string enc_start = EncodeBoundOrEmpty(cand.start_key);
      const std::string enc_end = EncodeBoundOrEmpty(cand.end_key);
      RangeTableProperties props;
      butil::Status s = raw_engine->GetRangeTableProperties(Constant::kTxnWriteCF, enc_start, enc_end, &props);
      if (!s.ok() || props.num_entries == 0) {
        g_active_compaction_skipped_on_recheck << 1;
        continue;
      }
      const bool phase_b = FLAGS_gc_enable_mvcc_properties_collector && props.num_versions > 0;
      double recheck_score = 0.0;
      if (phase_b) {
        uint64_t discardable = 0;
        discardable += GetEstimatedDiscardableEntries(props.num_deletes, props.oldest_delete_ts,
                                                      props.newest_delete_ts, gc_safe_point);
        const uint64_t stale = props.num_versions > props.num_rows ? props.num_versions - props.num_rows : 0;
        discardable += GetEstimatedDiscardableEntries(stale, props.oldest_stale_version_ts,
                                                      props.newest_stale_version_ts, gc_safe_point);
        recheck_score = GetCompactScore(props.num_deletions, discardable, props.num_entries, true);
      } else {
        bool cold = false;
        if (FLAGS_gc_active_compaction_cold_sst_seconds > 0 && props.oldest_ancester_time > 0) {
          const int64_t age_s = Helper::Timestamp() - static_cast<int64_t>(props.oldest_ancester_time);
          cold = age_s >= FLAGS_gc_active_compaction_cold_sst_seconds;
        }
        uint64_t signal = props.num_deletions;
        if (cold) {
          signal = std::max(signal, props.num_entries);
        }
        recheck_score = GetCompactScore(signal, 0, props.num_entries, false);
      }
      if (recheck_score <= 0.0) {
        g_active_compaction_skipped_on_recheck << 1;
        DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_txn_gc_detail) << fmt::format(
            "[active_compaction][region({})] recheck score dropped to 0, skip", cand.region_id);
        continue;
      }
    }

    // Record the attempt BEFORE the blocking call so the interval is measured from the start.
    {
      std::lock_guard<std::mutex> lk(g_active_compaction_state_mutex);
      auto& state = g_active_compaction_state[cand.region_id];
      state.last_compaction_time_ms = now_ms;
      state.last_compaction_safe_point = gc_safe_point;
    }
    ++issued;
    g_active_compaction_issued << 1;
    const bool force_bottommost = FLAGS_gc_active_compaction_force_bottommost;
    if (force_bottommost) {
      g_active_compaction_force_bottommost_issued << 1;
    }

    const std::string enc_start = EncodeBoundOrEmpty(cand.start_key);
    const std::string enc_end = EncodeBoundOrEmpty(cand.end_key);
    const int64_t compact_begin_ms = Helper::TimestampMs();
    // [TODO CompactionGuard] CompactRange uses the raw region bounds; SST boundaries are not aligned to
    // region bounds yet, so a compaction may pull in neighbouring regions' files. Out of scope here.
    // [TODO default CF] Only the write CF is compacted; default/lock CF active compaction is out of scope.
    // [M3] This is a per-region sub-range CompactRange, so the CompactionFilter Context.is_full_compaction
    // is normally false (is_full_compaction = "includes ALL table files of the CF"; sub-range does not).
    // When gc_compaction_filter_tombstone_require_full_compaction=true, this reclaims old MVCC versions but
    // does NOT physically delete Delete tombstones -- those are reclaimed by periodic full compaction / raft
    // GC. force_bottommost forces the bottommost LEVEL only, not a full compaction. (Warned at handler start.)
    butil::Status cs =
        raw_engine->CompactRange(Constant::kTxnWriteCF, enc_start, enc_end, force_bottommost);
    g_active_compaction_compact_blocked_ms << (Helper::TimestampMs() - compact_begin_ms);
    if (!cs.ok()) {
      DINGO_LOG(WARNING) << fmt::format("[active_compaction][region({})] CompactRange failed: {}",
                                        cand.region_id, cs.error_cstr());
    }
  }
}

}  // namespace dingodb
