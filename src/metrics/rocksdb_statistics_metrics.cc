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

#include "metrics/rocksdb_statistics_metrics.h"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "fmt/core.h"
#include "gflags/gflags.h"
#include "rocksdb/db.h"
#include "rocksdb/statistics.h"

namespace dingodb {

DEFINE_bool(enable_rocksdb_statistics_metric, false,
            "expose RocksDB Statistics (tickers/histograms) as bvar/Prometheus metrics. default false");

namespace {

// Curated cumulative tickers. Names are the metric suffix used in the exposed bvar name; they are
// intentionally short and stable rather than RocksDB's dotted internal names.
const std::vector<std::pair<rocksdb::Tickers, const char*>>& Tickers() {
  static const std::vector<std::pair<rocksdb::Tickers, const char*>> kTickers = {
      // block cache (derive hit-rate from hit/miss in Prometheus)
      {rocksdb::BLOCK_CACHE_HIT, "block_cache_hit"},
      {rocksdb::BLOCK_CACHE_MISS, "block_cache_miss"},
      {rocksdb::BLOCK_CACHE_DATA_HIT, "block_cache_data_hit"},
      {rocksdb::BLOCK_CACHE_DATA_MISS, "block_cache_data_miss"},
      // io volume / write amplification
      {rocksdb::BYTES_READ, "bytes_read"},
      {rocksdb::BYTES_WRITTEN, "bytes_written"},
      {rocksdb::COMPACT_READ_BYTES, "compact_read_bytes"},
      {rocksdb::COMPACT_WRITE_BYTES, "compact_write_bytes"},
      {rocksdb::FLUSH_WRITE_BYTES, "flush_write_bytes"},
      // write stall
      {rocksdb::STALL_MICROS, "stall_micros"},
      // tombstone / scan cost (directly reflects GC/Tombstone compaction-filter effect)
      {rocksdb::COMPACTION_KEY_DROP_NEWER_ENTRY, "compaction_key_drop_newer_entry"},
      {rocksdb::COMPACTION_KEY_DROP_OBSOLETE, "compaction_key_drop_obsolete"},
      {rocksdb::NUMBER_OF_RESEEKS_IN_ITERATION, "number_reseeks_in_iteration"},
      {rocksdb::NUMBER_ITER_SKIP, "number_iter_skip"},
      // op counters
      {rocksdb::NUMBER_KEYS_READ, "number_keys_read"},
      {rocksdb::NUMBER_KEYS_WRITTEN, "number_keys_written"},
      {rocksdb::NUMBER_DB_SEEK, "number_db_seek"},
      {rocksdb::NUMBER_DB_NEXT, "number_db_next"},
  };
  return kTickers;
}

// Curated latency histograms; each exposes p50/p99/avg/max.
const std::vector<std::pair<rocksdb::Histograms, const char*>>& Histograms() {
  static const std::vector<std::pair<rocksdb::Histograms, const char*>> kHistograms = {
      {rocksdb::DB_GET, "get_micros"},
      {rocksdb::DB_WRITE, "write_micros"},
      {rocksdb::SST_READ_MICROS, "sst_read_micros"},
      {rocksdb::COMPACTION_TIME, "compaction_micros"},
  };
  return kHistograms;
}

// Curated current-state DB properties read via GetIntProperty (per column family). `suffix` is the
// metric-name suffix; `prop` is the RocksDB property key.
const std::vector<std::pair<const char*, const char*>>& IntProperties() {
  static const std::vector<std::pair<const char*, const char*>> kProperties = {
      // compaction backlog / progress (directly reflects whether GC/Tombstone compaction keeps up)
      {"pending_compaction_bytes", "rocksdb.estimate-pending-compaction-bytes"},
      {"compaction_pending", "rocksdb.compaction-pending"},
      {"num_running_compactions", "rocksdb.num-running-compactions"},
      {"num_running_flushes", "rocksdb.num-running-flushes"},
      {"mem_table_flush_pending", "rocksdb.mem-table-flush-pending"},
      // memtable state / tombstones in memtable
      {"num_immutable_mem_table", "rocksdb.num-immutable-mem-table"},
      {"cur_size_all_mem_tables", "rocksdb.cur-size-all-mem-tables"},
      {"size_all_mem_tables", "rocksdb.size-all-mem-tables"},
      {"num_entries_active_mem_table", "rocksdb.num-entries-active-mem-table"},
      {"num_deletes_active_mem_table", "rocksdb.num-deletes-active-mem-table"},
      {"num_deletes_imm_mem_tables", "rocksdb.num-deletes-imm-mem-tables"},
      // size / key estimates
      {"estimate_num_keys", "rocksdb.estimate-num-keys"},
      {"estimate_live_data_size", "rocksdb.estimate-live-data-size"},
      {"live_sst_files_size", "rocksdb.live-sst-files-size"},
      {"total_sst_files_size", "rocksdb.total-sst-files-size"},
      // write stall / health
      {"is_write_stopped", "rocksdb.is-write-stopped"},
      {"actual_delayed_write_rate", "rocksdb.actual-delayed-write-rate"},
      {"background_errors", "rocksdb.background-errors"},
  };
  return kProperties;
}

// Number of LSM levels to report num-files-at-levelN for (RocksDB default is 7: L0..L6).
constexpr int kNumLevelsToReport = 7;

}  // namespace

RocksdbStatisticsMetrics& RocksdbStatisticsMetrics::GetInstance() {
  static RocksdbStatisticsMetrics instance;
  return instance;
}

bvar::Status<int64_t>* RocksdbStatisticsMetrics::GetOrCreate(const std::string& name) {
  // caller holds mutex_
  auto it = metrics_.find(name);
  if (it != metrics_.end()) {
    return it->second.get();
  }

  auto metric = std::make_unique<bvar::Status<int64_t>>();
  metric->expose(name);
  auto* raw = metric.get();
  metrics_.emplace(name, std::move(metric));
  return raw;
}

void RocksdbStatisticsMetrics::Collect(const std::string& db_label,
                                       const std::shared_ptr<rocksdb::Statistics>& statistics) {
  if (!FLAGS_enable_rocksdb_statistics_metric) {
    return;
  }
  if (statistics == nullptr) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  for (const auto& [ticker, suffix] : Tickers()) {
    std::string name = fmt::format("dingo_metrics_rocksdb_{}_{}", db_label, suffix);
    GetOrCreate(name)->set_value(static_cast<int64_t>(statistics->getTickerCount(ticker)));
  }

  for (const auto& [histogram, suffix] : Histograms()) {
    rocksdb::HistogramData data;
    statistics->histogramData(histogram, &data);
    GetOrCreate(fmt::format("dingo_metrics_rocksdb_{}_{}_p50", db_label, suffix))
        ->set_value(static_cast<int64_t>(data.median));
    GetOrCreate(fmt::format("dingo_metrics_rocksdb_{}_{}_p99", db_label, suffix))
        ->set_value(static_cast<int64_t>(data.percentile99));
    GetOrCreate(fmt::format("dingo_metrics_rocksdb_{}_{}_avg", db_label, suffix))
        ->set_value(static_cast<int64_t>(data.average));
    GetOrCreate(fmt::format("dingo_metrics_rocksdb_{}_{}_max", db_label, suffix))
        ->set_value(static_cast<int64_t>(data.max));
  }
}

void RocksdbStatisticsMetrics::CollectProperties(
    const std::string& db_label, rocksdb::DB* db,
    const std::vector<std::pair<std::string, rocksdb::ColumnFamilyHandle*>>& column_families) {
  if (!FLAGS_enable_rocksdb_statistics_metric) {
    return;
  }
  if (db == nullptr || column_families.empty()) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);

  for (const auto& [cf_name, cf_handle] : column_families) {
    if (cf_handle == nullptr) {
      continue;
    }

    // Integer properties: dingo_metrics_rocksdb_<db>_<cf>_<suffix>
    for (const auto& [suffix, prop] : IntProperties()) {
      uint64_t value = 0;
      if (db->GetIntProperty(cf_handle, prop, &value)) {
        GetOrCreate(fmt::format("dingo_metrics_rocksdb_{}_{}_{}", db_label, cf_name, suffix))
            ->set_value(static_cast<int64_t>(value));
      }
    }

    // Per-level file count (string property): dingo_metrics_rocksdb_<db>_<cf>_num_files_at_level<N>
    for (int level = 0; level < kNumLevelsToReport; ++level) {
      std::string value;
      if (db->GetProperty(cf_handle, fmt::format("rocksdb.num-files-at-level{}", level), &value)) {
        int64_t num_files = 0;
        try {
          num_files = std::stoll(value);
        } catch (const std::exception&) {
          continue;
        }
        GetOrCreate(fmt::format("dingo_metrics_rocksdb_{}_{}_num_files_at_level{}", db_label, cf_name, level))
            ->set_value(num_files);
      }
    }
  }
}

}  // namespace dingodb
