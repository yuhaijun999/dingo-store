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
#include <utility>
#include <vector>

#include "fmt/core.h"
#include "gflags/gflags.h"
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

}  // namespace dingodb
