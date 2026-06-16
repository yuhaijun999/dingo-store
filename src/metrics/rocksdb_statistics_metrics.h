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

#ifndef DINGODB_METRICS_ROCKSDB_STATISTICS_METRICS_H_
#define DINGODB_METRICS_ROCKSDB_STATISTICS_METRICS_H_

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "bvar/status.h"
#include "rocksdb/statistics.h"

namespace dingodb {

// Exposes a curated subset of RocksDB Statistics (tickers + histograms) as bvar variables, which the
// existing PrometheusMetricsDumper auto-exports to /metrics. Ticker counts are cumulative-since-open
// gauges (let Prometheus rate() derive intervals); histograms expose p50/p99/avg/max.
//
// One instance serves multiple DBs (e.g. the main store RocksDB and the raft-log RocksDB), each
// distinguished by a db_label that is woven into the exposed metric name:
//   dingo_metrics_rocksdb_<db_label>_<metric>
//
// Collection is gated by FLAGS_enable_rocksdb_statistics_metric; when off, Collect() is a no-op.
class RocksdbStatisticsMetrics {
 public:
  RocksdbStatisticsMetrics() = default;
  ~RocksdbStatisticsMetrics() = default;

  RocksdbStatisticsMetrics(const RocksdbStatisticsMetrics&) = delete;
  void operator=(const RocksdbStatisticsMetrics&) = delete;

  static RocksdbStatisticsMetrics& GetInstance();

  // Read the curated tickers/histograms from `statistics` and push them into the bvar variables
  // tagged with `db_label`. Safe to call with a null `statistics` (skipped). Thread-safe.
  void Collect(const std::string& db_label, const std::shared_ptr<rocksdb::Statistics>& statistics);

 private:
  // Lazily create-and-expose (and cache) a bvar::Status for `name` on first sight.
  bvar::Status<int64_t>* GetOrCreate(const std::string& name);

  std::mutex mutex_;
  std::map<std::string, std::unique_ptr<bvar::Status<int64_t>>> metrics_;
};

}  // namespace dingodb

#endif  // DINGODB_METRICS_ROCKSDB_STATISTICS_METRICS_H_
