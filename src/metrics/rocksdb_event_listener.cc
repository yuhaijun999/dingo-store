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

#include "metrics/rocksdb_event_listener.h"

#include <cstdint>
#include <string>

#include "common/logging.h"
#include "fmt/core.h"
#include "gflags/gflags.h"

namespace dingodb {

DEFINE_bool(enable_rocksdb_event_listener, false,
            "register the observability rocksdb::EventListener (flush/compaction/stall/error metrics + "
            "logs) on the store and raft-log DBs. checked at DB open. default false");

namespace {
std::string MetricName(const std::string& db_label, const std::string& suffix) {
  return fmt::format("dingo_metrics_rocksdb_event_{}_{}", db_label, suffix);
}
}  // namespace

RocksdbEventListener::RocksdbEventListener(const std::string& db_label)
    : db_label_(db_label),
      flush_completed_count_(MetricName(db_label, "flush_completed_count")),
      flush_data_bytes_(MetricName(db_label, "flush_data_bytes")),
      compaction_completed_count_(MetricName(db_label, "compaction_completed_count")),
      compaction_input_bytes_(MetricName(db_label, "compaction_input_bytes")),
      compaction_output_bytes_(MetricName(db_label, "compaction_output_bytes")),
      compaction_input_records_(MetricName(db_label, "compaction_input_records")),
      compaction_output_records_(MetricName(db_label, "compaction_output_records")),
      compaction_input_deletion_records_(MetricName(db_label, "compaction_input_deletion_records")),
      compaction_expired_deletion_records_(MetricName(db_label, "compaction_expired_deletion_records")),
      compaction_records_replaced_(MetricName(db_label, "compaction_records_replaced")),
      stall_condition_changed_count_(MetricName(db_label, "stall_condition_changed_count")),
      stall_condition_(MetricName(db_label, "stall_condition"), 0),
      background_error_count_(MetricName(db_label, "background_error_count")),
      external_file_ingested_count_(MetricName(db_label, "external_file_ingested_count")),
      external_file_ingested_bytes_(MetricName(db_label, "external_file_ingested_bytes")) {}

void RocksdbEventListener::OnFlushCompleted(rocksdb::DB* /*db*/, const rocksdb::FlushJobInfo& info) {
  flush_completed_count_ << 1;
  flush_data_bytes_ << static_cast<int64_t>(info.table_properties.data_size);
}

void RocksdbEventListener::OnCompactionCompleted(rocksdb::DB* /*db*/, const rocksdb::CompactionJobInfo& info) {
  const auto& stats = info.stats;
  compaction_completed_count_ << 1;
  compaction_input_bytes_ << static_cast<int64_t>(stats.total_input_bytes);
  compaction_output_bytes_ << static_cast<int64_t>(stats.total_output_bytes);
  compaction_input_records_ << static_cast<int64_t>(stats.num_input_records);
  compaction_output_records_ << static_cast<int64_t>(stats.num_output_records);
  compaction_input_deletion_records_ << static_cast<int64_t>(stats.num_input_deletion_records);
  compaction_expired_deletion_records_ << static_cast<int64_t>(stats.num_expired_deletion_records);
  compaction_records_replaced_ << static_cast<int64_t>(stats.num_records_replaced);
}

void RocksdbEventListener::OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) {
  stall_condition_changed_count_ << 1;
  stall_condition_.set_value(static_cast<int64_t>(info.condition.cur));
  // WriteStallCondition: 0=kNormal, 1=kDelayed, 2=kStopped.
  DINGO_LOG(WARNING) << fmt::format("[rocksdb.event][{}] write stall condition changed, cf={} prev={} cur={}",
                                    db_label_, info.cf_name, static_cast<int>(info.condition.prev),
                                    static_cast<int>(info.condition.cur));
}

void RocksdbEventListener::OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status* bg_error) {
  background_error_count_ << 1;
  DINGO_LOG(ERROR) << fmt::format("[rocksdb.event][{}] background error, reason={} status={}", db_label_,
                                  static_cast<int>(reason), bg_error != nullptr ? bg_error->ToString() : "nullptr");
}

void RocksdbEventListener::OnExternalFileIngested(rocksdb::DB* /*db*/,
                                                  const rocksdb::ExternalFileIngestionInfo& info) {
  external_file_ingested_count_ << 1;
  external_file_ingested_bytes_ << static_cast<int64_t>(info.table_properties.data_size);
}

}  // namespace dingodb
