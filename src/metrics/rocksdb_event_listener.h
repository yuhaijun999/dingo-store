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

#ifndef DINGODB_METRICS_ROCKSDB_EVENT_LISTENER_H_
#define DINGODB_METRICS_ROCKSDB_EVENT_LISTENER_H_

#include <cstdint>
#include <string>

#include "bvar/reducer.h"
#include "bvar/status.h"
#include "gflags/gflags_declare.h"
#include "rocksdb/listener.h"

namespace dingodb {

// Master switch (default false): when true, RocksdbEventListener is registered on the store and
// raft-log DBs at open time. Defined in rocksdb_event_listener.cc.
DECLARE_bool(enable_rocksdb_event_listener);

// Observability-only rocksdb::EventListener. Turns RocksDB flush/compaction/stall/background-error/
// ingest events into bvar metrics (and WARN/ERROR logs for write stalls and background errors),
// complementing the periodic GetIntProperty/Statistics polling with real-time, event-driven data.
//
// One instance is registered per DB (db_label distinguishes "store" vs "raft_log"); the metric names
// are dingo_metrics_rocksdb_event_<db_label>_*. Every callback is cheap (atomic increment / gauge set)
// and thread-safe, per the EventListener contract that callbacks must be thread-safe and must not run
// long / block (no DB::Put or DB::CompactFiles here).
class RocksdbEventListener : public rocksdb::EventListener {
 public:
  explicit RocksdbEventListener(const std::string& db_label);
  ~RocksdbEventListener() override = default;

  RocksdbEventListener(const RocksdbEventListener&) = delete;
  RocksdbEventListener& operator=(const RocksdbEventListener&) = delete;

  void OnFlushCompleted(rocksdb::DB* db, const rocksdb::FlushJobInfo& info) override;
  void OnCompactionCompleted(rocksdb::DB* db, const rocksdb::CompactionJobInfo& info) override;
  void OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) override;
  void OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status* bg_error) override;
  void OnExternalFileIngested(rocksdb::DB* db, const rocksdb::ExternalFileIngestionInfo& info) override;

 private:
  std::string db_label_;

  // flush
  bvar::Adder<int64_t> flush_completed_count_;
  bvar::Adder<int64_t> flush_data_bytes_;

  // compaction (num_*_deletion_records / records_replaced directly reflect GC/Tombstone cleanup)
  bvar::Adder<int64_t> compaction_completed_count_;
  bvar::Adder<int64_t> compaction_input_bytes_;
  bvar::Adder<int64_t> compaction_output_bytes_;
  bvar::Adder<int64_t> compaction_input_records_;
  bvar::Adder<int64_t> compaction_output_records_;
  bvar::Adder<int64_t> compaction_input_deletion_records_;    // tombstones fed into compaction
  bvar::Adder<int64_t> compaction_expired_deletion_records_;  // tombstones physically dropped
  bvar::Adder<int64_t> compaction_records_replaced_;          // older versions overwritten

  // write stall (cur condition: 0=normal, 1=delayed, 2=stopped)
  bvar::Adder<int64_t> stall_condition_changed_count_;
  bvar::Status<int64_t> stall_condition_;

  // background error / external ingest
  bvar::Adder<int64_t> background_error_count_;
  bvar::Adder<int64_t> external_file_ingested_count_;
  bvar::Adder<int64_t> external_file_ingested_bytes_;
};

}  // namespace dingodb

#endif  // DINGODB_METRICS_ROCKSDB_EVENT_LISTENER_H_
