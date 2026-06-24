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

#ifndef DINGODB_ENGINE_MVCC_PROPERTIES_COLLECTOR_H_  // NOLINT
#define DINGODB_ENGINE_MVCC_PROPERTIES_COLLECTOR_H_

#include <cstdint>
#include <string>

#include "gflags/gflags.h"
#include "rocksdb/table_properties.h"

namespace dingodb {

// [GC-Tombstone baseline step-B4] Master switch for registering the write CF MVCC properties
// collector. Default OFF: when false the collector is NOT registered on the write CF, so SST
// flush/compaction output is byte-for-byte identical to before and there is no extra CPU cost.
// DEFINEd in mvcc_properties_collector.cc to avoid clashing with the scheduler's gflags.
DECLARE_bool(gc_enable_mvcc_properties_collector);

// User-collected property keys written by MvccPropertiesCollector::Finish into each write CF SST.
// RangeTableProperties parsing (rocks_raw_engine.cc::ParseMvccUserProperties) must use the exact same
// keys. Values are decimal ASCII of an unsigned/signed 64-bit integer (simple, debuggable, endian-safe).
inline constexpr char kMvccPropNumEntries[] = "dingo.mvcc.num_entries";
inline constexpr char kMvccPropNumVersions[] = "dingo.mvcc.num_versions";
inline constexpr char kMvccPropNumRows[] = "dingo.mvcc.num_rows";
inline constexpr char kMvccPropNumDeletes[] = "dingo.mvcc.num_deletes";
inline constexpr char kMvccPropOldestDeleteTs[] = "dingo.mvcc.oldest_delete_ts";
inline constexpr char kMvccPropNewestDeleteTs[] = "dingo.mvcc.newest_delete_ts";
inline constexpr char kMvccPropOldestStaleVersionTs[] = "dingo.mvcc.oldest_stale_version_ts";
inline constexpr char kMvccPropNewestStaleVersionTs[] = "dingo.mvcc.newest_stale_version_ts";

// Per-SST collector. RocksDB creates one instance per SST being written (flush or compaction output),
// so all members below are per-SST instance state and are NOT reused across SSTs. For every record it
// decodes the write CF key into (user_key, commit_ts) and parses the WriteInfo value to read the op,
// accumulating MVCC statistics that are serialized into the SST user-collected properties in Finish().
//
// Safety: this runs on RocksDB flush/compaction background threads. It NEVER FATALs. A record that
// fails to decode/parse is conservatively skipped (not counted) and never aborts the SST write.
class MvccPropertiesCollector : public rocksdb::TablePropertiesCollector {
 public:
  MvccPropertiesCollector() = default;
  ~MvccPropertiesCollector() override = default;

  rocksdb::Status AddUserKey(const rocksdb::Slice& key, const rocksdb::Slice& value, rocksdb::EntryType type,
                             rocksdb::SequenceNumber seq, uint64_t file_size) override;

  rocksdb::Status Finish(rocksdb::UserCollectedProperties* properties) override;

  rocksdb::UserCollectedProperties GetReadableProperties() const override;

  const char* Name() const override { return "DingoMvccPropertiesCollector"; }

 private:
  // Previous user_key seen (encoded write CF key decodes to it). Used to detect new rows and stale
  // (non-newest) versions. Records arrive in key order (ascending user_key; within a user_key the ts
  // segment is descending-encoded so the newest commit_ts comes first).
  std::string last_user_key_;
  bool has_last_user_key_ = false;

  uint64_t num_entries_ = 0;   // records seen (kEntryPut only)
  uint64_t num_versions_ = 0;  // one per write CF Put record (== num_entries_, kept explicit for clarity)
  uint64_t num_rows_ = 0;      // distinct user_keys
  uint64_t num_deletes_ = 0;   // op=Delete WriteInfo records

  int64_t oldest_delete_ts_ = 0;
  int64_t newest_delete_ts_ = 0;
  int64_t oldest_stale_version_ts_ = 0;
  int64_t newest_stale_version_ts_ = 0;
};

// Factory: RocksDB instantiates one MvccPropertiesCollector per SST output via this factory.
class MvccPropertiesCollectorFactory : public rocksdb::TablePropertiesCollectorFactory {
 public:
  MvccPropertiesCollectorFactory() = default;
  ~MvccPropertiesCollectorFactory() override = default;

  rocksdb::TablePropertiesCollector* CreateTablePropertiesCollector(
      rocksdb::TablePropertiesCollectorFactory::Context context) override;

  const char* Name() const override { return "DingoMvccPropertiesCollectorFactory"; }
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_MVCC_PROPERTIES_COLLECTOR_H_  // NOLINT
