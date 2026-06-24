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

#include "engine/mvcc_properties_collector.h"

#include <cstdint>
#include <string>
#include <string_view>

#include "gflags/gflags.h"
#include "mvcc/codec.h"
#include "proto/store.pb.h"

namespace dingodb {

// See header for the rationale. Default false -> not registered on the write CF -> SST output
// byte-for-byte identical to before. Toggled by rocks_raw_engine.cc at CF options init time.
DEFINE_bool(gc_enable_mvcc_properties_collector, false,
            "register the write CF MVCC properties collector to persist MVCC stats into SST user "
            "properties for phase-B active-compaction scoring (default false; off = no extra cost)");

namespace {

// Serialize an integer to decimal ASCII for an SST user property value.
std::string ToProp(uint64_t v) { return std::to_string(v); }
std::string ToProp(int64_t v) { return std::to_string(v); }

}  // namespace

rocksdb::Status MvccPropertiesCollector::AddUserKey(const rocksdb::Slice& key, const rocksdb::Slice& value,
                                                    rocksdb::EntryType type, rocksdb::SequenceNumber /*seq*/,
                                                    uint64_t /*file_size*/) {
  // Only plain Put entries participate. Write CF records are all RocksDB Puts (a dingo Delete marker is
  // an op=Delete WriteInfo Put, not a RocksDB Delete). Anything else is skipped without counting.
  if (type != rocksdb::kEntryPut) {
    return rocksdb::Status::OK();
  }

  // Decode the write CF key into (user_key, commit_ts). NEVER FATAL on a background thread: on failure
  // conservatively skip this record (do not count it) so the SST write is unaffected.
  std::string user_key;
  int64_t commit_ts = 0;
  if (!mvcc::Codec::DecodeKey(std::string_view(key.data(), key.size()), user_key, commit_ts)) {
    return rocksdb::Status::OK();
  }

  // Parse the WriteInfo value to read the op. Conservatively skip on parse failure.
  pb::store::WriteInfo write_info;
  if (!write_info.ParseFromArray(value.data(), value.size())) {
    return rocksdb::Status::OK();
  }
  const pb::store::Op op = write_info.op();

  ++num_entries_;
  ++num_versions_;  // every write CF Put record is one MVCC version

  // New user_key -> a new logical row. Otherwise this is an older (non-newest) version of the current
  // row, i.e. a stale version (records arrive newest-first within a user_key due to descending ts).
  const bool is_new_row = !has_last_user_key_ || user_key != last_user_key_;
  if (is_new_row) {
    ++num_rows_;
    last_user_key_ = user_key;
    has_last_user_key_ = true;
  } else {
    // Non-first (older) version of the same user_key -> stale version. Track ts span.
    if (oldest_stale_version_ts_ == 0 || commit_ts < oldest_stale_version_ts_) {
      oldest_stale_version_ts_ = commit_ts;
    }
    if (commit_ts > newest_stale_version_ts_) {
      newest_stale_version_ts_ = commit_ts;
    }
  }

  if (op == pb::store::Op::Delete) {
    ++num_deletes_;
    if (oldest_delete_ts_ == 0 || commit_ts < oldest_delete_ts_) {
      oldest_delete_ts_ = commit_ts;
    }
    if (commit_ts > newest_delete_ts_) {
      newest_delete_ts_ = commit_ts;
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status MvccPropertiesCollector::Finish(rocksdb::UserCollectedProperties* properties) {
  if (properties == nullptr) {
    return rocksdb::Status::OK();
  }

  properties->insert({kMvccPropNumEntries, ToProp(num_entries_)});
  properties->insert({kMvccPropNumVersions, ToProp(num_versions_)});
  properties->insert({kMvccPropNumRows, ToProp(num_rows_)});
  properties->insert({kMvccPropNumDeletes, ToProp(num_deletes_)});
  properties->insert({kMvccPropOldestDeleteTs, ToProp(oldest_delete_ts_)});
  properties->insert({kMvccPropNewestDeleteTs, ToProp(newest_delete_ts_)});
  properties->insert({kMvccPropOldestStaleVersionTs, ToProp(oldest_stale_version_ts_)});
  properties->insert({kMvccPropNewestStaleVersionTs, ToProp(newest_stale_version_ts_)});

  return rocksdb::Status::OK();
}

rocksdb::UserCollectedProperties MvccPropertiesCollector::GetReadableProperties() const {
  rocksdb::UserCollectedProperties props;
  props.insert({kMvccPropNumEntries, ToProp(num_entries_)});
  props.insert({kMvccPropNumVersions, ToProp(num_versions_)});
  props.insert({kMvccPropNumRows, ToProp(num_rows_)});
  props.insert({kMvccPropNumDeletes, ToProp(num_deletes_)});
  props.insert({kMvccPropOldestDeleteTs, ToProp(oldest_delete_ts_)});
  props.insert({kMvccPropNewestDeleteTs, ToProp(newest_delete_ts_)});
  props.insert({kMvccPropOldestStaleVersionTs, ToProp(oldest_stale_version_ts_)});
  props.insert({kMvccPropNewestStaleVersionTs, ToProp(newest_stale_version_ts_)});
  return props;
}

rocksdb::TablePropertiesCollector* MvccPropertiesCollectorFactory::CreateTablePropertiesCollector(
    rocksdb::TablePropertiesCollectorFactory::Context /*context*/) {
  return new MvccPropertiesCollector();
}

}  // namespace dingodb
