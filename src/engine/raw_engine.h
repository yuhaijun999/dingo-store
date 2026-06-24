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

#ifndef DINGODB_ENGINE_KV_ENGINE_H_  // NOLINT
#define DINGODB_ENGINE_KV_ENGINE_H_

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "config/config.h"
#include "engine/iterator.h"
#include "engine/snapshot.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/store_internal.pb.h"

namespace dingodb {

// [GC-Tombstone baseline step-B1] Aggregated SST table properties over a key range of one CF.
// Used by the active-compaction scheduler to estimate how much garbage a region holds before
// deciding whether to issue a manual CompactRange. Intended for the write CF (Constant::kTxnWriteCF)
// only. All fields are simple counters aggregated across the SSTs that overlap the requested range.
//
// Two groups of signals:
//   * Phase-A signals come straight from RocksDB's built-in TableProperties (always available on the
//     RocksRawEngine). NOTE (reviewer MF1): a dingo Delete "tombstone" is an op=Delete WriteInfo Put
//     record (a plain RocksDB kValue entry), NOT a RocksDB Delete/SingleDelete. It therefore counts
//     toward num_entries but NEVER toward num_deletions. So the phase-A primary scoring signal is
//     num_entries plus "SST not full-compacted for a long time" (oldest_ancester_time); num_deletions
//     / num_range_deletions are only a minor add-on (Rollback noise, residual raft-GC point deletes).
//   * Phase-B signals are filled only when the MvccPropertiesCollector (step-B4) is registered on the
//     write CF and its user-collected properties are present in the SST. Otherwise they stay 0 (old
//     SSTs written before the collector, or the collector gflag is off) and the scheduler falls back
//     to the phase-A signals.
struct RangeTableProperties {
  // ---- Phase-A: RocksDB built-in TableProperties (always populated by RocksRawEngine) ----
  uint64_t num_entries = 0;          // total entries across overlapping SSTs (includes dingo tombstone Puts)
  uint64_t num_deletions = 0;        // RocksDB Delete/SingleDelete tombstones (NOT dingo Delete markers)
  uint64_t num_range_deletions = 0;  // RocksDB range tombstones
  uint64_t oldest_ancester_time = 0;  // min non-zero oldest_ancester_time -> cold-SST fallback signal (unix seconds)
  uint64_t sst_file_count = 0;        // number of SSTs overlapping the range

  // ---- Phase-B: filled by MvccPropertiesCollector (default 0 when absent) ----
  uint64_t num_versions = 0;               // total MVCC versions (one per write CF Put record)
  uint64_t num_rows = 0;                    // distinct user_keys
  uint64_t num_deletes = 0;                 // op=Delete WriteInfo Put records (dingo Delete markers)
  uint64_t num_discardable = 0;             // estimated discardable records (reserved; computed later from above)
  int64_t oldest_delete_ts = 0;            // min commit_ts among op=Delete records (0 = none)
  int64_t newest_delete_ts = 0;            // max commit_ts among op=Delete records (0 = none)
  int64_t oldest_stale_version_ts = 0;     // min commit_ts among non-newest versions (0 = none)
  int64_t newest_stale_version_ts = 0;     // max commit_ts among non-newest versions (0 = none)
};

class RawEngine : public std::enable_shared_from_this<RawEngine> {
 public:
  virtual ~RawEngine() = default;

  class Reader {
   public:
    Reader() = default;
    virtual ~Reader() = default;

    virtual butil::Status KvGet(const std::string& cf_name, const std::string& key, std::string& value) = 0;
    virtual butil::Status KvGet(const std::string& cf_name, std::shared_ptr<dingodb::Snapshot> snapshot,
                                const std::string& key, std::string& value) = 0;

    virtual butil::Status KvScan(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                                 std::vector<pb::common::KeyValue>& kvs) = 0;
    virtual butil::Status KvScan(const std::string& cf_name, std::shared_ptr<dingodb::Snapshot> snapshot,
                                 const std::string& start_key, const std::string& end_key,
                                 std::vector<pb::common::KeyValue>& kvs) = 0;

    virtual butil::Status KvCount(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                                  int64_t& count) = 0;
    virtual butil::Status KvCount(const std::string& cf_name, std::shared_ptr<dingodb::Snapshot> snapshot,
                                  const std::string& start_key, const std::string& end_key, int64_t& count) = 0;

    virtual std::shared_ptr<dingodb::Iterator> NewIterator(const std::string& cf_name, IteratorOptions options) = 0;
    virtual std::shared_ptr<dingodb::Iterator> NewIterator(const std::string& cf_name,
                                                           std::shared_ptr<Snapshot> snapshot,
                                                           IteratorOptions options) = 0;
  };
  using ReaderPtr = std::shared_ptr<Reader>;

  class Writer {
   public:
    Writer() = default;
    virtual ~Writer() = default;

    virtual butil::Status KvPut(const std::string& cf_name, const pb::common::KeyValue& kv) = 0;
    virtual butil::Status KvBatchPut(const std::string& cf_name, const std::vector<pb::common::KeyValue>& kvs) = 0;

    virtual butil::Status KvDelete(const std::string& cf_name, const std::string& key) = 0;

    virtual butil::Status KvBatchPutAndDelete(const std::string& cf_name,
                                              const std::vector<pb::common::KeyValue>& kvs_to_put,
                                              const std::vector<std::string>& keys_to_deletes) = 0;
    virtual butil::Status KvBatchPutAndDelete(
        const std::map<std::string, std::vector<pb::common::KeyValue>>& kv_put_with_cfs,
        const std::map<std::string, std::vector<std::string>>& kv_delete_with_cfs) = 0;

    virtual butil::Status KvDeleteRange(const std::string& cf_name, const pb::common::Range& range) = 0;
    virtual butil::Status KvBatchDeleteRange(
        const std::map<std::string, std::vector<pb::common::Range>>& range_with_cfs) = 0;

    butil::Status KvDeleteRange(const std::vector<std::string>& cf_names, const pb::common::Range& range) {
      std::map<std::string, std::vector<pb::common::Range>> delete_range_map;
      for (const auto& cf_name : cf_names) {
        delete_range_map[cf_name].push_back(range);
      }
      return KvBatchDeleteRange(delete_range_map);
    }
  };

  using WriterPtr = std::shared_ptr<Writer>;

  class Checkpoint {
   public:
    Checkpoint() = default;
    virtual ~Checkpoint() = default;

    virtual butil::Status Create(const std::string& /*dirpath*/) {
      return butil::Status(pb::error::ENOT_SUPPORT, "Not support checkpoint.");
    }
    virtual butil::Status Create(const std::string& /*dirpath*/, const std::vector<std::string>& /*cf_names*/,
                                 std::vector<pb::store_internal::SstFileInfo>& /*sst_files*/) {
      return butil::Status(pb::error::ENOT_SUPPORT, "Not support checkpoint.");
    }
  };
  using CheckpointPtr = std::shared_ptr<Checkpoint>;

  virtual bool Init(std::shared_ptr<Config> config, const std::vector<std::string>& cf_names) = 0;
  virtual void Close() = 0;
  virtual void Destroy() = 0;
  virtual bool Recover() { return true; }

  virtual std::string GetName() = 0;
  virtual pb::common::RawEngine GetRawEngineType() = 0;

  virtual SnapshotPtr GetSnapshot() = 0;

  virtual ReaderPtr Reader() = 0;
  virtual WriterPtr Writer() = 0;

  virtual CheckpointPtr NewCheckpoint() = 0;

  // range is encode range
  virtual butil::Status MergeCheckpointFiles(const std::string& path, const pb::common::Range& range,
                                             const std::vector<std::string>& cf_names,
                                             std::vector<std::string>& merge_sst_paths) = 0;
  virtual butil::Status IngestExternalFile(const std::string& cf_name, const std::vector<std::string>& files) = 0;

  virtual std::vector<int64_t> GetApproximateSizes(const std::string& cf_name,
                                                   std::vector<pb::common::Range>& ranges) = 0;

  virtual void Flush(const std::string& cf_name) = 0;
  virtual butil::Status Compact(const std::string& cf_name) = 0;

  // [GC-Tombstone baseline step-4] Manual compaction over a single CF key range, used by the active
  // compaction driver (step 5) to feed cold SSTs of a region through the compaction filter.
  // start_key / end_key are already-encoded (EncodeBytes) physical keys; an empty string means an
  // open bound (nullptr) on that side. Intentionally NON-pure-virtual with a NotSupported default so
  // that only RocksRawEngine has to override it (xdprocks / bdb / mem fall back to the default and
  // still compile). The legacy Compact(cf_name) above is left untouched.
  virtual butil::Status CompactRange(const std::string& /*cf_name*/, const std::string& /*start_key*/,
                                     const std::string& /*end_key*/, bool /*force_bottommost*/) {
    return butil::Status(pb::error::ENOT_SUPPORT, "CompactRange is not supported by this engine");
  }

  // [GC-Tombstone baseline step-B1] Aggregate the table properties of all SSTs that overlap a key
  // range of one CF (intended for the write CF). Used by the active-compaction scheduler to estimate
  // a region's reclaimable garbage. start_key / end_key are already-encoded (EncodeBytes) physical
  // keys; an empty string means an open bound (nullptr) on that side (same convention as CompactRange).
  // Like CompactRange, this is intentionally NON-pure-virtual with a NotSupported default so only
  // RocksRawEngine has to override it (xdprocks / bdb / mem fall back to the default and still compile,
  // letting the scheduler gracefully skip them). On error returns a non-OK status; it never FATALs.
  virtual butil::Status GetRangeTableProperties(const std::string& /*cf_name*/, const std::string& /*start_key*/,
                                                const std::string& /*end_key*/, RangeTableProperties* /*out*/) {
    return butil::Status(pb::error::ENOT_SUPPORT, "GetRangeTableProperties is not supported by this engine");
  }

 protected:
  RawEngine() = default;
};
using RawEnginePtr = std::shared_ptr<RawEngine>;

}  // namespace dingodb

#endif  // DINGODB_ENGINE_KV_ENGINE_H_  // NOLINT
