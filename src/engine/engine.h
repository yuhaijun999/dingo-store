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

#ifndef DINGODB_ENGINE_ENGINE_H_  // NOLINT
#define DINGODB_ENGINE_ENGINE_H_

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/context.h"
#include "common/stream.h"
#include "config/config.h"
#include "document/document_index.h"
#include "engine/raw_engine.h"
#include "engine/snapshot.h"
#include "engine/write_data.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/document.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/store.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

namespace mvcc {
class Reader;
using ReaderPtr = std::shared_ptr<Reader>;
}  // namespace mvcc

namespace store {
class Region;
using RegionPtr = std::shared_ptr<Region>;
}  // namespace store

class VectorIndexWrapper;
using VectorIndexWrapperPtr = std::shared_ptr<VectorIndexWrapper>;

class Engine : public std::enable_shared_from_this<Engine> {
  using Errno = pb::error::Errno;

 public:
  virtual ~Engine() = default;

  virtual bool Init(std::shared_ptr<Config> config) = 0;
  virtual bool Recover() { return true; }

  virtual std::string GetName() = 0;
  virtual pb::common::StorageEngine GetID() = 0;

  virtual std::shared_ptr<RawEngine> GetRawEngine(pb::common::RawEngine type) = 0;

  virtual std::shared_ptr<Snapshot> GetSnapshot() = 0;
  virtual butil::Status SaveSnapshot(std::shared_ptr<Context> ctx, int64_t region_id, bool force) = 0;
  virtual butil::Status AyncSaveSnapshot(std::shared_ptr<Context> ctx, int64_t region_id, bool force) = 0;

  virtual butil::Status Write(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) = 0;
  virtual butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) = 0;
  virtual butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data,
                                   WriteCbFunc cb) = 0;

  // raw kv reader
  class Reader {
   public:
    Reader() = default;
    virtual ~Reader() = default;

    virtual butil::Status KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) = 0;

    virtual butil::Status KvScan(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                                 std::vector<pb::common::KeyValue>& kvs) = 0;

    virtual butil::Status KvCount(std::shared_ptr<Context> ctx, const std::string& start_key,
                                  const std::string& end_key, int64_t& count) = 0;

    virtual dingodb::IteratorPtr NewIterator(const std::string& cf_name, int64_t ts,  // NOLINT
                                             IteratorOptions options) {               // NOLINT
      throw std::runtime_error("No support new iterator.");
      return nullptr;
    }
  };
  using ReaderPtr = std::shared_ptr<Reader>;

  // raw kv writer
  class Writer {
   public:
    Writer() = default;
    virtual ~Writer() = default;

    virtual butil::Status KvPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs) = 0;
    virtual butil::Status KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                                   std::vector<bool>& key_states) = 0;
    virtual butil::Status KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) = 0;
    virtual butil::Status KvPutIfAbsent(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                        bool is_atomic, std::vector<bool>& key_states) = 0;
    virtual butil::Status KvCompareAndSet(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                          const std::vector<std::string>& expect_values, bool is_atomic,
                                          std::vector<bool>& key_states) = 0;
  };
  using WriterPtr = std::shared_ptr<Writer>;

  // Vector reader
  class VectorReader {
   public:
    VectorReader() = default;
    virtual ~VectorReader() = default;

    struct Context {
      int64_t partition_id{};
      int64_t region_id{};

      pb::common::RawEngine raw_engine_type{pb::common::RAW_ENG_ROCKSDB};
      pb::common::StorageEngine store_engine_type{pb::common::STORE_ENG_RAFT_STORE};

      pb::common::Range region_range;

      int64_t ts{0};

      std::vector<pb::common::VectorWithId> vector_with_ids;
      std::vector<int64_t> vector_ids;
      pb::common::VectorSearchParameter parameter;
      std::vector<std::string> selected_scalar_keys;
      pb::common::VectorScalardata scalar_data_for_filter;

      int64_t start_id{};
      int64_t end_id{};
      int64_t limit{};

      bool with_vector_data{};
      bool with_scalar_data{};
      bool with_table_data{};
      bool is_reverse{};
      bool use_scalar_filter{};

      VectorIndexWrapperPtr vector_index;
      pb::common::ScalarSchema scalar_schema;
    };

    virtual butil::Status VectorBatchSearch(std::shared_ptr<VectorReader::Context> ctx,
                                            std::vector<pb::index::VectorWithDistanceResult>& results) = 0;

    virtual butil::Status VectorBatchQuery(std::shared_ptr<VectorReader::Context> ctx,
                                           std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;

    virtual butil::Status VectorGetBorderId(int64_t ts, const pb::common::Range& region_range, bool get_min,
                                            int64_t& vector_id) = 0;
    virtual butil::Status VectorScanQuery(std::shared_ptr<VectorReader::Context> ctx,
                                          std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;
    virtual butil::Status VectorGetRegionMetrics(int64_t region_id, const pb::common::Range& region_range,
                                                 VectorIndexWrapperPtr vector_index,
                                                 pb::common::VectorIndexMetrics& region_metrics) = 0;

    virtual butil::Status VectorCount(int64_t ts, const pb::common::Range& range, int64_t& count) = 0;
    virtual butil::Status VectorCountMemory(std::shared_ptr<VectorReader::Context> ctx, int64_t& count) = 0;
    virtual butil::Status VectorBuild(std::shared_ptr<VectorReader::Context> ctx,
                                      const pb::common::VectorBuildParameter& parameter, int64_t ts,
                                      pb::common::VectorStateParameter& vector_state_parameter) = 0;

    virtual butil::Status VectorLoad(std::shared_ptr<VectorReader::Context> ctx,
                                     const pb::common::VectorLoadParameter& parameter,
                                     pb::common::VectorStateParameter& vector_state_parameter) = 0;

    virtual butil::Status VectorStatus(std::shared_ptr<VectorReader::Context> ctx,
                                       pb::common::VectorStateParameter& vector_state_parameter,
                                       pb::error::Error& internal_error) = 0;

    virtual butil::Status VectorReset(std::shared_ptr<VectorReader::Context> ctx, bool delete_data_file,
                                      pb::common::VectorStateParameter& vector_state_parameter) = 0;

    virtual butil::Status VectorDump(std::shared_ptr<VectorReader::Context> ctx, bool dump_all,
                                     std::vector<std::string>& dump_datas) = 0;

    // This function is for testing only
    virtual butil::Status VectorBatchSearchDebug(std::shared_ptr<VectorReader::Context> ctx,
                                                 std::vector<pb::index::VectorWithDistanceResult>& results,
                                                 int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                                 int64_t& search_time_us) = 0;
  };
  using VectorReaderPtr = std::shared_ptr<VectorReader>;

  // Document reader
  class DocumentReader {
   public:
    DocumentReader() = default;
    virtual ~DocumentReader() = default;

    struct Context {
      int64_t partition_id{};
      int64_t region_id{};

      pb::common::RawEngine raw_engine_type{pb::common::RAW_ENG_ROCKSDB};
      pb::common::StorageEngine store_engine_type{pb::common::STORE_ENG_RAFT_STORE};

      pb::common::Range region_range;

      int64_t ts{0};

      std::vector<int64_t> document_ids;
      pb::common::DocumentSearchParameter parameter;
      std::vector<std::string> selected_scalar_keys;

      int64_t start_id{};
      int64_t end_id{};
      int64_t limit{};

      bool with_scalar_data{};
      bool with_table_data{};
      bool is_reverse{};
      bool use_scalar_filter{};

      DocumentIndexWrapperPtr document_index;
      pb::common::ScalarSchema scalar_schema;

      StreamPtr Stream() { return stream; }
      void SetStream(StreamPtr streamptr) { stream = streamptr; }
      StreamPtr stream;
    };

    virtual butil::Status DocumentSearch(std::shared_ptr<DocumentReader::Context> ctx,
                                         std::vector<pb::common::DocumentWithScore>& results) = 0;
    virtual butil::Status DocumentSearchAll(std::shared_ptr<DocumentReader::Context> ctx, bool& has_more,
                                            std::vector<pb::common::DocumentWithScore>& results) = 0;

    virtual butil::Status DocumentBatchQuery(std::shared_ptr<DocumentReader::Context> ctx,
                                             std::vector<pb::common::DocumentWithId>& doc_with_ids) = 0;

    virtual butil::Status DocumentGetBorderId(int64_t ts, const pb::common::Range& region_range, bool get_min,
                                              int64_t& document_id) = 0;
    virtual butil::Status DocumentScanQuery(std::shared_ptr<DocumentReader::Context> ctx,
                                            std::vector<pb::common::DocumentWithId>& doc_with_ids) = 0;
    virtual butil::Status DocumentGetRegionMetrics(int64_t region_id, const pb::common::Range& region_range,
                                                   DocumentIndexWrapperPtr document_index,
                                                   pb::common::DocumentIndexMetrics& region_metrics) = 0;

    virtual butil::Status DocumentCount(int64_t ts, const pb::common::Range& range, int64_t& count) = 0;
  };
  using DocumentReaderPtr = std::shared_ptr<DocumentReader>;

  class TxnReader {
   public:
    TxnReader() = default;
    virtual ~TxnReader() = default;

    virtual butil::Status TxnBatchGet(std::shared_ptr<Context> ctx, int64_t start_ts,
                                      const std::vector<std::string>& keys, std::vector<pb::common::KeyValue>& kvs,
                                      const std::set<int64_t>& resolved_locks,
                                      pb::store::TxnResultInfo& txn_result_info) = 0;
    virtual butil::Status TxnScan(std::shared_ptr<Context> ctx, int64_t start_ts, const pb::common::Range& range,
                                  int64_t limit, bool key_only, bool is_reverse,
                                  const std::set<int64_t>& resolved_locks, bool disable_coprocessor,
                                  const pb::common::CoprocessorV2& coprocessor,
                                  pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs,
                                  bool& has_more, std::string& end_scan_key) = 0;
    virtual butil::Status TxnScanLock(std::shared_ptr<Context> ctx, int64_t min_lock_ts, int64_t max_lock_ts,
                                      const pb::common::Range& range, int64_t limit,
                                      std::vector<pb::store::LockInfo>& lock_infos, bool& has_more,
                                      std::string& end_scan_key) = 0;
  };
  using TxnReaderPtr = std::shared_ptr<TxnReader>;

  class TxnWriter {
   public:
    TxnWriter() = default;
    virtual ~TxnWriter() = default;

    virtual butil::Status TxnPessimisticLock(std::shared_ptr<Context> ctx,
                                             const std::vector<pb::store::Mutation>& mutations,
                                             const std::string& primary_lock, int64_t start_ts, int64_t lock_ttl,
                                             int64_t for_update_ts, bool return_values,
                                             std::vector<pb::common::KeyValue>& kvs) = 0;
    virtual butil::Status TxnPessimisticRollback(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                                 int64_t start_ts, int64_t for_update_ts,
                                                 const std::vector<std::string>& keys) = 0;
    virtual butil::Status TxnPrewrite(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                      const std::vector<pb::store::Mutation>& mutations,
                                      const std::string& primary_lock, int64_t start_ts, int64_t lock_ttl,
                                      int64_t txn_size, bool try_one_pc, int64_t min_commit_ts, int64_t max_commit_ts,
                                      const std::vector<int64_t>& pessimistic_checks,
                                      const std::map<int64_t, int64_t>& for_update_ts_checks,
                                      const std::map<int64_t, std::string>& lock_extra_datas,
                                      const std::vector<std::string>& secondaries) = 0;
    virtual butil::Status TxnCommit(std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts,
                                    int64_t commit_ts, const std::vector<std::string>& keys) = 0;
    virtual butil::Status TxnCheckTxnStatus(std::shared_ptr<Context> ctx, const std::string& primary_key,
                                            int64_t lock_ts, int64_t caller_start_ts, int64_t current_ts,
                                            bool force_sync_commit) = 0;
    virtual butil::Status TxnCheckSecondaryLocks(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                                 int64_t start_ts, const std::vector<std::string>& keys) = 0;
    virtual butil::Status TxnResolveLock(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                         const std::vector<std::string>& keys) = 0;
    virtual butil::Status TxnBatchRollback(std::shared_ptr<Context> ctx, int64_t start_ts,
                                           const std::vector<std::string>& keys) = 0;
    virtual butil::Status TxnHeartBeat(std::shared_ptr<Context> ctx, const std::string& primary_lock, int64_t start_ts,
                                       int64_t advise_lock_ttl) = 0;
    virtual butil::Status TxnDeleteRange(std::shared_ptr<Context> ctx, const std::string& start_key,
                                         const std::string& end_key) = 0;
    virtual butil::Status TxnGc(std::shared_ptr<Context> ctx, int64_t safe_point_ts) = 0;
  };
  using TxnWriterPtr = std::shared_ptr<TxnWriter>;

  virtual mvcc::ReaderPtr NewMVCCReader(pb::common::RawEngine type) = 0;

  virtual ReaderPtr NewReader(pb::common::RawEngine type) = 0;
  virtual WriterPtr NewWriter(pb::common::RawEngine type) = 0;
  virtual VectorReaderPtr NewVectorReader(pb::common::RawEngine type) = 0;
  virtual DocumentReaderPtr NewDocumentReader(pb::common::RawEngine type) = 0;

  virtual TxnReaderPtr NewTxnReader(pb::common::RawEngine type) = 0;
  virtual TxnWriterPtr NewTxnWriter(pb::common::RawEngine type) = 0;

  //  This is used by RaftStoreEngine to Persist Meta
  //  This is a alternative method, will be replace by zihui new Interface.
  virtual butil::Status MetaPut(std::shared_ptr<Context> /*ctx*/,
                                const pb::coordinator_internal::MetaIncrement& /*meta*/) {
    return butil::Status(Errno::ENOT_SUPPORT, "Not support");
  }

 protected:
  Engine() = default;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ENGINE_H_  // NOLINT
