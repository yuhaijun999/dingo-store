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

#ifndef DINGODB_ENGINE_STORAGE_H_
#define DINGODB_ENGINE_STORAGE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/context.h"
#include "engine/engine.h"
#include "engine/raft_store_engine.h"
#include "engine/raw_engine.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

class Storage {
 public:
  Storage(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Engine> mono_engine, mvcc::TsProviderPtr ts_provider);
  ~Storage() = default;

  // kv read
  butil::Status KvGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                      std::vector<pb::common::KeyValue>& kvs);

  butil::Status KvScanBegin(std::shared_ptr<Context> ctx, const std::string& cf_name, int64_t region_id,
                            const pb::common::Range& range, int64_t max_fetch_cnt, bool key_only,
                            bool disable_auto_release, bool disable_coprocessor,
                            const pb::store::Coprocessor& coprocessor, std::string* scan_id,
                            std::vector<pb::common::KeyValue>* kvs);

  static butil::Status KvScanContinue(std::shared_ptr<Context> ctx, const std::string& scan_id, int64_t max_fetch_cnt,
                                      std::vector<pb::common::KeyValue>* kvs, bool& has_more);

  static butil::Status KvScanRelease(std::shared_ptr<Context> ctx, const std::string& scan_id);

  butil::Status KvScanBeginV2(std::shared_ptr<Context> ctx, const std::string& cf_name, int64_t region_id,
                              const pb::common::Range& range, int64_t max_fetch_cnt, bool key_only,
                              bool disable_auto_release, bool disable_coprocessor,
                              const pb::common::CoprocessorV2& coprocessor, int64_t scan_id,
                              std::vector<pb::common::KeyValue>* kvs);

  static butil::Status KvScanContinueV2(std::shared_ptr<Context> ctx, int64_t scan_id, int64_t max_fetch_cnt,
                                        std::vector<pb::common::KeyValue>* kvs, bool& has_more);

  static butil::Status KvScanReleaseV2(std::shared_ptr<Context> ctx, int64_t scan_id);

  // kv write
  butil::Status KvPut(std::shared_ptr<Context> ctx, std::vector<pb::common::KeyValue>& kvs);

  butil::Status KvPutIfAbsent(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                              bool is_atomic, std::vector<bool>& key_states);

  butil::Status KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                         std::vector<bool>& key_states);

  butil::Status KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range);

  butil::Status KvCompareAndSet(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                const std::vector<std::string>& expect_values, bool is_atomic,
                                std::vector<bool>& key_states);

  // txn reader
  butil::Status TxnBatchGet(std::shared_ptr<Context> ctx, int64_t start_ts, const std::vector<std::string>& keys,
                            const std::set<int64_t>& resolved_locks, pb::store::TxnResultInfo& txn_result_info,
                            std::vector<pb::common::KeyValue>& kvs);
  butil::Status TxnScan(std::shared_ptr<Context> ctx, const pb::stream::StreamRequestMeta& req_stream_meta,
                        int64_t start_ts, const pb::common::Range& range, int64_t limit, bool key_only, bool is_reverse,
                        const std::set<int64_t>& resolved_locks, pb::store::TxnResultInfo& txn_result_info,
                        std::vector<pb::common::KeyValue>& kvs, bool& has_more, std::string& end_scan_key,
                        bool disable_coprocessor, const pb::common::CoprocessorV2& coprocessor);
  butil::Status TxnScanLock(std::shared_ptr<Context> ctx, const pb::stream::StreamRequestMeta& req_stream_meta,
                            int64_t max_ts, const pb::common::Range& range, int64_t limit,
                            pb::store::TxnResultInfo& txn_result_info, std::vector<pb::store::LockInfo>& lock_infos,
                            bool& has_more, std::string& end_scan_key);
  butil::Status TxnDump(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                        int64_t start_ts, int64_t end_ts, pb::store::TxnResultInfo& txn_result_info,
                        std::vector<pb::store::TxnWriteKey>& txn_write_keys,
                        std::vector<pb::store::TxnWriteValue>& txn_write_values,
                        std::vector<pb::store::TxnLockKey>& txn_lock_keys,
                        std::vector<pb::store::TxnLockValue>& txn_lock_values,
                        std::vector<pb::store::TxnDataKey>& txn_data_keys,
                        std::vector<pb::store::TxnDataValue>& txn_data_values);
  // txn writer
  butil::Status TxnPessimisticLock(std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation>& mutations,
                                   const std::string& primary_lock, int64_t start_ts, int64_t lock_ttl,
                                   int64_t for_update_ts, bool return_values, std::vector<pb::common::KeyValue>& kvs);
  butil::Status TxnPessimisticRollback(std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts,
                                       int64_t for_update_ts, const std::vector<std::string>& keys);
  butil::Status TxnPrewrite(std::shared_ptr<Context> ctx, store::RegionPtr region,
                            const std::vector<pb::store::Mutation>& mutations, const std::string& primary_lock,
                            int64_t start_ts, int64_t lock_ttl, int64_t txn_size, bool try_one_pc,
                            int64_t min_commit_ts, int64_t max_commit_ts,
                            const std::vector<int64_t>& pessimistic_checks,
                            const std::map<int64_t, int64_t>& for_update_ts_checks,
                            const std::map<int64_t, std::string>& lock_extra_datas,
                            const std::vector<std::string>& secondaries);
  butil::Status TxnCommit(std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts, int64_t commit_ts,
                          const std::vector<std::string>& keys);
  butil::Status TxnBatchRollback(std::shared_ptr<Context> ctx, int64_t start_ts, const std::vector<std::string>& keys);
  butil::Status TxnCheckTxnStatus(std::shared_ptr<Context> ctx, const std::string& primary_key, int64_t lock_ts,
                                  int64_t caller_start_ts, int64_t current_ts, bool force_sync_commit);
  butil::Status TxnCheckSecondaryLocks(std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts,
                                       const std::vector<std::string>& keys);
  butil::Status TxnResolveLock(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                               const std::vector<std::string>& keys);
  butil::Status TxnHeartBeat(std::shared_ptr<Context> ctx, const std::string& primary_lock, int64_t start_ts,
                             int64_t advise_lock_ttl);
  butil::Status TxnGc(std::shared_ptr<Context> ctx, int64_t safe_point_ts);
  butil::Status TxnDeleteRange(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key);

  // vector index
  butil::Status VectorAdd(std::shared_ptr<Context> ctx, bool is_sync,
                          const std::vector<pb::common::VectorWithId>& vectors, bool is_update);
  butil::Status VectorDelete(std::shared_ptr<Context> ctx, bool is_sync, store::RegionPtr region,
                             const std::vector<int64_t>& ids);

  butil::Status VectorBatchQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                 std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status VectorBatchSearch(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                  std::vector<pb::index::VectorWithDistanceResult>& results);
  butil::Status VectorGetBorderId(store::RegionPtr region, bool get_min, int64_t ts, int64_t& vector_id);
  butil::Status VectorScanQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status VectorGetRegionMetrics(store::RegionPtr region, VectorIndexWrapperPtr vector_index_wrapper,
                                       pb::common::VectorIndexMetrics& region_metrics);

  butil::Status VectorCount(store::RegionPtr region, pb::common::Range range, int64_t ts, int64_t& count);
  butil::Status VectorCountMemory(std::shared_ptr<Engine::VectorReader::Context> ctx, int64_t& count);

  butil::Status VectorImport(std::shared_ptr<Context> ctx, bool is_sync,
                             const std::vector<pb::common::VectorWithId>& vectors,
                             const std::vector<int64_t>& delete_ids);

  butil::Status VectorBuild(std::shared_ptr<Engine::VectorReader::Context> ctx,
                            const pb::common::VectorBuildParameter& parameter, int64_t ts,
                            pb::common::VectorStateParameter& vector_state_parameter);

  butil::Status VectorLoad(std::shared_ptr<Engine::VectorReader::Context> ctx,
                           const pb::common::VectorLoadParameter& parameter,
                           pb::common::VectorStateParameter& vector_state_parameter);

  butil::Status VectorStatus(std::shared_ptr<Engine::VectorReader::Context> ctx,
                             pb::common::VectorStateParameter& vector_state_parameter,
                             pb::error::Error& internal_error);

  butil::Status VectorReset(std::shared_ptr<Engine::VectorReader::Context> ctx, bool delete_data_file,
                            pb::common::VectorStateParameter& vector_state_parameter);

  butil::Status VectorDump(std::shared_ptr<Engine::VectorReader::Context> ctx, bool dump_all,
                           std::vector<std::string>& dump_datas);

  static butil::Status VectorCalcDistance(
      const ::dingodb::pb::index::VectorCalcDistanceRequest& request,
      std::vector<std::vector<float>>& distances,                            // NOLINT
      std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,    // NOLINT
      std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors);  // NOLINT

  // This function is for testing only
  butil::Status VectorBatchSearchDebug(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                       std::vector<pb::index::VectorWithDistanceResult>& results,
                                       int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                       int64_t& search_time_us);

  // document index
  butil::Status DocumentAdd(std::shared_ptr<Context> ctx, bool is_sync,
                            const std::vector<pb::common::DocumentWithId>& document_with_ids, bool is_update);
  butil::Status DocumentDelete(std::shared_ptr<Context> ctx, bool is_sync, store::RegionPtr region,
                               const std::vector<int64_t>& ids);

  butil::Status DocumentBatchQuery(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                   std::vector<pb::common::DocumentWithId>& document_with_ids);
  butil::Status DocumentSearch(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                               std::vector<pb::common::DocumentWithScore>& results);
  butil::Status DocumentSearchAll(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                  const pb::stream::StreamRequestMeta& req_stream_meta, bool& has_more,
                                  std::vector<pb::common::DocumentWithScore>& results);
  butil::Status DocumentGetBorderId(store::RegionPtr region, bool get_min, int64_t ts, int64_t& document_id);
  butil::Status DocumentScanQuery(std::shared_ptr<Engine::DocumentReader::Context> ctx,
                                  std::vector<pb::common::DocumentWithId>& document_with_ids);
  butil::Status DocumentGetRegionMetrics(store::RegionPtr region, DocumentIndexWrapperPtr document_index_wrapper,
                                         pb::common::DocumentIndexMetrics& region_metrics);

  butil::Status DocumentCount(store::RegionPtr region, pb::common::Range range, int64_t ts, int64_t& count);

  // common functions
  RaftStoreEnginePtr GetRaftStoreEngine();

  std::shared_ptr<Engine> GetStoreEngine(pb::common::StorageEngine store_engine_type);

  mvcc::ReaderPtr GetEngineMVCCReader(pb::common::StorageEngine store_engine_type,
                                      pb::common::RawEngine raw_engine_type);

  Engine::ReaderPtr GetEngineReader(pb::common::StorageEngine store_engine_type, pb::common::RawEngine raw_engine_type);
  Engine::TxnReaderPtr GetEngineTxnReader(pb::common::StorageEngine store_engine_type,
                                          pb::common::RawEngine raw_engine_type);
  Engine::WriterPtr GetEngineWriter(pb::common::StorageEngine store_engine_type, pb::common::RawEngine raw_engine_type);
  Engine::TxnWriterPtr GetEngineTxnWriter(pb::common::StorageEngine store_engine_type,
                                          pb::common::RawEngine raw_engine_type);
  Engine::VectorReaderPtr GetEngineVectorReader(pb::common::StorageEngine store_engine_type,
                                                pb::common::RawEngine raw_engine_type);
  Engine::DocumentReaderPtr GetEngineDocumentReader(pb::common::StorageEngine store_engine_type,
                                                    pb::common::RawEngine raw_engine_type);
  RawEnginePtr GetRawEngine(pb::common::StorageEngine store_engine_type, pb::common::RawEngine raw_engine_type);

  static Snapshot* GetSnapshot();
  void ReleaseSnapshot();

  butil::Status ValidateLeader(int64_t region_id);
  butil::Status ValidateLeader(store::RegionPtr region);
  bool IsLeader(int64_t region_id);
  bool IsLeader(store::RegionPtr region);

  butil::Status PrepareMerge(std::shared_ptr<Context> ctx, int64_t job_id,
                             const pb::common::RegionDefinition& region_definition, int64_t min_applied_log_id);
  butil::Status CommitMerge(std::shared_ptr<Context> ctx, int64_t job_id,
                            const pb::common::RegionDefinition& region_definition, int64_t prepare_merge_log_id,
                            const std::vector<pb::raft::LogEntry>& entries);

  butil::Status BackupData(std::shared_ptr<Context> ctx, store::RegionPtr region,
                           const pb::common::RegionType& region_type, std::string backup_ts, int64_t backup_tso,
                           const std::string& storage_path, const pb::common::StorageBackend& storage_backend,
                           const pb::common::CompressionType& compression_type, int32_t compression_level,
                           dingodb::pb::store::BackupDataResponse* response);

  butil::Status BackupMeta(std::shared_ptr<Context> ctx, store::RegionPtr region,
                           const pb::common::RegionType& region_type, std::string backup_ts, int64_t backup_tso,
                           const std::string& storage_path, const pb::common::StorageBackend& storage_backend,
                           const pb::common::CompressionType& compression_type, int32_t compression_level,
                           dingodb::pb::store::BackupMetaResponse* response);

  static butil::Status ControlConfig(std::shared_ptr<Context> ctx,
                                     const std::vector<pb::common::ControlConfigVariable>& variables,
                                     dingodb::pb::store::ControlConfigResponse* response);

  butil::Status RestoreMeta(std::shared_ptr<Context> ctx, store::RegionPtr region, std::string backup_ts,
                            int64_t backup_tso, const pb::common::StorageBackend& storage_backend,
                            const dingodb::pb::common::BackupDataFileValueSstMetaGroup& sst_metas);

  butil::Status RestoreData(std::shared_ptr<Context> ctx, store::RegionPtr region, std::string backup_ts,
                            int64_t backup_tso, const pb::common::StorageBackend& storage_backend,
                            const dingodb::pb::common::BackupDataFileValueSstMetaGroup& sst_metas);

 private:
  std::shared_ptr<Engine> raft_engine_;
  std::shared_ptr<Engine> mono_engine_;

  mvcc::TsProviderPtr ts_provider_;
};

using StoragePtr = std::shared_ptr<Storage>;

}  // namespace dingodb

#endif  // DINGODB_ENGINE_STORAGE_H
