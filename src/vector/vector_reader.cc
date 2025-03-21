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

#include "vector/vector_reader.h"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coprocessor/coprocessor_scalar.h"
#include "coprocessor/coprocessor_v2.h"
#include "coprocessor/utils.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"
#include "vector/codec.h"
#include "vector/vector_index.h"
#include "vector/vector_index_factory.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

#ifndef ENABLE_SCALAR_WITH_COPROCESSOR
#define ENABLE_SCALAR_WITH_COPROCESSOR
#endif
// #undef ENABLE_SCALAR_WITH_COPROCESSOR

DEFINE_int64(vector_index_max_range_search_result_count, 1024, "max range search result count");
DEFINE_int64(vector_index_bruteforce_batch_count, 2048, "bruteforce batch count");
DEFINE_bool(dingo_log_switch_scalar_speed_up_detail, false, "scalar speed up log");

bvar::LatencyRecorder g_bruteforce_search_latency("dingo_bruteforce_search_latency");
bvar::LatencyRecorder g_bruteforce_range_search_latency("dingo_bruteforce_range_search_latency");

DECLARE_bool(dingo_log_switch_coprocessor_scalar_detail);

butil::Status VectorReader::QueryVectorWithId(int64_t ts, const pb::common::Range& region_range, int64_t partition_id,
                                              int64_t vector_id, bool with_vector_data,
                                              pb::common::VectorWithId& vector_with_id) {
  std::string plain_key = VectorCodec::PackageVectorKey(Helper::GetKeyPrefix(region_range), partition_id, vector_id);

  std::string plain_value;
  auto status = reader_->KvGet(Constant::kVectorDataCF, ts, plain_key, plain_value);
  if (!status.ok()) {
    return status;
  }

  if (with_vector_data) {
    pb::common::Vector vector;
    CHECK(vector.ParseFromString(plain_value)) << "Parse vector proto error";

    vector_with_id.mutable_vector()->Swap(&vector);
  }

  vector_with_id.set_id(vector_id);

  return butil::Status();
}

butil::Status VectorReader::SearchVector(
    int64_t ts, int64_t partition_id, VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    const pb::common::ScalarSchema& scalar_schema,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "Empty vector with ids";
    return butil::Status();
  }

  auto vector_filter = parameter.vector_filter();
  auto vector_filter_type = parameter.vector_filter_type();

  bool with_vector_data = !(parameter.without_vector_data());
  std::vector<pb::index::VectorWithDistanceResult> tmp_results;

  // scalar post filter
  if (dingodb::pb::common::VectorFilter::SCALAR_FILTER == vector_filter &&
      dingodb::pb::common::VectorFilterType::QUERY_POST == vector_filter_type) {
    uint32_t top_n = parameter.top_n();
    bool enable_range_search = parameter.enable_range_search();

    if (BAIDU_UNLIKELY(vector_with_ids[0].scalar_data().scalar_data_size() == 0) &&
        !parameter.has_vector_coprocessor()) {
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(
          vector_index, region_range, vector_with_ids, parameter, vector_with_distance_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
    } else if (parameter.has_vector_coprocessor()) {
      if (BAIDU_UNLIKELY(vector_with_ids[0].scalar_data().scalar_data_size() != 0)) {
        DINGO_LOG(WARNING) << "vector_with_ids[0].scalar_data() deprecated. use coprocessor.";
      }
      top_n *= 10;
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids,
                                                                       parameter, tmp_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }

      std::shared_ptr<RawCoprocessor> scalar_coprocessor =
          std::make_shared<CoprocessorScalar>(Helper::GetKeyPrefix(region_range.start_key()));

      status = scalar_coprocessor->Open(CoprocessorPbWrapper{parameter.vector_coprocessor()});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << "scalar coprocessor::Open failed " << status.error_cstr();
        return status;
      }

      for (auto& vector_with_distance_result : tmp_results) {
        pb::index::VectorWithDistanceResult new_vector_with_distance_result;

        for (auto& temp_vector_with_distance : *vector_with_distance_result.mutable_vector_with_distances()) {
          int64_t temp_id = temp_vector_with_distance.vector_with_id().id();
          bool compare_result = false;
          butil::Status status = CompareVectorScalarDataWithCoprocessor(ts, region_range, partition_id, temp_id,
                                                                        scalar_coprocessor, compare_result);
          if (!status.ok()) {
            return status;
          }

          if (!compare_result) {
            continue;
          }

          new_vector_with_distance_result.add_vector_with_distances()->Swap(&temp_vector_with_distance);
          // topk
          if (!enable_range_search) {
            if (new_vector_with_distance_result.vector_with_distances_size() >= parameter.top_n()) {
              break;
            }
          }
        }
        vector_with_distance_results.emplace_back(std::move(new_vector_with_distance_result));
      }

    } else {  //! parameter.has_vector_coprocessor() && vector_with_ids[0].scalar_data().scalar_data_size() != 0
      top_n *= 10;
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids,
                                                                       parameter, tmp_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }

      for (auto& vector_with_distance_result : tmp_results) {
        pb::index::VectorWithDistanceResult new_vector_with_distance_result;

        for (auto& temp_vector_with_distance : *vector_with_distance_result.mutable_vector_with_distances()) {
          int64_t temp_id = temp_vector_with_distance.vector_with_id().id();
          bool compare_result = false;
          butil::Status status = CompareVectorScalarData(ts, region_range, partition_id, temp_id,
                                                         vector_with_ids[0].scalar_data(), compare_result);
          if (!status.ok()) {
            return status;
          }
          if (!compare_result) {
            continue;
          }

          new_vector_with_distance_result.add_vector_with_distances()->Swap(&temp_vector_with_distance);
          // topk
          if (!enable_range_search) {
            if (new_vector_with_distance_result.vector_with_distances_size() >= parameter.top_n()) {
              break;
            }
          }
        }
        vector_with_distance_results.emplace_back(std::move(new_vector_with_distance_result));
      }
    }
  } else if (dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER == vector_filter) {  // vector id array search
    butil::Status status = DoVectorSearchForVectorIdPreFilter(vector_index, vector_with_ids, parameter, region_range,
                                                              vector_with_distance_results);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForVectorIdPreFilter failed");
      return status;
    }
  } else if (dingodb::pb::common::VectorFilter::SCALAR_FILTER == vector_filter &&
             dingodb::pb::common::VectorFilterType::QUERY_PRE == vector_filter_type) {  // scalar pre filter search

    butil::Status status = DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                            scalar_schema, vector_with_distance_results);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForScalarPreFilter failed : {}", status.error_cstr());
      return status;
    }
  } else if (dingodb::pb::common::VectorFilter::TABLE_FILTER ==
             vector_filter) {  //  table coprocessor pre filter search. not impl
    butil::Status status = DoVectorSearchForTableCoprocessor(vector_index, region_range, vector_with_ids, parameter,
                                                             vector_with_distance_results);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForTableCoprocessor failed  : {}", status.error_cstr());
      return status;
    }
  }

  // if vector index does not support restruct vector ,we restruct it using RocksDB
  if (with_vector_data) {
    for (auto& result : vector_with_distance_results) {
      for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
        if (vector_with_distance.vector_with_id().vector().float_values_size() > 0 ||
            vector_with_distance.vector_with_id().vector().binary_values_size() > 0) {
          continue;
        }

        pb::common::VectorWithId vector_with_id;
        auto status = QueryVectorWithId(ts, region_range, partition_id, vector_with_distance.vector_with_id().id(),
                                        true, vector_with_id);
        if (!status.ok()) {
          if (status.error_code() == pb::error::EKEY_NOT_FOUND) {
            DINGO_LOG(WARNING) << fmt::format("Vector not found,  partition_id: {}, region_id : {},  vector_id: {}",
                                              partition_id, vector_index->Id(),
                                              vector_with_distance.vector_with_id().id());
            continue;
          }
          return status;
        }
        vector_with_distance.mutable_vector_with_id()->Swap(&vector_with_id);
      }
    }
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorTableData(int64_t ts, const pb::common::Range& region_range,
                                                 int64_t partition_id, pb::common::VectorWithId& vector_with_id) {
  std::string plain_key = VectorCodec::PackageVectorKey(region_range.start_key()[0], partition_id, vector_with_id.id());

  std::string plain_value;
  auto status = reader_->KvGet(Constant::kVectorTableCF, ts, plain_key, plain_value);
  if (!status.ok()) {
    return status;
  }

  pb::common::VectorTableData vector_table;
  CHECK(vector_table.ParseFromString(plain_value)) << "Prase vector table data error.";

  *(vector_with_id.mutable_table_data()) = vector_table;

  return butil::Status();
}

butil::Status VectorReader::QueryVectorTableData(int64_t ts, const pb::common::Range& region_range,
                                                 int64_t partition_id,
                                                 std::vector<pb::index::VectorWithDistanceResult>& results) {
  // get metadata by parameter
  for (auto& result : results) {
    for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
      pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
      QueryVectorTableData(ts, region_range, partition_id, vector_with_id);
    }
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorTableData(int64_t ts, const pb::common::Range& region_range,
                                                 int64_t partition_id,
                                                 std::vector<pb::common::VectorWithDistance>& vector_with_distances) {
  // get metadata by parameter
  for (auto& vector_with_distance : vector_with_distances) {
    pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
    QueryVectorTableData(ts, region_range, partition_id, vector_with_id);
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorScalarData(int64_t ts, const pb::common::Range& region_range,
                                                  int64_t partition_id, std::vector<std::string> selected_scalar_keys,
                                                  pb::common::VectorWithId& vector_with_id) {
  std::string plain_key =
      VectorCodec::PackageVectorKey(Helper::GetKeyPrefix(region_range), partition_id, vector_with_id.id());

  std::string plain_value;
  auto status = reader_->KvGet(Constant::kVectorScalarCF, ts, plain_key, plain_value);
  if (!status.ok()) {
    return status;
  }

  pb::common::VectorScalardata vector_scalar;
  CHECK(vector_scalar.ParseFromString(plain_value)) << "Parase vector scalar data error.";

  auto* scalar = vector_with_id.mutable_scalar_data()->mutable_scalar_data();
  for (const auto& [key, value] : vector_scalar.scalar_data()) {
    if (!selected_scalar_keys.empty() &&
        std::find(selected_scalar_keys.begin(), selected_scalar_keys.end(), key) == selected_scalar_keys.end()) {
      continue;
    }

    scalar->insert({key, value});
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorScalarData(int64_t ts, const pb::common::Range& region_range,
                                                  int64_t partition_id, std::vector<std::string> selected_scalar_keys,
                                                  std::vector<pb::index::VectorWithDistanceResult>& results) {
  // get metadata by parameter
  for (auto& result : results) {
    for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
      pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
      QueryVectorScalarData(ts, region_range, partition_id, selected_scalar_keys, vector_with_id);
    }
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorScalarData(int64_t ts, const pb::common::Range& region_range,
                                                  int64_t partition_id, std::vector<std::string> selected_scalar_keys,
                                                  std::vector<pb::common::VectorWithDistance>& vector_with_distances) {
  // get metadata by parameter
  for (auto& vector_with_distance : vector_with_distances) {
    pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
    QueryVectorScalarData(ts, region_range, partition_id, selected_scalar_keys, vector_with_id);
  }

  return butil::Status();
}

butil::Status VectorReader::CompareVectorScalarData(int64_t ts, const pb::common::Range& region_range,
                                                    int64_t partition_id, int64_t vector_id,
                                                    const pb::common::VectorScalardata& source_scalar_data,
                                                    bool& compare_result) {
  compare_result = false;

  std::string plain_key = VectorCodec::PackageVectorKey(Helper::GetKeyPrefix(region_range), partition_id, vector_id);

  std::string plain_value;
  auto status = reader_->KvGet(Constant::kVectorScalarCF, ts, plain_key, plain_value);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("Get vector scalar data failed, vector_id: {} error: {} ", vector_id,
                                      status.error_str());
    return status;
  }

  pb::common::VectorScalardata vector_scalar;
  CHECK(vector_scalar.ParseFromString(plain_value)) << "Parse vector scalar data error.";

  for (const auto& [key, value] : source_scalar_data.scalar_data()) {
    auto it = vector_scalar.scalar_data().find(key);
    if (it == vector_scalar.scalar_data().end()) {
      compare_result = false;
      return butil::Status();
    }

    compare_result = Helper::IsEqualVectorScalarValue(value, it->second);
    if (!compare_result) {
      return butil::Status();
    }
  }

  compare_result = true;
  return butil::Status();
}

butil::Status VectorReader::CompareVectorScalarDataWithCoprocessor(
    int64_t ts, const pb::common::Range& region_range, int64_t partition_id, int64_t vector_id,
    const std::shared_ptr<RawCoprocessor>& scalar_coprocessor, bool& compare_result) {
  compare_result = false;

  std::string plain_key = VectorCodec::PackageVectorKey(Helper::GetKeyPrefix(region_range), partition_id, vector_id);

  std::string plain_value;
  auto status = reader_->KvGet(Constant::kVectorScalarCF, ts, plain_key, plain_value);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("Get vector scalar data failed, vector_id: {} error: {} ", vector_id,
                                      status.error_str());
    return status;
  }

  pb::common::VectorScalardata vector_scalar;
  CHECK(vector_scalar.ParseFromString(plain_value)) << "Parse vector scalar data error.";

  auto lambda_scalar_compare_with_coprocessor_function =
      [&scalar_coprocessor](const pb::common::VectorScalardata& internal_vector_scalar) {
        bool is_reverse = false;
        butil::Status status = scalar_coprocessor->Filter(internal_vector_scalar, is_reverse);
        if (!status.ok()) {
          LOG(WARNING) << "scalar coprocessor filter failed, error: " << status.error_cstr();
          return false;
        }
        return is_reverse;
      };

  compare_result = lambda_scalar_compare_with_coprocessor_function(vector_scalar);

  return butil::Status();
}

butil::Status VectorReader::VectorBatchSearch(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                              std::vector<pb::index::VectorWithDistanceResult>& results) {  // NOLINT
  // Search vectors by vectors
  auto status = SearchVector(ctx->ts, ctx->partition_id, ctx->vector_index, ctx->region_range, ctx->vector_with_ids,
                             ctx->parameter, ctx->scalar_schema, results);
  if (!status.ok()) {
    return status;
  }

  if (!ctx->parameter.without_scalar_data()) {
    // Get scalar data by parameter
    std::vector<std::string> selected_scalar_keys = Helper::PbRepeatedToVector(ctx->parameter.selected_keys());
    auto status = QueryVectorScalarData(ctx->ts, ctx->region_range, ctx->partition_id, selected_scalar_keys, results);
    if (!status.ok()) {
      return status;
    }
  }

  if (!ctx->parameter.without_table_data()) {
    // Get table data by parameter
    auto status = QueryVectorTableData(ctx->ts, ctx->region_range, ctx->partition_id, results);
    if (!status.ok()) {
      return status;
    }
  }

  return butil::Status();
}

butil::Status VectorReader::VectorBatchQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                             std::vector<pb::common::VectorWithId>& vector_with_ids) {
  for (auto vector_id : ctx->vector_ids) {
    pb::common::VectorWithId vector_with_id;
    auto status = QueryVectorWithId(ctx->ts, ctx->region_range, ctx->partition_id, vector_id, ctx->with_vector_data,
                                    vector_with_id);
    if ((!status.ok()) && status.error_code() != pb::error::EKEY_NOT_FOUND) {
      DINGO_LOG(WARNING) << fmt::format("Query vector_with_id failed, vector_id: {} error: {}", vector_id,
                                        status.error_str());
    }

    // if the id is not exist, the vector_with_id will be empty, sdk client will handle this
    vector_with_ids.push_back(vector_with_id);
  }

  if (ctx->with_scalar_data) {
    for (auto& vector_with_id : vector_with_ids) {
      if (vector_with_id.ByteSizeLong() == 0) {
        continue;
      }

      auto status = QueryVectorScalarData(ctx->ts, ctx->region_range, ctx->partition_id, ctx->selected_scalar_keys,
                                          vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("Query vector scalar data failed, vector_id: {} error: {} ",
                                          vector_with_id.id(), status.error_str());
      }
    }
  }

  if (ctx->with_table_data) {
    for (auto& vector_with_id : vector_with_ids) {
      if (vector_with_id.ByteSizeLong() == 0) {
        continue;
      }

      auto status = QueryVectorTableData(ctx->ts, ctx->region_range, ctx->partition_id, vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("Query vector table data failed, vector_id: {} error: {} ",
                                          vector_with_id.id(), status.error_str());
      }
    }
  }

  return butil::Status::OK();
}

butil::Status VectorReader::VectorGetBorderId(int64_t ts, const pb::common::Range& region_range, bool get_min,
                                              int64_t& vector_id) {
  return GetBorderId(ts, region_range, get_min, vector_id);
}

butil::Status VectorReader::VectorScanQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                            std::vector<pb::common::VectorWithId>& vector_with_ids) {
  DINGO_LOG(INFO) << fmt::format("Scan vector, region_id: {} start_id: {} is_reverse: {} limit: {}", ctx->region_id,
                                 ctx->start_id, ctx->is_reverse, ctx->limit);

  // scan for ids
  std::vector<int64_t> vector_ids;
  auto status = ScanVectorId(ctx, vector_ids);
  if (!status.ok()) {
    DINGO_LOG(INFO) << "Scan vector id failed, error: " << status.error_str();
    return status;
  }

  DINGO_LOG(INFO) << "scan vector id count: " << vector_ids.size();

  if (vector_ids.empty()) {
    return butil::Status();
  }

  // query vector with id
  for (auto vector_id : vector_ids) {
    pb::common::VectorWithId vector_with_id;
    auto status = QueryVectorWithId(ctx->ts, ctx->region_range, ctx->partition_id, vector_id, ctx->with_vector_data,
                                    vector_with_id);
    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format("Query vector data failed, vector_id {} error: {}", vector_id,
                                        status.error_str());
    }

    // if the id is not exist, the vector_with_id will be empty, sdk client will handle this
    vector_with_ids.push_back(vector_with_id);
  }

  if (ctx->with_scalar_data) {
    for (auto& vector_with_id : vector_with_ids) {
      if (vector_with_id.ByteSizeLong() == 0) {
        continue;
      }

      auto status = QueryVectorScalarData(ctx->ts, ctx->region_range, ctx->partition_id, ctx->selected_scalar_keys,
                                          vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("Query vector scalar data failed, vector_id {} error: {}",
                                          vector_with_id.id(), status.error_str());
      }
    }
  }

  if (ctx->with_table_data) {
    for (auto& vector_with_id : vector_with_ids) {
      if (vector_with_id.ByteSizeLong() == 0) {
        continue;
      }

      auto status = QueryVectorTableData(ctx->ts, ctx->region_range, ctx->partition_id, vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("Query vector table data failed, vector_id {} error: {}", vector_with_id.id(),
                                          status.error_str());
      }
    }
  }

  return butil::Status::OK();
}

butil::Status VectorReader::VectorGetRegionMetrics(int64_t /*region_id*/, const pb::common::Range& region_range,
                                                   VectorIndexWrapperPtr vector_index,
                                                   pb::common::VectorIndexMetrics& region_metrics) {
  int64_t total_vector_count = 0;
  int64_t total_deleted_count = 0;
  int64_t total_memory_usage = 0;
  int64_t max_id = 0;
  int64_t min_id = 0;

  auto inner_vector_index = vector_index->GetOwnVectorIndex();
  if (inner_vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", vector_index->Id());
  }

  auto status = inner_vector_index->GetCount(total_vector_count);
  if (!status.ok()) {
    return status;
  }

  status = inner_vector_index->GetDeletedCount(total_deleted_count);
  if (!status.ok()) {
    return status;
  }

  status = inner_vector_index->GetMemorySize(total_memory_usage);
  if (!status.ok()) {
    return status;
  }

  status = GetBorderId(0, region_range, true, min_id);
  if (!status.ok()) {
    return status;
  }

  status = GetBorderId(0, region_range, false, max_id);
  if (!status.ok()) {
    return status;
  }

  region_metrics.set_current_count(total_vector_count);
  region_metrics.set_deleted_count(total_deleted_count);
  region_metrics.set_memory_bytes(total_memory_usage);
  region_metrics.set_max_id(max_id);
  region_metrics.set_min_id(min_id);

  return butil::Status();
}

butil::Status VectorReader::VectorCount(int64_t ts, const pb::common::Range& range, int64_t& count) {
  return reader_->KvCount(Constant::kVectorDataCF, ts, range.start_key(), range.end_key(), count);
}

butil::Status VectorReader::VectorCountMemory(std::shared_ptr<Engine::VectorReader::Context> ctx, int64_t& count) {
  return ctx->vector_index->GetCount(count);
}

butil::Status VectorReader::VectorBuild(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                        const pb::common::VectorBuildParameter& parameter, int64_t ts,
                                        pb::common::VectorStateParameter& vector_state_parameter) {
  return ctx->vector_index->Build(ctx->region_range, reader_, parameter, ts, vector_state_parameter);
}

butil::Status VectorReader::VectorLoad(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                       const pb::common::VectorLoadParameter& parameter,
                                       pb::common::VectorStateParameter& vector_state_parameter) {
  return ctx->vector_index->Load(parameter, vector_state_parameter);
}

butil::Status VectorReader::VectorStatus(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                         pb::common::VectorStateParameter& vector_state_parameter,
                                         pb::error::Error& internal_error) {
  return ctx->vector_index->Status(vector_state_parameter, internal_error);
}

butil::Status VectorReader::VectorReset(std::shared_ptr<Engine::VectorReader::Context> ctx, bool delete_data_file,
                                        pb::common::VectorStateParameter& vector_state_parameter) {
  return ctx->vector_index->Reset(delete_data_file, vector_state_parameter);
}

butil::Status VectorReader::VectorDump(std::shared_ptr<Engine::VectorReader::Context> ctx, bool dump_all,
                                       std::vector<std::string>& dump_datas) {
  return ctx->vector_index->Dump(dump_all, dump_datas);
}

// GetBorderId
butil::Status VectorReader::GetBorderId(int64_t ts, const pb::common::Range& region_range, bool get_min,
                                        int64_t& vector_id) {
  const std::string& start_key = region_range.start_key();
  const std::string& end_key = region_range.end_key();

  std::string plain_key;
  if (get_min) {
    auto status = reader_->KvMinKey(Constant::kVectorDataCF, ts, start_key, end_key, plain_key);
    if (!status.ok()) {
      return status;
    }

  } else {
    auto status = reader_->KvMaxKey(Constant::kVectorDataCF, ts, start_key, end_key, plain_key);
    if (!status.ok()) {
      return status;
    }
  }

  vector_id = plain_key.empty() ? 0 : VectorCodec::UnPackageVectorId(plain_key);

  return butil::Status::OK();
}

// ScanVectorId
butil::Status VectorReader::ScanVectorId(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                         std::vector<int64_t>& vector_ids) {
  auto& range = ctx->region_range;
  std::string encode_seek_key =
      VectorCodec::EncodeVectorKey(Helper::GetKeyPrefix(range), ctx->partition_id, ctx->start_id);
  auto encode_range = mvcc::Codec::EncodeRange(range);

  if (!ctx->is_reverse) {
    if (encode_seek_key < encode_range.start_key()) {
      encode_seek_key = encode_range.start_key();
    }

    if (encode_seek_key >= encode_range.end_key()) {
      return butil::Status::OK();
    }

    IteratorOptions options;
    options.upper_bound = encode_range.end_key();
    auto iter = reader_->NewIterator(Constant::kVectorDataCF, ctx->ts, options);
    if (iter == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range {}", Helper::RangeToString(range));
      return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
    }
    for (iter->Seek(encode_seek_key); iter->Valid(); iter->Next()) {
      pb::common::VectorWithId vector;

      std::string key(iter->Key());
      int64_t vector_id = VectorCodec::DecodeVectorIdFromEncodeKeyWithTs(key);
      CHECK(vector_id > 0) << fmt::format("vector_id({}) is invaild", vector_id);

      if (ctx->end_id != 0 && vector_id > ctx->end_id) {
        break;
      }

      if (ctx->use_scalar_filter) {
        bool compare_result = false;
        auto status = CompareVectorScalarData(ctx->ts, ctx->region_range, ctx->partition_id, vector_id,
                                              ctx->scalar_data_for_filter, compare_result);
        if (!status.ok()) {
          return status;
        }
        if (!compare_result) {
          continue;
        }
      }

      vector_ids.push_back(vector_id);
      if (vector_ids.size() >= ctx->limit) {
        break;
      }
    }
  } else {
    encode_seek_key = Helper::PrefixNext(encode_seek_key);
    if (encode_seek_key > encode_range.end_key()) {
      encode_seek_key = encode_range.end_key();
    }

    if (encode_seek_key < encode_range.start_key()) {
      return butil::Status::OK();
    }

    IteratorOptions options;
    options.lower_bound = encode_range.start_key();
    auto iter = reader_->NewIterator(Constant::kVectorDataCF, ctx->ts, options);
    if (iter == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range {}", Helper::RangeToString(range));
      return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
    }

    for (iter->SeekForPrev(encode_seek_key); iter->Valid(); iter->Prev()) {
      pb::common::VectorWithId vector;

      std::string key(iter->Key());
      int64_t vector_id = VectorCodec::DecodeVectorIdFromEncodeKeyWithTs(key);
      CHECK(vector_id > 0) << fmt::format("vector_id({}) is invaild", vector_id);

      if (ctx->end_id != 0 && vector_id < ctx->end_id) {
        break;
      }

      if (ctx->use_scalar_filter) {
        bool compare_result = false;
        auto status = CompareVectorScalarData(ctx->ts, ctx->region_range, ctx->partition_id, vector_id,
                                              ctx->scalar_data_for_filter, compare_result);
        if (!status.ok()) {
          return status;
        }
        if (!compare_result) {
          continue;
        }
      }

      vector_ids.push_back(vector_id);
      if (vector_ids.size() >= ctx->limit) {
        break;
      }
    }
  }

  return butil::Status::OK();
}

butil::Status VectorReader::DoVectorSearchForVectorIdPreFilter(  // NOLINT
    VectorIndexWrapperPtr vector_index, const std::vector<pb::common::VectorWithId>& vector_with_ids,
    const pb::common::VectorSearchParameter& parameter, const pb::common::Range& region_range,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {
  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
  auto vector_ids = Helper::PbRepeatedToVector(parameter.vector_ids());
  auto status =
      VectorReader::SetVectorIndexIdsFilter(parameter.is_negation(), parameter.is_sorted(), vector_ids, filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_str();
    return status;
  }

  status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids, parameter,
                                                     vector_with_distance_results, parameter.top_n(), filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorReader::DoVectorSearchForScalarPreFilter(
    VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    const pb::common::ScalarSchema& scalar_schema,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {  // NOLINT
  // scalar pre filter search
  butil::Status status;

  // if vector index type is diskann, not support scalar pre filter search!!!
  if (vector_index->Type() == pb::common::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s = "DiskANN not support scalar pre filter search";
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
  }

  bool use_coprocessor = parameter.has_vector_coprocessor();

  if (!use_coprocessor && vector_with_ids[0].scalar_data().scalar_data_size() == 0) {
    std::string s = fmt::format("vector_with_ids[0].scalar_data() empty not support");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  bool enable_speed_up = false;
  if (use_coprocessor) {
    status = VectorIndexUtils::IsNeedToScanKeySpeedUpCF(scalar_schema, parameter.vector_coprocessor(), enable_speed_up);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

  } else {
    status =
        VectorIndexUtils::IsNeedToScanKeySpeedUpCF(scalar_schema, vector_with_ids[0].scalar_data(), enable_speed_up);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  std::set<std::string> compare_keys;
  std::shared_ptr<RawCoprocessor> scalar_coprocessor;

  if (use_coprocessor) {
    if (enable_speed_up) {
      status = Utils::CollectKeysFromCoprocessor(parameter.vector_coprocessor(), compare_keys);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
    }

    const auto& coprocessor = parameter.vector_coprocessor();

    scalar_coprocessor = std::make_shared<CoprocessorScalar>(Helper::GetKeyPrefix(region_range.start_key()));

    status = scalar_coprocessor->Open(CoprocessorPbWrapper{coprocessor});
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "scalar coprocessor::Open failed " << status.error_cstr();
      return status;
    }

  } else {
    if (enable_speed_up) {
      const auto& std_vector_scalar = vector_with_ids[0].scalar_data();
      for (const auto& [key, _] : std_vector_scalar.scalar_data()) {
        compare_keys.insert(key);
      }
    }
  }

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_scalar_speed_up_detail)
      << fmt::format("enable_speed_up: {} use_coprocessor: {} scalar_schema: {} compare_keys: {}",
                     (enable_speed_up ? "true" : "false"), (use_coprocessor ? "true" : "false"),
                     scalar_schema.ShortDebugString(), Helper::SetToString(compare_keys));

  std::vector<int64_t> vector_ids;
  vector_ids.reserve(1024);
  if (enable_speed_up) {
    const auto& std_vector_scalar = use_coprocessor ? pb::common::VectorScalardata() : vector_with_ids[0].scalar_data();
    status = InternalVectorSearchForScalarPreFilterWithScalarKeySpeedUpCF(
        region_range, compare_keys, use_coprocessor, scalar_coprocessor, std_vector_scalar, vector_ids);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  } else {
    const auto& std_vector_scalar = use_coprocessor ? pb::common::VectorScalardata() : vector_with_ids[0].scalar_data();
    status = InternalVectorSearchForScalarPreFilterWithScalarCF(region_range, use_coprocessor, scalar_coprocessor,
                                                                std_vector_scalar, vector_ids);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
  status = VectorReader::SetVectorIndexIdsFilter(false, false, vector_ids, filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids, parameter,
                                                     vector_with_distance_results, parameter.top_n(), filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

bool VectorReader::ScalarCompareCore(const pb::common::VectorScalardata& std_vector_scalar,
                                     const pb::common::VectorScalardata& internal_vector_scalar) {
  for (const auto& [key, value] : std_vector_scalar.scalar_data()) {
    auto it = internal_vector_scalar.scalar_data().find(key);
    if (it == internal_vector_scalar.scalar_data().end()) {
      return false;
    }

    bool compare_result = Helper::IsEqualVectorScalarValue(value, it->second);
    if (!compare_result) {
      return false;
    }
  }
  return true;
}

butil::Status VectorReader::InternalVectorSearchForScalarPreFilterWithScalarCF(
    pb::common::Range region_range, bool use_coprocessor, const std::shared_ptr<RawCoprocessor>& scalar_coprocessor,
    const pb::common::VectorScalardata& std_vector_scalar,
    std::vector<int64_t>& vector_ids) {  // NOLINT
  // scalar pre filter search
  butil::Status status;

  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_scalar_speed_up_detail)
      << fmt::format("exec vector search scalar pre filter with scalar");

  auto encode_range = mvcc::Codec::EncodeRange(region_range);

  IteratorOptions options;
  options.upper_bound = encode_range.end_key();

  auto iter = reader_->NewIterator(Constant::kVectorScalarCF, 0, options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range {}", Helper::RangeToString(region_range));
    return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
  }

  for (iter->Seek(encode_range.start_key()); iter->Valid(); iter->Next()) {
    pb::common::VectorScalardata internal_vector_scalar;
    std::string value(mvcc::Codec::UnPackageValue(iter->Value()));
    CHECK(internal_vector_scalar.ParseFromString(value)) << "Parse vector scalar data error.";

    bool compare_result = use_coprocessor ? ScalarCompareWithCoprocessorCore(scalar_coprocessor, internal_vector_scalar)
                                          : ScalarCompareCore(std_vector_scalar, internal_vector_scalar);

    if (compare_result) {
      std::string key(iter->Key());
      int64_t internal_vector_id = VectorCodec::DecodeVectorIdFromEncodeKeyWithTs(key);
      CHECK(internal_vector_id > 0) << fmt::format("decode vector id failed key: {}", Helper::StringToHex(key));
      vector_ids.push_back(internal_vector_id);
    }
  }

  return butil::Status::OK();
}

bool VectorReader::ScalarCompareWithCoprocessorCore(
    const std::shared_ptr<RawCoprocessor>& scalar_coprocessor,
    const pb::common::VectorScalardata& internal_vector_scalar) {  // NOLINT(*static)
  bool is_reserved = false;
  butil::Status status = scalar_coprocessor->Filter(internal_vector_scalar, is_reserved);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "scalar coprocessor filter failed, error: " << status.error_cstr();
    return false;
  }
  return is_reserved;
}

butil::Status VectorReader::InternalVectorSearchForScalarPreFilterWithScalarKeySpeedUpCF(
    pb::common::Range region_range, const std::set<std::string>& compare_keys, bool use_coprocessor,
    const std::shared_ptr<RawCoprocessor>& scalar_coprocessor, const pb::common::VectorScalardata& std_vector_scalar,
    std::vector<int64_t>& vector_ids) {  // NOLINT
  auto encode_range = mvcc::Codec::EncodeRange(region_range);

  IteratorOptions options;
  options.upper_bound = encode_range.end_key();

  auto iter = reader_->NewIterator(Constant::kVectorScalarKeySpeedUpCF, 0, options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range {}", Helper::RangeToString(region_range));
    return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
  }

  int64_t last_vector_id = std::numeric_limits<int64_t>::min();
  pb::common::VectorScalardata internal_vector_scalar;

  auto scalar_compare_func = [&vector_ids, &internal_vector_scalar, &last_vector_id, use_coprocessor,
                              &scalar_coprocessor, &std_vector_scalar, this]() {
    bool compare_result = use_coprocessor
                              ? this->ScalarCompareWithCoprocessorCore(scalar_coprocessor, internal_vector_scalar)
                              : this->ScalarCompareCore(std_vector_scalar, internal_vector_scalar);
    if (compare_result) {
      vector_ids.push_back(last_vector_id);
    }

    internal_vector_scalar.Clear();
    return butil::Status::OK();
  };

  for (iter->Seek(encode_range.start_key()); iter->Valid(); iter->Next()) {
    std::string key(iter->Key());
    int64_t vector_id = VectorCodec::DecodeVectorIdFromEncodeKeyWithTs(key);
    CHECK(vector_id > 0) << fmt::format("vector_id({}) is invaild", vector_id);

    if (last_vector_id != vector_id && last_vector_id != std::numeric_limits<int64_t>::min()) {
      auto status = scalar_compare_func();
      if (!status.ok()) {
        return status;
      }
    }

    std::string scalar_key = VectorCodec::DecodeScalarKeyFromEncodeKeyWithTs(key);
    CHECK(!scalar_key.empty()) << fmt::format("decode scalar key({}) failed.", Helper::StringToHex(key));

    if (compare_keys.find(scalar_key) != compare_keys.end()) {
      pb::common::ScalarValue scalar_value;
      std::string value(mvcc::Codec::UnPackageValue(iter->Value()));
      CHECK(scalar_value.ParseFromString(value)) << "Parse vector scalar data error.";

      internal_vector_scalar.mutable_scalar_data()->insert({scalar_key, scalar_value});
    }
    last_vector_id = vector_id;
  }

  if (last_vector_id != std::numeric_limits<int64_t>::min()) {
    auto status = scalar_compare_func();
    if (!status.ok()) {
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status VectorReader::DoVectorSearchForTableCoprocessor(  // NOLINT(*static)
    [[maybe_unused]] VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    [[maybe_unused]] const std::vector<pb::common::VectorWithId>& vector_with_ids,
    [[maybe_unused]] const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {  // NOLINT
  //   std::string s = fmt::format("vector index search table filter for coprocessor not support now !!! ");
  //   DINGO_LOG(ERROR) << s;
  //   return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);

  DINGO_LOG(DEBUG) << "vector index search table filter for coprocessor support";

  // if vector index type is diskann, not support scalar pre filter search!!!
  if (vector_index->Type() == pb::common::VECTOR_INDEX_TYPE_DISKANN) {
    std::string s = "DiskANN not support scalar pre filter search";
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
  }

  // table pre filter search

  if (!parameter.has_vector_coprocessor()) {
    std::string s = fmt::format("vector_coprocessor empty not support");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  const auto& coprocessor = parameter.vector_coprocessor();

  std::shared_ptr<RawCoprocessor> table_coprocessor =
      std::make_shared<CoprocessorV2>(Helper::GetKeyPrefix(region_range.start_key()));
  butil::Status status;
  status = table_coprocessor->Open(CoprocessorPbWrapper{coprocessor});
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "table coprocessor::Open failed " << status.error_cstr();
    return status;
  }

  auto lambda_table_compare_with_coprocessor_function = [&table_coprocessor](
                                                            const pb::common::VectorTableData& internal_vector_table) {
    bool is_reverse = false;
    butil::Status status =
        table_coprocessor->Filter(internal_vector_table.table_key(), internal_vector_table.table_value(), is_reverse);
    if (!status.ok()) {
      LOG(WARNING) << "scalar coprocessor filter failed, error: " << status.error_cstr();
      return false;
    }
    return is_reverse;
  };

  auto encode_range = mvcc::Codec::EncodeRange(region_range);

  IteratorOptions options;
  options.upper_bound = encode_range.end_key();

  auto iter = reader_->NewIterator(Constant::kVectorTableCF, 0, options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range {}", Helper::RangeToString(region_range));
    return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
  }

  std::vector<int64_t> vector_ids;
  vector_ids.reserve(1024);
  for (iter->Seek(encode_range.start_key()); iter->Valid(); iter->Next()) {
    pb::common::VectorTableData internal_vector_table;
    std::string value(mvcc::Codec::UnPackageValue(iter->Value()));
    CHECK(internal_vector_table.ParseFromString(value)) << "Parse vector table data error.";

    bool compare_result = lambda_table_compare_with_coprocessor_function(internal_vector_table);
    if (compare_result) {
      std::string key(iter->Key());
      int64_t internal_vector_id = VectorCodec::DecodeVectorIdFromEncodeKeyWithTs(key);
      CHECK(internal_vector_id > 0) << fmt::format("decode vector id failed key: {}", Helper::StringToHex(key));
      vector_ids.push_back(internal_vector_id);
    }
  }

  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
  status = VectorReader::SetVectorIndexIdsFilter(false, false, vector_ids, filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids, parameter,
                                                     vector_with_distance_results, parameter.top_n(), filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorReader::VectorBatchSearchDebug(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                                   std::vector<pb::index::VectorWithDistanceResult>& results,
                                                   int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                                   int64_t& search_time_us) {  // NOLINT
  // Search vectors by vectors
  auto status =
      SearchVectorDebug(ctx->partition_id, ctx->vector_index, ctx->region_range, ctx->vector_with_ids, ctx->parameter,
                        results, deserialization_id_time_us, scan_scalar_time_us, search_time_us);
  if (!status.ok()) {
    return status;
  }

  if (!ctx->parameter.without_scalar_data()) {
    // Get scalar data by parameter
    std::vector<std::string> selected_scalar_keys = Helper::PbRepeatedToVector(ctx->parameter.selected_keys());
    auto status = QueryVectorScalarData(ctx->ts, ctx->region_range, ctx->partition_id, selected_scalar_keys, results);
    if (!status.ok()) {
      return status;
    }
  }

  if (!ctx->parameter.without_table_data()) {
    // Get table data by parameter
    auto status = QueryVectorTableData(ctx->ts, ctx->region_range, ctx->partition_id, results);
    if (!status.ok()) {
      return status;
    }
  }

  return butil::Status();
}

butil::Status VectorReader::SearchVectorDebug(
    int64_t partition_id, VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results, int64_t& deserialization_id_time_us,
    int64_t& scan_scalar_time_us, int64_t& search_time_us) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "Empty vector with ids";
    return butil::Status();
  }

  auto vector_filter = parameter.vector_filter();
  auto vector_filter_type = parameter.vector_filter_type();

  bool with_vector_data = !(parameter.without_vector_data());
  std::vector<pb::index::VectorWithDistanceResult> tmp_results;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  // scalar post filter
  if (dingodb::pb::common::VectorFilter::SCALAR_FILTER == vector_filter &&
      dingodb::pb::common::VectorFilterType::QUERY_POST == vector_filter_type) {
    uint32_t top_n = parameter.top_n();
    bool enable_range_search = parameter.enable_range_search();

    if (BAIDU_UNLIKELY(vector_with_ids[0].scalar_data().scalar_data_size() == 0) &&
        !parameter.has_vector_coprocessor()) {
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(
          vector_index, region_range, vector_with_ids, parameter, vector_with_distance_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
    } else if (parameter.has_vector_coprocessor()) {
      auto start = lambda_time_now_function();
      if (BAIDU_UNLIKELY(vector_with_ids[0].scalar_data().scalar_data_size() != 0)) {
        DINGO_LOG(WARNING) << "vector_with_ids[0].scalar_data() deprecated. use coprocessor.";
      }
      top_n *= 10;
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids,
                                                                       parameter, tmp_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
      auto end = lambda_time_now_function();
      search_time_us = lambda_time_diff_microseconds_function(start, end);

      auto start_kv_get = lambda_time_now_function();
      std::shared_ptr<RawCoprocessor> scalar_coprocessor =
          std::make_shared<CoprocessorScalar>(Helper::GetKeyPrefix(region_range.start_key()));

      status = scalar_coprocessor->Open(CoprocessorPbWrapper{parameter.vector_coprocessor()});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << "scalar coprocessor::Open failed " << status.error_cstr();
        return status;
      }

      for (auto& vector_with_distance_result : tmp_results) {
        pb::index::VectorWithDistanceResult new_vector_with_distance_result;

        for (auto& temp_vector_with_distance : *vector_with_distance_result.mutable_vector_with_distances()) {
          int64_t temp_id = temp_vector_with_distance.vector_with_id().id();
          bool compare_result = false;
          butil::Status status = CompareVectorScalarDataWithCoprocessor(0, region_range, partition_id, temp_id,
                                                                        scalar_coprocessor, compare_result);
          if (!status.ok()) {
            return status;
          }
          if (!compare_result) {
            continue;
          }

          new_vector_with_distance_result.add_vector_with_distances()->Swap(&temp_vector_with_distance);
          if (!enable_range_search) {
            if (new_vector_with_distance_result.vector_with_distances_size() >= parameter.top_n()) {
              break;
            }
          }
        }
        vector_with_distance_results.emplace_back(std::move(new_vector_with_distance_result));
      }
      auto end_kv_get = lambda_time_now_function();
      scan_scalar_time_us = lambda_time_diff_microseconds_function(start_kv_get, end_kv_get);
    } else {
      top_n *= 10;
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids,
                                                                       parameter, tmp_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }

      for (auto& vector_with_distance_result : tmp_results) {
        pb::index::VectorWithDistanceResult new_vector_with_distance_result;

        for (auto& temp_vector_with_distance : *vector_with_distance_result.mutable_vector_with_distances()) {
          int64_t temp_id = temp_vector_with_distance.vector_with_id().id();
          bool compare_result = false;
          butil::Status status = CompareVectorScalarData(0, region_range, partition_id, temp_id,
                                                         vector_with_ids[0].scalar_data(), compare_result);
          if (!status.ok()) {
            return status;
          }
          if (!compare_result) {
            continue;
          }

          new_vector_with_distance_result.add_vector_with_distances()->Swap(&temp_vector_with_distance);
          // topk
          if (!enable_range_search) {
            if (new_vector_with_distance_result.vector_with_distances_size() >= parameter.top_n()) {
              break;
            }
          }
        }
        vector_with_distance_results.emplace_back(std::move(new_vector_with_distance_result));
      }
    }

  } else if (dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER == vector_filter) {  // vector id array search
    butil::Status status = DoVectorSearchForVectorIdPreFilterDebug(vector_index, vector_with_ids, parameter,
                                                                   region_range, vector_with_distance_results,
                                                                   deserialization_id_time_us, search_time_us);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForVectorIdPreFilterDebug failed");
      return status;
    }
  } else if (dingodb::pb::common::VectorFilter::SCALAR_FILTER == vector_filter &&
             dingodb::pb::common::VectorFilterType::QUERY_PRE == vector_filter_type) {  // scalar pre filter search

    butil::Status status =
        DoVectorSearchForScalarPreFilterDebug(vector_index, region_range, vector_with_ids, parameter,
                                              vector_with_distance_results, scan_scalar_time_us, search_time_us);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForScalarPreFilterDebug failed ");
      return status;
    }
  } else if (dingodb::pb::common::VectorFilter::TABLE_FILTER ==
             vector_filter) {  //  table coprocessor pre filter search. not impl
    butil::Status status = DoVectorSearchForTableCoprocessor(vector_index, region_range, vector_with_ids, parameter,
                                                             vector_with_distance_results);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForTableCoprocessor failed ");
      return status;
    }
  }

  // if vector index does not support restruct vector ,we restruct it using RocksDB
  if (with_vector_data) {
    for (auto& result : vector_with_distance_results) {
      for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
        if (vector_with_distance.vector_with_id().vector().float_values_size() > 0 ||
            vector_with_distance.vector_with_id().vector().binary_values_size() > 0) {
          continue;
        }

        pb::common::VectorWithId vector_with_id;
        auto status = QueryVectorWithId(0, region_range, partition_id, vector_with_distance.vector_with_id().id(), true,
                                        vector_with_id);
        if (!status.ok()) {
          return status;
        }
        vector_with_distance.mutable_vector_with_id()->Swap(&vector_with_id);
      }
    }
  }

  return butil::Status();
}

butil::Status VectorReader::DoVectorSearchForVectorIdPreFilterDebug(  // NOLINT
    VectorIndexWrapperPtr vector_index, const std::vector<pb::common::VectorWithId>& vector_with_ids,
    const pb::common::VectorSearchParameter& parameter, const pb::common::Range& region_range,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results, int64_t& deserialization_id_time_us,
    int64_t& search_time_us) {
  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
  auto start_ids = lambda_time_now_function();
  auto vector_ids = Helper::PbRepeatedToVector(parameter.vector_ids());
  auto status =
      VectorReader::SetVectorIndexIdsFilter(parameter.is_negation(), parameter.is_sorted(), vector_ids, filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  auto end_ids = lambda_time_now_function();
  deserialization_id_time_us = lambda_time_diff_microseconds_function(start_ids, end_ids);

  auto start_search = lambda_time_now_function();

  status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids, parameter,
                                                     vector_with_distance_results, parameter.top_n(), filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  auto end_search = lambda_time_now_function();
  search_time_us = lambda_time_diff_microseconds_function(start_search, end_search);

  return butil::Status::OK();
}

butil::Status VectorReader::DoVectorSearchForScalarPreFilterDebug(
    VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results, int64_t& scan_scalar_time_us,
    int64_t& search_time_us) {
  // scalar pre filter search
  butil::Status status;
#if !defined(ENABLE_SCALAR_WITH_COPROCESSOR)
  const auto& std_vector_scalar = vector_with_ids[0].scalar_data();
  auto lambda_scalar_compare_function =
      [&std_vector_scalar](const pb::common::VectorScalardata& internal_vector_scalar) {
        for (const auto& [key, value] : std_vector_scalar.scalar_data()) {
          auto it = internal_vector_scalar.scalar_data().find(key);
          if (it == internal_vector_scalar.scalar_data().end()) {
            return false;
          }

          bool compare_result = Helper::IsEqualVectorScalarValue(value, it->second);
          if (!compare_result) {
            return false;
          }
        }
        return true;
      };

#else   // ENABLE_SCALAR_WITH_COPROCESSOR
  if (!parameter.has_vector_coprocessor()) {
    std::string s = fmt::format("vector_coprocessor empty not support");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  const auto& coprocessor = parameter.vector_coprocessor();

  std::shared_ptr<RawCoprocessor> scalar_coprocessor =
      std::make_shared<CoprocessorScalar>(Helper::GetKeyPrefix(region_range.start_key()));
  status = scalar_coprocessor->Open(CoprocessorPbWrapper{coprocessor});
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "scalar coprocessor::Open failed " << status.error_cstr();
    return status;
  }

  auto lambda_scalar_compare_with_coprocessor_function =
      [&scalar_coprocessor](const pb::common::VectorScalardata& internal_vector_scalar) {
        bool is_reverse = false;
        butil::Status status = scalar_coprocessor->Filter(internal_vector_scalar, is_reverse);
        if (!status.ok()) {
          LOG(WARNING) << "scalar coprocessor filter failed, error: " << status.error_cstr();
          return is_reverse;
        }
        return is_reverse;
      };
#endif  // #if !defined( ENABLE_SCALAR_WITH_COPROCESSOR)

  auto encode_range = mvcc::Codec::EncodeRange(region_range);

  IteratorOptions options;
  options.upper_bound = encode_range.end_key();

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  auto start_iter = lambda_time_now_function();
  auto iter = reader_->NewIterator(Constant::kVectorScalarCF, 0, options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range {}", Helper::RangeToString(region_range));
    return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
  }

  std::vector<int64_t> vector_ids;
  vector_ids.reserve(1024);
  for (iter->Seek(encode_range.start_key()); iter->Valid(); iter->Next()) {
    pb::common::VectorScalardata internal_vector_scalar;
    std::string value(mvcc::Codec::UnPackageValue(iter->Value()));
    CHECK(internal_vector_scalar.ParseFromString(value)) << "Parse vector scalar data error.";

#if !defined(ENABLE_SCALAR_WITH_COPROCESSOR)
    bool compare_result = lambda_scalar_compare_function(internal_vector_scalar);
#else
    bool compare_result = lambda_scalar_compare_with_coprocessor_function(internal_vector_scalar);
#endif
    if (compare_result) {
      std::string key(iter->Key());
      int64_t internal_vector_id = VectorCodec::DecodeVectorIdFromEncodeKeyWithTs(key);
      CHECK(internal_vector_id > 0) << fmt::format("decode vector id failed key: {}", Helper::StringToHex(key));
      vector_ids.push_back(internal_vector_id);
    }
  }
  auto end_iter = lambda_time_now_function();
  scan_scalar_time_us = lambda_time_diff_microseconds_function(start_iter, end_iter);

  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;

  status = VectorReader::SetVectorIndexIdsFilter(false, false, vector_ids, filters);
  if (!status.ok()) {
    return status;
  }

  auto start_search = lambda_time_now_function();

  status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids, parameter,
                                                     vector_with_distance_results, parameter.top_n(), filters);
  if (!status.ok()) {
    return status;
  }

  auto end_search = lambda_time_now_function();
  search_time_us = lambda_time_diff_microseconds_function(start_search, end_search);
  return butil::Status::OK();
}

butil::Status VectorReader::SetVectorIndexIdsFilter(bool is_negation, bool is_sorted, std::vector<int64_t>& vector_ids,
                                                    std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters) {
  if (!is_sorted) {
    std::sort(vector_ids.begin(), vector_ids.end());
  }
  filters.push_back(std::make_shared<VectorIndex::SortFilterFunctor>(vector_ids, is_negation));
  return butil::Status::OK();
}

butil::Status VectorReader::SearchAndRangeSearchWrapper(
    VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results, uint32_t topk,
    std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters) {
  bool with_vector_data = !(parameter.without_vector_data());
  bool enable_range_search = parameter.enable_range_search();
  float radius = parameter.radius();
  butil::Status status;

  // if vector index does not support restruct vector ,we restruct it using RocksDB
  // if use_brute_force is true, we use brute force search, else we call vector index search, if vector index not
  // support, then use brute force search again to get result
  if (parameter.use_brute_force()) {
    if (enable_range_search) {
      status = BruteForceRangeSearch(vector_index, vector_with_ids, radius, region_range, filters, with_vector_data,
                                     parameter, vector_with_distance_results);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("RangeSearch vector index failed, error: {} {}", status.error_code(),
                                        status.error_str());
        return status;
      }
    } else {
      status = BruteForceSearch(vector_index, vector_with_ids, topk, region_range, filters, with_vector_data, parameter,
                                vector_with_distance_results);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Search vector index failed, error: {} {}", status.error_code(),
                                        status.error_str());
        return status;
      }
    }
  } else {
    if (enable_range_search) {
      status = vector_index->RangeSearch(vector_with_ids, radius, region_range, filters, with_vector_data, parameter,
                                         vector_with_distance_results);
      if (status.error_code() == pb::error::Errno::EVECTOR_NOT_SUPPORT) {
        DINGO_LOG(INFO) << "RangeSearch vector index not support, try brute force, id: " << vector_index->Id();
        return BruteForceRangeSearch(vector_index, vector_with_ids, radius, region_range, filters, with_vector_data,
                                     parameter, vector_with_distance_results);
      } else if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("RangeSearch vector index failed, error: {} {}", status.error_code(),
                                        status.error_str());
        return status;
      }
    } else {
      status = vector_index->Search(vector_with_ids, topk, region_range, filters, with_vector_data, parameter,
                                    vector_with_distance_results);
      if (status.error_code() == pb::error::Errno::EVECTOR_NOT_SUPPORT) {
        DINGO_LOG(DEBUG) << "Search vector index not support, try brute force, id: " << vector_index->Id();
        return BruteForceSearch(vector_index, vector_with_ids, topk, region_range, filters, with_vector_data, parameter,
                                vector_with_distance_results);
      } else if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Search vector index failed, error: {} {}", status.error_code(),
                                        status.error_str());
        return status;
      }
    }
  }

  return butil::Status::OK();
}

// DistanceResult
// This class is used for priority queue to merge the search result from many batch scan data from raw engine.
class DistanceResult {
 public:
  float distance{0};
  pb::common::VectorWithDistance vector_with_distance{};
  DistanceResult() = default;
  DistanceResult(float distance, pb::common::VectorWithDistance vector_with_distance)
      : distance(distance), vector_with_distance(vector_with_distance) {}
};

// Overload the < operator.
bool operator<(const DistanceResult& result1, const DistanceResult& result2) {
  if (result1.distance != result2.distance) {
    return result1.distance < result2.distance;
  } else {
    return result1.vector_with_distance.vector_with_id().id() < result2.vector_with_distance.vector_with_id().id();
  }
}

// Overload the > operator.
bool operator>(const DistanceResult& result1, const DistanceResult& result2) {
  if (result1.distance != result2.distance) {
    return result1.distance > result2.distance;
  } else {
    return result1.vector_with_distance.vector_with_id().id() > result2.vector_with_distance.vector_with_id().id();
  }
}

// ScanData from raw engine, build vector index and search
butil::Status VectorReader::BruteForceSearch(VectorIndexWrapperPtr vector_index,
                                             const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                             uint32_t topk, const pb::common::Range& region_range,
                                             std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,
                                             bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                             std::vector<pb::index::VectorWithDistanceResult>& results) {
  auto metric_type = vector_index->GetMetricType();
  auto dimension = vector_index->GetDimension();

  auto encode_range = mvcc::Codec::EncodeRange(region_range);

  pb::common::RegionEpoch epoch;
  pb::common::VectorIndexParameter index_parameter;
  index_parameter.mutable_flat_parameter()->set_dimension(dimension);
  index_parameter.mutable_flat_parameter()->set_metric_type(metric_type);

  IteratorOptions options;
  options.upper_bound = encode_range.end_key();
  auto iter = reader_->NewIterator(Constant::kVectorDataCF, 0, options);

  iter->Seek(encode_range.start_key());
  if (!iter->Valid()) {
    return butil::Status();
  }

  BvarLatencyGuard bvar_guard(&g_bruteforce_search_latency);

  // topk results
  std::vector<std::priority_queue<DistanceResult>> top_results;
  top_results.resize(vector_with_ids.size());

  int64_t count = 0;
  std::vector<pb::common::VectorWithId> vector_with_id_batch;
  std::vector<pb::index::VectorWithDistanceResult> results_batch;

  // scan data from raw engine
  while (iter->Valid()) {
    std::string key(iter->Key());
    auto vector_id = VectorCodec::DecodeVectorIdFromEncodeKeyWithTs(key);
    CHECK(vector_id > 0) << fmt::format("vector_id({}) is invaild", vector_id);

    std::string value(mvcc::Codec::UnPackageValue(iter->Value()));

    pb::common::Vector vector;
    CHECK(vector.ParseFromString(value)) << "Parse vector proto error";

    pb::common::VectorWithId vector_with_id;
    vector_with_id.mutable_vector()->Swap(&vector);
    vector_with_id.set_id(vector_id);

    vector_with_id_batch.push_back(vector_with_id);

    if (vector_with_id_batch.size() == FLAGS_vector_index_bruteforce_batch_count) {
      auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
      auto flat_index = VectorIndexFactory::NewFlat(INT64_MAX, index_parameter, epoch, region_range, thread_pool);
      CHECK(flat_index != nullptr) << "flat_index is nullptr";

      auto ret = flat_index->AddByParallel(vector_with_id_batch);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Add vector to flat index failed, error: {} {}", ret.error_code(),
                                        ret.error_str());
        return ret;
      }

      ret = flat_index->SearchByParallel(vector_with_ids, topk, filters, reconstruct, parameter, results_batch);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Search vector index failed, error: {} {}", ret.error_code(), ret.error_str());
        return ret;
      }

      CHECK(results_batch.size() == vector_with_ids.size());

      for (int i = 0; i < results_batch.size(); i++) {
        auto& result = results_batch[i];
        auto& top_result = top_results[i];

        for (const auto& vector_with_distance : result.vector_with_distances()) {
          auto& top_result = top_results[i];
          if (top_result.size() < topk) {
            top_result.emplace(vector_with_distance.distance(), vector_with_distance);
          } else {
            if (top_result.top().distance > vector_with_distance.distance()) {
              top_result.pop();
              top_result.emplace(vector_with_distance.distance(), vector_with_distance);
            }
          }
        }
      }

      results_batch.clear();
      vector_with_id_batch.clear();
    }

    iter->Next();
  }

  if (!vector_with_id_batch.empty()) {
    auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
    auto flat_index = VectorIndexFactory::NewFlat(INT64_MAX, index_parameter, epoch, region_range, thread_pool);
    CHECK(flat_index != nullptr) << "flat_index is nullptr";

    auto ret = flat_index->AddByParallel(vector_with_id_batch);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Add vector to flat index failed, error: {} {}", ret.error_code(),
                                      ret.error_str());
      return ret;
    }

    ret = flat_index->SearchByParallel(vector_with_ids, topk, filters, reconstruct, parameter, results_batch);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Search vector index failed, error: {} {}", ret.error_code(), ret.error_str());
      return ret;
    }

    CHECK(results_batch.size() == vector_with_ids.size());

    for (int i = 0; i < results_batch.size(); i++) {
      auto& result = results_batch[i];
      auto& top_result = top_results[i];

      for (const auto& vector_with_distance : result.vector_with_distances()) {
        auto& top_result = top_results[i];
        if (top_result.size() < topk) {
          top_result.emplace(vector_with_distance.distance(), vector_with_distance);
        } else {
          if (top_result.top().distance > vector_with_distance.distance()) {
            top_result.pop();
            top_result.emplace(vector_with_distance.distance(), vector_with_distance);
          }
        }
      }
    }

    results_batch.clear();
    vector_with_id_batch.clear();
  }

  // copy top_results to results
  // we don't do sorting by distance here
  // the client will do sorting by distance
  results.resize(top_results.size());

  for (int i = 0; i < top_results.size(); i++) {
    auto& top_result = top_results[i];
    auto& result = results[i];

    std::deque<pb::common::VectorWithDistance> vector_with_distances_deque;

    while (!top_result.empty()) {
      auto top = top_result.top();
      vector_with_distances_deque.emplace_front(top.vector_with_distance);
      top_result.pop();
    }

    while (!vector_with_distances_deque.empty()) {
      auto& vector_with_distance = *result.add_vector_with_distances();
      vector_with_distance.Swap(&vector_with_distances_deque.front());
      vector_with_distances_deque.pop_front();
    }
  }

  return butil::Status::OK();
}

butil::Status VectorReader::BruteForceRangeSearch(VectorIndexWrapperPtr vector_index,
                                                  const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                  float radius, const pb::common::Range& region_range,
                                                  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters,
                                                  bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                                  std::vector<pb::index::VectorWithDistanceResult>& results) {
  auto metric_type = vector_index->GetMetricType();
  auto dimension = vector_index->GetDimension();

  auto encode_range = mvcc::Codec::EncodeRange(region_range);

  pb::common::RegionEpoch epoch;
  pb::common::VectorIndexParameter index_parameter;
  index_parameter.mutable_flat_parameter()->set_dimension(dimension);
  index_parameter.mutable_flat_parameter()->set_metric_type(metric_type);

  IteratorOptions options;
  options.upper_bound = encode_range.end_key();
  auto iter = reader_->NewIterator(Constant::kVectorDataCF, 0, options);

  iter->Seek(encode_range.start_key());
  if (!iter->Valid()) {
    return butil::Status();
  }

  BvarLatencyGuard bvar_guard(&g_bruteforce_range_search_latency);

  // range search results
  std::vector<std::vector<std::pair<float, pb::common::VectorWithDistance>>> range_rsults;
  range_rsults.resize(vector_with_ids.size());

  int64_t count = 0;
  std::vector<pb::common::VectorWithId> vector_with_id_batch;
  std::vector<pb::index::VectorWithDistanceResult> results_batch;

  // scan data from raw engine
  while (iter->Valid()) {
    std::string key(iter->Key());
    auto vector_id = VectorCodec::DecodeVectorIdFromEncodeKeyWithTs(key);
    CHECK(vector_id > 0) << fmt::format("vector_id({}) is invaild", vector_id);

    std::string value(mvcc::Codec::UnPackageValue(iter->Value()));

    pb::common::Vector vector;
    CHECK(vector.ParseFromString(value)) << "Parse vector proto error.";

    pb::common::VectorWithId vector_with_id;
    vector_with_id.mutable_vector()->Swap(&vector);
    vector_with_id.set_id(vector_id);

    vector_with_id_batch.push_back(vector_with_id);

    if (vector_with_id_batch.size() == FLAGS_vector_index_bruteforce_batch_count) {
      auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
      auto flat_index = VectorIndexFactory::NewFlat(INT64_MAX, index_parameter, epoch, region_range, thread_pool);
      CHECK(flat_index != nullptr) << "flat_index is nullptr";

      auto ret = flat_index->AddByParallel(vector_with_id_batch);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Add vector to flat index failed, error: {} {}", ret.error_code(),
                                        ret.error_str());
        return ret;
      }

      ret = flat_index->RangeSearchByParallel(vector_with_ids, radius, filters, reconstruct, parameter, results_batch);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << fmt::format("RangeSearch vector index failed, error: {} {}", ret.error_code(),
                                        ret.error_str());
        return ret;
      }

      CHECK(results_batch.size() == vector_with_ids.size());

      for (int i = 0; i < results_batch.size(); i++) {
        auto& result = results_batch[i];
        auto& top_result = range_rsults[i];

        for (const auto& vector_with_distance : result.vector_with_distances()) {
          auto& top_result = range_rsults[i];
          if (top_result.size() < FLAGS_vector_index_max_range_search_result_count) {
            top_result.emplace_back(vector_with_distance.distance(), vector_with_distance);
          } else {
            DINGO_LOG(WARNING) << fmt::format("RangeSearch result count exceed limit, limit: {}, actual: {}",
                                              FLAGS_vector_index_max_range_search_result_count, top_result.size() + 1);
            break;
          }
        }
      }

      results_batch.clear();
      vector_with_id_batch.clear();
    }

    iter->Next();
  }

  if (!vector_with_id_batch.empty()) {
    auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
    auto flat_index = VectorIndexFactory::NewFlat(INT64_MAX, index_parameter, epoch, region_range, thread_pool);
    CHECK(flat_index != nullptr) << "flat_index is nullptr";

    auto ret = flat_index->AddByParallel(vector_with_id_batch);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Add vector to flat index failed, error: {} {}", ret.error_code(),
                                      ret.error_str());
      return ret;
    }

    ret = flat_index->RangeSearchByParallel(vector_with_ids, radius, filters, reconstruct, parameter, results_batch);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("RangeSearch vector index failed, error: {} {}", ret.error_code(),
                                      ret.error_str());
      return ret;
    }

    CHECK(results_batch.size() == vector_with_ids.size());

    for (int i = 0; i < results_batch.size(); i++) {
      auto& result = results_batch[i];
      auto& top_result = range_rsults[i];

      for (const auto& vector_with_distance : result.vector_with_distances()) {
        auto& top_result = range_rsults[i];
        if (top_result.size() < FLAGS_vector_index_max_range_search_result_count) {
          top_result.emplace_back(vector_with_distance.distance(), vector_with_distance);
        } else {
          DINGO_LOG(WARNING) << fmt::format("RangeSearch result count exceed limit, limit: {}, actual: {}",
                                            FLAGS_vector_index_max_range_search_result_count, top_result.size() + 1);
          break;
        }
      }
    }

    results_batch.clear();
    vector_with_id_batch.clear();
  }

  // copy top_results to results
  // we don't do sorting by distance here
  // the client will do sorting by distance
  results.resize(range_rsults.size());
  for (int i = 0; i < range_rsults.size(); i++) {
    auto& top_result = range_rsults[i];
    auto& result = results[i];

    for (auto& top : top_result) {
      auto& vector_with_distance = *result.add_vector_with_distances();
      vector_with_distance.Swap(&top.second);
    }
  }

  return butil::Status::OK();
}

}  // namespace dingodb
