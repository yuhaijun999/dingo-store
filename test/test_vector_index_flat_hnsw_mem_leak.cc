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

#include <gtest/gtest.h>
#include <malloc.h>
#include <stdio.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "butil/status.h"
#include "faiss/MetricType.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_factory.h"
#include "vector/vector_index_flat.h"

namespace dingodb {

class VectorIndexFlatHnswMemLeakTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {
    vector_index_flat_for_l2.reset();
    vector_index_flat_for_ip.reset();
    vector_index_flat_for_cosine.reset();
  }

  void SetUp() override {}

  void TearDown() override {}

  inline static std::shared_ptr<VectorIndex> vector_index_flat_for_l2;      // id = 1;
  inline static std::shared_ptr<VectorIndex> vector_index_flat_for_ip;      // id = 2;
  inline static std::shared_ptr<VectorIndex> vector_index_flat_for_cosine;  // id = 3;
  inline static faiss::idx_t dimension = 1024;
  //inline static int data_base_size = 100000;
  inline static int data_base_size = 10000;
  inline static std::vector<float> data_base;
  inline static int64_t id_for_l2 = 1;
  inline static int64_t id_for_ip = 2;
  inline static int64_t id_for_cosine = 3;
  // include this ID
  inline static int64_t vector_id_start = 1;

  // vector_id_end = vector_id_start + data_base_size [Do not include this ID]
  inline static int64_t vector_id_end = vector_id_start + data_base_size;

  inline static int vector_ids_search_size = 10;

  inline static int search_topk = 3;

  ///////////////////////////////hnsw///////////////////////////////////////////////////
  inline static std::shared_ptr<VectorIndex> vector_index_hnsw_for_l2;
  inline static std::shared_ptr<VectorIndex> vector_index_hnsw_for_ip;
  inline static std::shared_ptr<VectorIndex> vector_index_hnsw_for_cosine;
  inline static uint32_t efconstruction = 40;
  inline static uint32_t max_elements = data_base_size;
  inline static int32_t nlinks = 32;
};

TEST_F(VectorIndexFlatHnswMemLeakTest, Flat) {
  static const pb::common::Range kRange;
  // valid param L2
  {
    int64_t id = id_for_l2;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    vector_index_flat_for_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_flat_for_l2.get(), nullptr);
  }

  // valid param IP
  {
    int64_t id = id_for_ip;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    vector_index_flat_for_ip = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_flat_for_ip.get(), nullptr);
  }

  // valid param COSINE
  {
    int64_t id = id_for_cosine;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    index_parameter.mutable_flat_parameter()->set_dimension(dimension);
    index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    vector_index_flat_for_cosine = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_flat_for_cosine.get(), nullptr);
  }

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(dimension * data_base_size, 0.0f);

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dimension; j++) data_base[dimension * i + j] = distrib(rng);
      data_base[dimension * i] += i / 1000.;
      if (i % 1000 == 0) {
        std::cerr << ".";
      }
    }
    std::cerr << "\nCreate Random Data Finish" << '\n';

    // add all data
    {
      std::vector<pb::common::VectorWithId> vector_with_ids;

      for (size_t id = vector_id_start, j = 0; id < vector_id_end; id++, j++) {
        pb::common::VectorWithId vector_with_id;

        vector_with_id.set_id(id);
        for (size_t i = 0; i < dimension; i++) {
          vector_with_id.mutable_vector()->add_float_values(data_base[j * dimension + i]);
        }

        vector_with_ids.push_back(vector_with_id);
      }

      auto ok = vector_index_flat_for_l2->Add(vector_with_ids);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      std::cout << "Add "
                << "L2"
                << " Finished" << std::endl;

      ok = vector_index_flat_for_ip->Add(vector_with_ids);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      std::cout << "Add "
                << "IP"
                << " Finished" << std::endl;

      ok = vector_index_flat_for_cosine->Add(vector_with_ids);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      std::cout << "Add "
                << "cosine"
                << " Finished" << std::endl;
    }

    auto lambda_random_function = []() {
      std::vector<int64_t> vector_ids;
      vector_ids.resize(data_base_size);

      for (size_t i = 0, id = vector_id_start; i < data_base_size; i++, id++) {
        vector_ids[i] = id;
      }

      auto seed = std::chrono::system_clock::now().time_since_epoch().count();
      std::shuffle(vector_ids.begin(), vector_ids.end(), std::default_random_engine(seed));

      std::vector<int64_t> vector_ids_for_search;
      vector_ids_for_search.resize(vector_ids_search_size);
      for (size_t i = 0; i < vector_ids_search_size; i++) {
        vector_ids_for_search[i] = vector_ids[i];
      }

      return std::tuple<std::vector<int64_t>, std::vector<int64_t>>(vector_ids, vector_ids_for_search);
    };

    auto lambda_alg_function = [&lambda_random_function](std::shared_ptr<VectorIndex> vector_index_flat,
                                                         std::string name) {
      butil::Status ok;
      pb::common::VectorWithId vector_with_id;
      vector_with_id.set_id(0);
      vector_with_id.mutable_vector()->set_dimension(dimension);
      vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      for (size_t i = 0; i < dimension; i++) {
        float value = data_base[i];
        vector_with_id.mutable_vector()->add_float_values(value);
      }
      uint32_t topk = search_topk;
      std::vector<pb::index::VectorWithDistanceResult> results;
      std::vector<pb::common::VectorWithId> vector_with_ids;
      vector_with_ids.push_back(vector_with_id);

      auto [vector_ids, vector_ids_for_search] = lambda_random_function();

      auto vector_ids_for_search_copy = vector_ids_for_search;

      std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
      filters.emplace_back(std::make_shared<VectorIndex::FlatListFilterFunctor>(std::move(vector_ids_for_search)));

      ok = vector_index_flat->Search(vector_with_ids, topk, filters, results, false);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      std::cout << "Search " << name << " Finished" << std::endl;
    };

    // l2 ok
    { lambda_alg_function(vector_index_flat_for_l2, "L2"); }

    // ip ok
    { lambda_alg_function(vector_index_flat_for_ip, "IP"); }

    // cosine ok
    { lambda_alg_function(vector_index_flat_for_cosine, "cosine"); }
  }

  // delete
  {
    std::vector<int64_t> delete_ids;
    delete_ids.reserve(data_base_size);
    for (size_t i = 0, id = vector_id_start; i < data_base_size; i++, id++) {
      delete_ids.push_back(id);
    }
    vector_index_flat_for_l2->Delete(delete_ids);
    std::cout << "Delete "
              << "L2"
              << " Finished" << std::endl;
    vector_index_flat_for_ip->Delete(delete_ids);
    std::cout << "Delete "
              << "IP"
              << " Finished" << std::endl;
    vector_index_flat_for_cosine->Delete(delete_ids);
    std::cout << "Delete "
              << "cosine"
              << " Finished" << std::endl;
  }

  vector_index_flat_for_l2 = nullptr;
  vector_index_flat_for_ip = nullptr;
  vector_index_flat_for_cosine = nullptr;
  data_base.resize(0);
  data_base.shrink_to_fit();
  auto is_release = malloc_trim(0);
  if (1 == is_release) {
    std::cout << "memory was actually released back to the system" << std::endl;
  } else {
    std::cout << "it was not possible to release any memory" << std::endl;
  }
  static int s_count = 0;
  std::cout << "Press Any Key to Continue..." << ++s_count << std::endl;
  getchar();
}

TEST_F(VectorIndexFlatHnswMemLeakTest, FlatStop) {
  std::cout << "Press Any Key to Exit..." << std::endl;
  getchar();
}

TEST_F(VectorIndexFlatHnswMemLeakTest, Hnsw) {
  static const pb::common::Range kRange;
  // valid param L2
  {
    int64_t id = id_for_l2;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
    index_parameter.mutable_hnsw_parameter()->set_dimension(dimension);
    index_parameter.mutable_hnsw_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
    index_parameter.mutable_hnsw_parameter()->set_efconstruction(efconstruction);
    index_parameter.mutable_hnsw_parameter()->set_max_elements(max_elements);
    index_parameter.mutable_hnsw_parameter()->set_nlinks(nlinks);

    vector_index_hnsw_for_l2 = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_hnsw_for_l2.get(), nullptr);
  }

  // IP
  {
    int64_t id = id_for_ip;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
    index_parameter.mutable_hnsw_parameter()->set_dimension(dimension);
    index_parameter.mutable_hnsw_parameter()->set_metric_type(
        ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);
    index_parameter.mutable_hnsw_parameter()->set_efconstruction(efconstruction);
    index_parameter.mutable_hnsw_parameter()->set_max_elements(max_elements);
    index_parameter.mutable_hnsw_parameter()->set_nlinks(nlinks);

    vector_index_hnsw_for_ip = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_hnsw_for_ip.get(), nullptr);
  }

  // cosine
  {
    int64_t id = id_for_cosine;
    pb::common::VectorIndexParameter index_parameter;
    index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
    index_parameter.mutable_hnsw_parameter()->set_dimension(dimension);
    index_parameter.mutable_hnsw_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE);
    index_parameter.mutable_hnsw_parameter()->set_efconstruction(efconstruction);
    index_parameter.mutable_hnsw_parameter()->set_max_elements(max_elements);
    index_parameter.mutable_hnsw_parameter()->set_nlinks(nlinks);

    vector_index_hnsw_for_cosine = VectorIndexFactory::New(id, index_parameter, kRange);
    EXPECT_NE(vector_index_hnsw_for_cosine.get(), nullptr);
  }

  butil::Status ok;

  // create random data
  {
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;

    data_base.resize(dimension * data_base_size, 0.0f);
    // float* xb = new float[dimension_ * data_base_size_];

    for (int i = 0; i < data_base_size; i++) {
      for (int j = 0; j < dimension; j++) data_base[dimension * i + j] = distrib(rng);
      data_base[dimension * i] += i / 1000.;
      if (i % 1000 == 0) {
        std::cerr << ".";
      }
    }
    std::cerr << "\nCreate Random Data Finish" << '\n';
  }

  // add all data
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.reserve(data_base_size);

    for (size_t id = vector_id_start, j = 0; id < vector_id_end; id++, j++) {
      pb::common::VectorWithId vector_with_id;

      vector_with_id.set_id(id);
      for (size_t i = 0; i < dimension; i++) {
        vector_with_id.mutable_vector()->add_float_values(data_base[j * dimension + i]);
      }

      vector_with_ids.push_back(vector_with_id);
    }

    std::cout << "Prepare Add param "
              << " Finished" << std::endl;
    ok = vector_index_hnsw_for_l2->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "Add "
              << "L2"
              << " Finished" << std::endl;

    ok = vector_index_hnsw_for_ip->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "Add "
              << "IP"
              << " Finished" << std::endl;

    ok = vector_index_hnsw_for_cosine->Add(vector_with_ids);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "Add "
              << "cosine"
              << " Finished" << std::endl;
  }

  auto lambda_random_function = []() {
    std::vector<int64_t> vector_ids;
    vector_ids.resize(data_base_size);

    for (size_t i = 0, id = vector_id_start; i < data_base_size; i++, id++) {
      vector_ids[i] = id;
    }

    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::shuffle(vector_ids.begin(), vector_ids.end(), std::default_random_engine(seed));

    std::vector<int64_t> vector_ids_for_search;
    vector_ids_for_search.resize(vector_ids_search_size);
    for (size_t i = 0; i < vector_ids_search_size; i++) {
      vector_ids_for_search[i] = vector_ids[i];
    }

    return std::tuple<std::vector<int64_t>, std::vector<int64_t>>(vector_ids, vector_ids_for_search);
  };

  auto lambda_alg_function = [&lambda_random_function](std::shared_ptr<VectorIndex> vector_index_hnsw,
                                                       std::string name) {
    butil::Status ok;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(0);
    vector_with_id.mutable_vector()->set_dimension(dimension);
    vector_with_id.mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    for (size_t i = 0; i < dimension; i++) {
      float value = data_base[i];
      vector_with_id.mutable_vector()->add_float_values(value);
    }
    uint32_t topk = search_topk;
    std::vector<pb::index::VectorWithDistanceResult> results;
    std::vector<pb::common::VectorWithId> vector_with_ids;
    vector_with_ids.push_back(vector_with_id);

    auto [vector_ids, vector_ids_for_search] = lambda_random_function();

    std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
    filters.push_back(std::make_shared<VectorIndex::HnswListFilterFunctor>(vector_ids_for_search));
    ok = vector_index_hnsw->Search(vector_with_ids, topk, filters, results, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "Search " << name << " Finished" << std::endl;
  };

  // l2 ok
  { lambda_alg_function(vector_index_hnsw_for_l2, "L2"); }

  // ip ok
  { lambda_alg_function(vector_index_hnsw_for_ip, "IP"); }

  // cosine ok
  { lambda_alg_function(vector_index_hnsw_for_cosine, "cosine"); }

  // delete
  {
    std::vector<int64_t> delete_ids;
    delete_ids.reserve(data_base_size);
    for (size_t i = 0, id = vector_id_start; i < data_base_size; i++, id++) {
      delete_ids.push_back(id);
    }
    vector_index_hnsw_for_l2->Delete(delete_ids);
    std::cout << "Delete "
              << "L2"
              << " Finished" << std::endl;
    vector_index_hnsw_for_ip->Delete(delete_ids);
    std::cout << "Delete "
              << "IP"
              << " Finished" << std::endl;
    vector_index_hnsw_for_cosine->Delete(delete_ids);
    std::cout << "Delete "
              << "cosine"
              << " Finished" << std::endl;
  }

  vector_index_hnsw_for_l2 = nullptr;
  vector_index_hnsw_for_ip = nullptr;
  vector_index_hnsw_for_cosine = nullptr;
  data_base.resize(0);
  data_base.shrink_to_fit();
  auto is_release = malloc_trim(0);
  if (1 == is_release) {
    std::cout << "memory was actually released back to the system" << std::endl;
  } else {
    std::cout << "it was not possible to release any memory" << std::endl;
  }
  static int s_count = 0;
  std::cout << "Press Any Key to Continue..." << ++s_count << std::endl;
  getchar();
}

TEST_F(VectorIndexFlatHnswMemLeakTest, HnswStop) {
  std::cout << "Press Any Key to Exit..." << std::endl;
  getchar();
}

}  // namespace dingodb
