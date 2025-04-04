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

#include "split/split_checker.h"

#include <sys/stat.h>

#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <string_view>
#include <vector>

#include "common/constant.h"
#include "common/helper.h"
#include "config/config_helper.h"
#include "engine/iterator.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"
#include "server/service_helper.h"
#include "vector/vector_index_manager.h"

namespace dingodb {
DECLARE_bool(enable_region_split_and_merge_for_lite);
DECLARE_bool(region_enable_auto_split);

MergedIterator::MergedIterator(RawEnginePtr raw_engine, const std::vector<std::string>& cf_names,
                               const std::string& end_key)
    : raw_engine_(raw_engine) {
  auto snapshot = raw_engine->GetSnapshot();

  for (const auto& cf_name : cf_names) {
    IteratorOptions options;
    options.upper_bound = end_key;
    auto iter = raw_engine->Reader()->NewIterator(cf_name, snapshot, options);
    iters_.push_back(iter);
  }
}

void MergedIterator::Seek(const std::string& target) {
  for (int i = 0; i < iters_.size(); ++i) {
    auto iter = iters_[i];
    iter->Seek(target);
    Next(iter, i);
  }
}

bool MergedIterator::Valid() { return !min_heap_.empty(); }

void MergedIterator::Next() {
  if (min_heap_.empty()) {
    return;
  }

  const Entry entry = min_heap_.top();
  min_heap_.pop();
  auto iter = iters_[entry.iter_pos];
  Next(iter, entry.iter_pos);
}

std::string_view MergedIterator::Key() {
  const Entry& entry = min_heap_.top();
  return entry.key;
}

uint32_t MergedIterator::KeyValueSize() {
  const Entry& entry = min_heap_.top();
  return entry.key.size() + entry.value_size;
}

void MergedIterator::Next(IteratorPtr iter, int iter_pos) {
  if (iter->Valid()) {
    Entry entry;
    entry.iter_pos = iter_pos;
    entry.key = iter->Key();
    entry.value_size = iter->Value().size();
    min_heap_.push(entry);
    iter->Next();
  }
}

// base physics key, contain key of multi version.
std::string HalfSplitChecker::SplitKey(store::RegionPtr region, const pb::common::Range& range,
                                       const std::vector<std::string>& cf_names, uint32_t& count, int64_t& size) {
  MergedIterator iter(raw_engine_, cf_names, range.end_key());
  iter.Seek(range.start_key());

  int64_t chunk_size = 0;
  std::string prev_key;
  std::vector<std::string> keys;
  bool is_split = false;
  for (; iter.Valid(); iter.Next()) {
    int64_t key_value_size = iter.KeyValueSize();
    size += key_value_size;
    chunk_size += key_value_size;
    if (chunk_size >= split_chunk_size_) {
      chunk_size = 0;
      keys.push_back(std::string(iter.Key()));
    }
    if (size >= split_threshold_size_) {
      is_split = true;
    }
    if (prev_key != iter.Key()) {
      prev_key = iter.Key();
      ++count;
    }
  }

  int mid = keys.size() / 2;
  std::string split_key = keys.empty() ? "" : keys[mid];

  DINGO_LOG(INFO) << fmt::format(
      "[split.check][region({})] policy(HALF) split_threshold_size({}) split_chunk_size({}) actual_size({}) count({})",
      region->Id(), split_threshold_size_, split_chunk_size_, size, count);

  return is_split ? split_key : "";
}

// base physics key, contain key of multi version.
std::string SizeSplitChecker::SplitKey(store::RegionPtr region, const pb::common::Range& range,
                                       const std::vector<std::string>& cf_names, uint32_t& count, int64_t& size) {
  MergedIterator iter(raw_engine_, cf_names, range.end_key());
  iter.Seek(range.start_key());

  std::string prev_key;
  std::string split_key;
  bool is_split = false;
  int64_t split_pos = split_size_ * split_ratio_;
  for (; iter.Valid(); iter.Next()) {
    size += iter.KeyValueSize();
    if (split_key.empty() && size >= split_pos) {
      split_key = iter.Key();
    } else if (size >= split_size_) {
      is_split = true;
    }

    if (prev_key != iter.Key()) {
      prev_key = iter.Key();
      ++count;
    }
  }

  DINGO_LOG(INFO) << fmt::format(
      "[split.check][region({})] policy(SIZE) split_size({}) split_ratio({}) actual_size({}) count({})", region->Id(),
      split_size_, split_ratio_, size, count);

  return is_split ? split_key : "";
}

// base logic key, ignore key of multi version.
std::string KeysSplitChecker::SplitKey(store::RegionPtr region, const pb::common::Range& range,
                                       const std::vector<std::string>& cf_names, uint32_t& count, int64_t& size) {
  MergedIterator iter(raw_engine_, cf_names, range.end_key());
  iter.Seek(range.start_key());

  int64_t split_key_count = 0;
  std::string prev_key;
  std::string split_key;
  bool is_split = false;
  uint32_t split_key_number = split_keys_number_ * split_keys_ratio_;
  for (; iter.Valid(); iter.Next()) {
    if (prev_key != iter.Key()) {
      prev_key = iter.Key();
      ++split_key_count;
      ++count;
    }
    size += iter.KeyValueSize();

    if (split_key.empty() && split_key_count >= split_key_number) {
      split_key = iter.Key();
    } else if (split_key_count == split_keys_number_) {
      is_split = true;
    }
  }

  DINGO_LOG(INFO) << fmt::format(
      "[split.check][region({})] policy(KEYS) split_key_number({}) split_key_ratio({}) actual_size({}) count({})",
      region->Id(), split_keys_number_, split_keys_ratio_, size, count);

  return is_split ? split_key : "";
}

static bool CheckLeaderAndFollowerStatus(int64_t region_id) {
  auto region = Server::GetInstance().GetRegion(region_id);
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[split.check][region({})] Not found region.", region_id);
    return false;
  }
  if (region->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE) {
    return true;
  } else if (region->GetStoreEngineType() == pb::common::STORE_ENG_MEMORY) {
    DINGO_LOG(ERROR) << fmt::format("[split.check][region({})] Not support memory engine.", region_id);
    return false;
  }

  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[split.check][region({})] get engine failed.", region_id);
    return false;
  }

  auto node = raft_store_engine->GetNode(region_id);
  if (node == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[split.check][region({})] get raft node failed.", region_id);
    return false;
  }

  if (!node->IsLeader()) {
    return false;
  }

  auto status = Helper::ValidateRaftStatusForSplitMerge(node->GetStatus());
  if (!status.ok()) {
    DINGO_LOG(INFO) << fmt::format("[split.check][region({})] {}", region_id, status.error_str());
    return false;
  }

  return true;
}

void SplitCheckTask::SplitCheck() {
  if (region_ == nullptr) {
    return;
  }
  if (split_checker_ == nullptr) {
    return;
  }

  int64_t start_time = Helper::TimestampMs();
  auto epoch = region_->Epoch();
  auto plain_range = region_->Range(false);
  auto encode_range = mvcc::Codec::EncodeRange(plain_range);

  std::vector<std::string> raw_cf_names;
  std::vector<std::string> txn_cf_names;
  Helper::GetColumnFamilyNames(encode_range.start_key(), raw_cf_names, txn_cf_names);

  std::vector<std::string> cf_names = txn_cf_names.empty() ? raw_cf_names : txn_cf_names;

  // Get split key.
  // for txn region, we need to translate the user key to padding key.
  // for raw region, we use the user key directly.
  // todo: transform range?
  DINGO_LOG(INFO) << fmt::format("[split.check][region({})] Will check SplitKey for raw_range{} cf_names({})",
                                 region_->Id(), Helper::RangeToString(plain_range), Helper::VectorToString(cf_names));
  uint32_t key_count = 0;
  int64_t size = 0;
  std::string encode_split_key = split_checker_->SplitKey(region_, encode_range, cf_names, key_count, size);

  int64_t ts = 0;
  std::string plain_split_key;
  if (!encode_split_key.empty() && !mvcc::Codec::DecodeKey(encode_split_key, plain_split_key, ts)) {
    DINGO_LOG(FATAL) << fmt::format("Decode key fail, key: {}", Helper::StringToHex(encode_split_key));
  }

  // Update region key count metrics.
  if (region_metrics_ != nullptr) {
    if (key_count > 0) {
      region_metrics_->SetKeyCount(key_count);
      region_metrics_->SetNeedUpdateKeyCount(false);
    }
    if (size > 0) {
      region_metrics_->SetRegionSize(size);
    }
  }

  bool need_split = true;
  std::string reason;
  do {
    if (plain_split_key.empty()) {
      reason = "split key is empty";
      need_split = false;
      break;
    }
    if (region_->Epoch().version() != epoch.version()) {
      reason = "region version change";
      need_split = false;
      break;
    }
    if (!region_->CheckKeyInRange(plain_split_key)) {
      reason = fmt::format("invalid split key, not in region range {}", Helper::RangeToString(plain_range));
      need_split = false;
      break;
    }
    if (region_->DisableChange()) {
      reason = "region disable split";
      need_split = false;
      break;
    }
    if (region_->TemporaryDisableChange()) {
      reason = "region temporary disable split";
      need_split = false;
      break;
    }
    if (region_->State() != pb::common::NORMAL) {
      reason = fmt::format("region state is {}, not normal", pb::common::StoreRegionState_Name(region_->State()));
      need_split = false;
      break;
    }
    if (!CheckLeaderAndFollowerStatus(region_->Id())) {
      reason = "not leader or follower abnormal";
      need_split = false;
      break;
    }
    if (region_->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE &&
        !FLAGS_enable_region_split_and_merge_for_lite) {
      reason = fmt::format(" mono_store region {} disable split ", region_->Id());
      need_split = false;
      break;
    }
    int runing_num = VectorIndexManager::GetVectorIndexTaskRunningNum();
    if (runing_num > Constant::kVectorIndexTaskRunningNumExpectValue) {
      reason = fmt::format("runing vector index task num({}) too many, exceed expect num({})", runing_num,
                           Constant::kVectorIndexTaskRunningNumExpectValue);
      need_split = false;
      break;
    }
  } while (false);

  DINGO_LOG(INFO) << fmt::format(
      "[split.check][region({})] split check result({}) reason({}) split_policy({}) split_key({}) epoch({}-{}) "
      "range({}) elapsed time({}ms)",
      region_->Id(), need_split, reason, split_checker_->GetPolicyName(), Helper::StringToHex(plain_split_key),
      region_->Epoch().conf_version(), region_->Epoch().version(), Helper::RangeToString(plain_range),
      Helper::TimestampMs() - start_time);
  if (!need_split) {
    return;
  }

  // Invoke coordinator SplitRegion api.
  auto coordinator_interaction = Server::GetInstance().GetCoordinatorInteraction();
  pb::coordinator::SplitRegionRequest request;
  request.mutable_split_request()->set_split_from_region_id(region_->Id());
  request.mutable_split_request()->set_split_watershed_key(plain_split_key);
  request.mutable_split_request()->set_store_create_region(ConfigHelper::GetSplitStrategy() ==
                                                           pb::raft::POST_CREATE_REGION);
  pb::coordinator::SplitRegionResponse response;
  auto status = coordinator_interaction->SendRequest("SplitRegion", request, response);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[split.check][region({})] send SplitRegion failed, error: {}", region_->Id(),
                                      Helper::PrintStatus(status));
    return;
  }
}

bool SplitCheckWorkers::Init(uint32_t num) {
  for (int i = 0; i < num; ++i) {
    auto worker = Worker::New();
    if (!worker->Init()) {
      return false;
    }
    workers_.push_back(worker);
  }

  return true;
}

void SplitCheckWorkers::Destroy() {
  for (auto& worker : workers_) {
    worker->Destroy();
  }
}

bool SplitCheckWorkers::Execute(TaskRunnablePtr task) {
  if (!workers_[offset_]->Execute(task)) {
    return false;
  }
  offset_ = (offset_ + 1) % workers_.size();

  return true;
}

bool SplitCheckWorkers::IsExistRegionChecking(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  return checking_regions_.find(region_id) != checking_regions_.end();
}

void SplitCheckWorkers::AddRegionChecking(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (checking_regions_.find(region_id) == checking_regions_.end()) {
    checking_regions_.insert(region_id);
  }
}

void SplitCheckWorkers::DeleteRegionChecking(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  checking_regions_.erase(region_id);
}

static std::shared_ptr<SplitChecker> BuildSplitChecker(std::shared_ptr<RawEngine> raw_engine) {
  std::string policy = ConfigHelper::GetSplitPolicy();
  if (policy == "HALF") {
    int64_t split_threshold_size = ConfigHelper::GetRegionMaxSize();
    uint32_t split_chunk_size = ConfigHelper::GetSplitChunkSize();
    return std::make_shared<HalfSplitChecker>(raw_engine, split_threshold_size, split_chunk_size);

  } else if (policy == "SIZE") {
    int64_t split_threshold_size = ConfigHelper::GetRegionMaxSize();
    float split_ratio = ConfigHelper::GetSplitSizeRatio();
    return std::make_shared<SizeSplitChecker>(raw_engine, split_threshold_size, split_ratio);

  } else if (policy == "KEYS") {
    uint32_t split_key_number = ConfigHelper::GetSplitKeysNumber();
    float split_keys_ratio = ConfigHelper::GetSplitKeysRatio();
    return std::make_shared<KeysSplitChecker>(raw_engine, split_key_number, split_keys_ratio);
  }

  DINGO_LOG(ERROR) << fmt::format("[split.check] build split checker failed, policy {}", policy);

  return nullptr;
}

void PreSplitCheckTask::PreSplitCheck() {
  // if system capacity is very low, suspend all split check to avoid split region.
  auto ret = ServiceHelper::ValidateClusterReadOnly();
  if (!ret.ok()) {
    DINGO_LOG(INFO) << fmt::format("[split.check] cluster is read-only, suspend all split check, error: {} {}",
                                   pb::error::Errno_Name(ret.error_code()), ret.error_str());
    return;
  }

  auto metrics = Server::GetInstance().GetStoreMetricsManager()->GetStoreRegionMetrics();
  auto regions = GET_STORE_REGION_META->GetAllAliveRegion();
  int64_t split_check_approximate_size = ConfigHelper::GetSplitCheckApproximateSize();
  for (auto& region : regions) {
    if (!region->IsSupportSplitAndMerge()) {
      continue;
    }

    auto region_metric = metrics->GetMetrics(region->Id());
    bool need_scan_check = true;
    std::string reason;
    do {
      if (region_metric == nullptr) {
        need_scan_check = false;
        reason = "region metric is nullptr";
        break;
      }
      if (split_check_workers_ == nullptr) {
        need_scan_check = false;
        reason = "split check worker is nullptr";
        break;
      }
      if (region->State() != pb::common::NORMAL) {
        need_scan_check = false;
        reason = "region state is not normal";
        break;
      }
      if (region->DisableChange()) {
        need_scan_check = false;
        reason = "region is disable split";
        break;
      }
      if (region->TemporaryDisableChange()) {
        need_scan_check = false;
        reason = "region is temporary disable change";
        break;
      }
      if (split_check_workers_->IsExistRegionChecking(region->Id())) {
        need_scan_check = false;
        reason = "region already exist split check";
        break;
      }
      if (!CheckLeaderAndFollowerStatus(region->Id())) {
        need_scan_check = false;
        reason = "not leader or follower abnormal";
        break;
      }
      if (region_metric->InnerRegionMetrics().region_size() < split_check_approximate_size) {
        need_scan_check = false;
        reason = "region approximate size too small";
        break;
      }
      int runing_num = VectorIndexManager::GetVectorIndexTaskRunningNum();
      if (runing_num > Constant::kVectorIndexTaskRunningNumExpectValue) {
        need_scan_check = false;
        reason = fmt::format("runing vector index task num({}) too many, exceed expect num({})", runing_num,
                             Constant::kVectorIndexTaskRunningNumExpectValue);
        break;
      }
    } while (false);

    DINGO_LOG(INFO) << fmt::format(
        "[split.check][region({})] presplit check result({}) reason({}) approximate size({}/{})", region->Id(),
        need_scan_check, reason, region_metric == nullptr ? 0 : region_metric->InnerRegionMetrics().region_size(),
        split_check_approximate_size);
    if (!need_scan_check) {
      continue;
    }

    auto raw_engine = Server::GetInstance().GetRawEngine(region->GetRawEngineType());
    if (raw_engine == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("[split.check][region({})] get raw_engine failed, skip this region.",
                                      region->Id());
      continue;
    }

    auto split_checker = BuildSplitChecker(raw_engine);
    if (split_checker == nullptr) {
      continue;
    }

    auto task = std::make_shared<SplitCheckTask>(split_check_workers_, region, region_metric, split_checker);
    if (split_check_workers_->Execute(task)) {
      split_check_workers_->AddRegionChecking(region->Id());
    }
  }
}

bool PreSplitChecker::Init(int num) {
  if (!worker_->Init()) {
    return false;
  }

  if (!split_check_workers_->Init(num)) {
    return false;
  }

  return true;
}

void PreSplitChecker::Destroy() {
  worker_->Destroy();
  split_check_workers_->Destroy();
}

bool PreSplitChecker::Execute(TaskRunnablePtr task) { return worker_->Execute(task); }

void PreSplitChecker::TriggerPreSplitCheck(void*) {
  if (!FLAGS_region_enable_auto_split) {
    DINGO_LOG(INFO) << "disable auto split";
    return;
  }
  // Free at ExecuteRoutine()
  auto task = std::make_shared<PreSplitCheckTask>(Server::GetInstance().GetPreSplitChecker()->GetSplitCheckWorkers());
  Server::GetInstance().GetPreSplitChecker()->Execute(task);
}

}  // namespace dingodb