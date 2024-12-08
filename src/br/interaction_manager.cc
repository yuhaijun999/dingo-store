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

#include "br/interaction_manager.h"

#include <memory>
#include <string>

namespace br {

InteractionManager::InteractionManager() = default;

InteractionManager::~InteractionManager() = default;

InteractionManager& InteractionManager::GetInstance() {
  static InteractionManager instance;
  return instance;
}

void InteractionManager::SetCoordinatorInteraction(ServerInteractionPtr interaction) {
  dingodb::RWLockWriteGuard guard(&rw_lock_);
  coordinator_interaction_ = interaction;
}

void InteractionManager::SetStoreInteraction(ServerInteractionPtr interaction) {
  dingodb::RWLockWriteGuard guard(&rw_lock_);
  store_interaction_ = interaction;
}

void InteractionManager::SetIndexInteraction(ServerInteractionPtr interaction) {
  dingodb::RWLockWriteGuard guard(&rw_lock_);
  index_interaction_ = interaction;
}
void InteractionManager::SetDocumentInteraction(ServerInteractionPtr interaction) {
  dingodb::RWLockWriteGuard guard(&rw_lock_);
  document_interaction_ = interaction;
}

ServerInteractionPtr InteractionManager::GetCoordinatorInteraction() {
  ServerInteractionPtr interaction;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    interaction = coordinator_interaction_;
  }
  return interaction;
}
ServerInteractionPtr InteractionManager::GetStoreInteraction() {
  ServerInteractionPtr interaction;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    interaction = store_interaction_;
  }
  return interaction;
}
ServerInteractionPtr InteractionManager::GetIndexInteraction() {
  ServerInteractionPtr interaction;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    interaction = index_interaction_;
  }
  return interaction;
}
ServerInteractionPtr InteractionManager::GetDocumentInteraction() {
  ServerInteractionPtr interaction;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    interaction = document_interaction_;
  }
  return interaction;
}

bool InteractionManager::CreateStoreInteraction(std::vector<std::string> addrs) {
  auto interaction = std::make_shared<ServerInteraction>();
  if (!interaction->Init(addrs)) {
    DINGO_LOG(ERROR) << "Fail to init store_interaction";
    return false;
  }

  {
    dingodb::RWLockWriteGuard guard(&rw_lock_);
    if (store_interaction_ == nullptr) {
      store_interaction_ = interaction;
    }
  }

  return true;
}

butil::Status InteractionManager::CreateStoreInteraction(int64_t region_id) {
  auto region_entry = RegionRouter::GetInstance().QueryRegionEntry(region_id);
  if (region_entry == nullptr) {
    std::string s = fmt::format("not found store region entry {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EREGION_NOT_FOUND, s);
  }

  if (dingodb::pb::common::RegionType::STORE_REGION != region_entry->Region().region_type()) {
    std::string s = fmt::format("region : {} is not a store region. region_type : {}", region_id,
                                dingodb::pb::common::RegionType_Name(region_entry->Region().region_type()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  if (!CreateStoreInteraction(region_entry->GetAddrs())) {
    std::string s = fmt::format("init store interaction failed, region {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  return butil::Status();
}

bool InteractionManager::CreateIndexInteraction(std::vector<std::string> addrs) {
  auto interaction = std::make_shared<ServerInteraction>();
  if (!interaction->Init(addrs)) {
    DINGO_LOG(ERROR) << "Fail to init index_interaction";
    return false;
  }

  {
    dingodb::RWLockWriteGuard guard(&rw_lock_);
    if (index_interaction_ == nullptr) {
      index_interaction_ = interaction;
    }
  }

  return true;
}
butil::Status InteractionManager::CreateIndexInteraction(int64_t region_id) {
  auto region_entry = RegionRouter::GetInstance().QueryRegionEntry(region_id);
  if (region_entry == nullptr) {
    std::string s = fmt::format("not found index region entry {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EREGION_NOT_FOUND, s);
  }

  if (dingodb::pb::common::RegionType::INDEX_REGION != region_entry->Region().region_type()) {
    std::string s = fmt::format("region : {} is not a index region. region_type : {}", region_id,
                                dingodb::pb::common::RegionType_Name(region_entry->Region().region_type()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  if (!CreateIndexInteraction(region_entry->GetAddrs())) {
    std::string s = fmt::format("init index interaction failed, region {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  return butil::Status();
}

bool InteractionManager::CreateDocumentInteraction(std::vector<std::string> addrs) {
  auto interaction = std::make_shared<ServerInteraction>();
  if (!interaction->Init(addrs)) {
    DINGO_LOG(ERROR) << "Fail to init document_interaction_" << std::endl;
    return false;
  }

  {
    dingodb::RWLockWriteGuard guard(&rw_lock_);
    if (document_interaction_ == nullptr) {
      document_interaction_ = interaction;
    }
  }

  return true;
}

butil::Status InteractionManager::CreateDocumentInteraction(int64_t region_id) {
  auto region_entry = RegionRouter::GetInstance().QueryRegionEntry(region_id);
  if (region_entry == nullptr) {
    std::string s = fmt::format("not found document region entry {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EREGION_NOT_FOUND, s);
  }

  if (dingodb::pb::common::RegionType::DOCUMENT_REGION != region_entry->Region().region_type()) {
    std::string s = fmt::format("region : {} is not a document region. region_type : {}", region_id,
                                dingodb::pb::common::RegionType_Name(region_entry->Region().region_type()));
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  if (!CreateDocumentInteraction(region_entry->GetAddrs())) {
    std::string s = fmt::format("init document interaction failed, region {}", region_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  return butil::Status();
}

void InteractionManager::ResetCoordinatorInteraction() {
  dingodb::RWLockWriteGuard guard(&rw_lock_);
  coordinator_interaction_.reset();
}
void InteractionManager::ResetStoreInteraction() {
  dingodb::RWLockWriteGuard guard(&rw_lock_);
  store_interaction_.reset();
}
void InteractionManager::ResetIndexInteraction() {
  dingodb::RWLockWriteGuard guard(&rw_lock_);
  index_interaction_.reset();
}
void InteractionManager::ResetDocumentInteraction() {
  dingodb::RWLockWriteGuard guard(&rw_lock_);
  document_interaction_.reset();
}

int64_t InteractionManager::GetCoordinatorInteractionLatency() {
  int64_t latency = 0;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    if (coordinator_interaction_ == nullptr) {
      return 0;
    }

    latency = coordinator_interaction_->GetLatency();
  }
  return latency;
}

int64_t InteractionManager::GetStoreInteractionLatency() {
  int64_t latency = 0;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    if (store_interaction_ == nullptr) {
      return 0;
    }

    latency = store_interaction_->GetLatency();
  }
  return latency;
}

int64_t InteractionManager::GetIndexInteractionLatency() {
  int64_t latency = 0;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    if (index_interaction_ == nullptr) {
      return 0;
    }

    latency = index_interaction_->GetLatency();
  }
  return latency;
}

int64_t InteractionManager::GetDocumentInteractionLatency() {
  int64_t latency = 0;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    if (document_interaction_ == nullptr) {
      return 0;
    }

    latency = document_interaction_->GetLatency();
  }
  return latency;
}

}  // namespace br