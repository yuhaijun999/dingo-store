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

std::pair<butil::Status, ServerInteractionPtr> InteractionManager::CloneCoordinatorInteraction() {
  ServerInteractionPtr interaction;
  std::vector<std::string> addrs;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    addrs = coordinator_interaction_->GetAddrs();
  }

  return CreateInteraction(addrs);
}
std::pair<butil::Status, ServerInteractionPtr> InteractionManager::CloneStoreInteraction() {
  ServerInteractionPtr interaction;
  std::vector<std::string> addrs;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    addrs = store_interaction_->GetAddrs();
  }

  return CreateInteraction(addrs);
}
std::pair<butil::Status, ServerInteractionPtr> InteractionManager::CloneIndexInteraction() {
  ServerInteractionPtr interaction;
  std::vector<std::string> addrs;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    addrs = index_interaction_->GetAddrs();
  }

  return CreateInteraction(addrs);
}
std::pair<butil::Status, ServerInteractionPtr> InteractionManager::CloneDocumentInteraction() {
  ServerInteractionPtr interaction;
  std::vector<std::string> addrs;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    addrs = document_interaction_->GetAddrs();
  }

  return CreateInteraction(addrs);
}

std::pair<butil::Status, ServerInteractionPtr> InteractionManager::CreateInteraction(
    const std::vector<std::string>& addrs) {
  butil::Status status;

  ServerInteractionPtr interaction = std::make_shared<br::ServerInteraction>();

  if (!interaction->Init(addrs)) {
    std::string s = fmt::format("Fail to init interaction, addrs");
    for (const auto& addr : addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    status = butil::Status(dingodb::pb::error::EINTERNAL, s);
    return {status, nullptr};
  }
  return {butil::Status::OK(), interaction};
}

}  // namespace br