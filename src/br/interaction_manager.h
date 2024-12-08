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

#ifndef DINGODB_BR_INTERATION_MANAGER_H_
#define DINGODB_BR_INTERATION_MANAGER_H_

#include "br/interation.h"
#include "br/router.h"
#include "common/synchronization.h"

namespace br {
class InteractionManager {
 public:
  static InteractionManager& GetInstance();

  void SetCoordinatorInteraction(ServerInteractionPtr interaction);
  void SetStoreInteraction(ServerInteractionPtr interaction);
  void SetIndexInteraction(ServerInteractionPtr interaction);
  void SetDocumentInteraction(ServerInteractionPtr interaction);

  ServerInteractionPtr GetCoordinatorInteraction();
  ServerInteractionPtr GetStoreInteraction();
  ServerInteractionPtr GetIndexInteraction();
  ServerInteractionPtr GetDocumentInteraction();

  bool CreateStoreInteraction(std::vector<std::string> addrs);
  [[deprecated(
      "CreateStoreInteraction with region_id is deprecated. Use CreateStoreInteraction addrs instead.")]] butil::Status
  CreateStoreInteraction(int64_t region_id);

  bool CreateIndexInteraction(std::vector<std::string> addrs);
  [[deprecated(
      "CreateIndexInteraction with region_id is deprecated. Use CreateIndexInteraction addrs instead.")]] butil::Status
  CreateIndexInteraction(int64_t region_id);

  bool CreateDocumentInteraction(std::vector<std::string> addrs);
  [[deprecated(
      "CreateDocumentInteraction with region_id is deprecated. Use CreateDocumentInteraction addrs instead.")]] butil::
      Status
      CreateDocumentInteraction(int64_t region_id);

  void ResetCoordinatorInteraction();
  void ResetStoreInteraction();
  void ResetIndexInteraction();
  void ResetDocumentInteraction();

  int64_t GetCoordinatorInteractionLatency();
  int64_t GetStoreInteractionLatency();
  int64_t GetIndexInteractionLatency();
  int64_t GetDocumentInteractionLatency();

  template <typename Request, typename Response>
  butil::Status SendRequestWithoutContext(const std::string& service_name, const std::string& api_name,
                                          const Request& request, Response& response);

  template <typename Request, typename Response>
  [[deprecated(
      "SendRequestWithContext is deprecated. Use SendRequestWithoutContext instead. "
      "instead.")]] butil::Status
  SendRequestWithContext(const std::string& service_name, const std::string& api_name, Request& request,
                         Response& response);

  template <typename Request, typename Response>
  butil::Status AllSendRequestWithoutContext(const std::string& service_name, const std::string& api_name,
                                             const Request& request, Response& response);

  template <typename Request, typename Response>
  [[deprecated(
      "AllSendRequestWithContext is deprecated. Use AllSendRequestWithoutContext instead. "
      "instead.")]] butil::Status
  AllSendRequestWithContext(const std::string& service_name, const std::string& api_name, const Request& request,
                            Response& response);

 private:
  InteractionManager();
  ~InteractionManager();

  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;
  ServerInteractionPtr index_interaction_;
  ServerInteractionPtr document_interaction_;

  dingodb::RWLock rw_lock_;
};

template <typename Request, typename Response>
butil::Status InteractionManager::SendRequestWithoutContext(const std::string& service_name,
                                                            const std::string& api_name, const Request& request,
                                                            Response& response) {
  dingodb::RWLockReadGuard guard(&rw_lock_);
  if (service_name == "CoordinatorService" || service_name == "MetaService") {
    return coordinator_interaction_->SendRequest(service_name, api_name, request, response);
  } else if (service_name == "StoreService") {
    return store_interaction_->SendRequest(service_name, api_name, request, response);
  } else if (service_name == "IndexService") {
    return index_interaction_->SendRequest(service_name, api_name, request, response);
  } else if (service_name == "DocumentService") {
    return document_interaction_->SendRequest(service_name, api_name, request, response);
  } else {
    DINGO_LOG(FATAL) << "Unknown service name: " << service_name;
  }

  return butil::Status();
}

template <typename Request, typename Response>
butil::Status InteractionManager::SendRequestWithContext(const std::string& service_name, const std::string& api_name,
                                                         Request& request, Response& response) {
  ServerInteractionPtr interaction;
  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    if (service_name == "StoreService") {
      interaction = store_interaction_;
    } else if (service_name == "IndexService") {
      interaction = index_interaction_;
    } else if (service_name == "DocumentService") {
      interaction = document_interaction_;
    } else {
      DINGO_LOG(FATAL) << "Unknown service name: " << service_name;
    }
  }

  if (interaction == nullptr) {
    butil::Status status;
    if (service_name == "StoreService") {
      status = CreateStoreInteraction(request.context().region_id());
    } else if (service_name == "IndexService") {
      status = CreateIndexInteraction(request.context().region_id());
    } else if (service_name == "DocumentService") {
      status = CreateDocumentInteraction(request.context().region_id());
    }
    if (!status.ok()) {
      return status;
    }
  }

  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    if (service_name == "StoreService") {
      interaction = store_interaction_;
    } else if (service_name == "IndexService") {
      interaction = index_interaction_;
    } else if (service_name == "DocumentService") {
      interaction = document_interaction_;
    } else {
      DINGO_LOG(FATAL) << "Unknown service name: " << service_name;
    }
  }

  for (;;) {
    {
      dingodb::RWLockReadGuard guard(&rw_lock_);
      auto status = interaction->SendRequest(service_name, api_name, request, response);
      if (status.ok()) {
        return status;
      }

      if (response.error().errcode() == dingodb::pb::error::EREGION_VERSION) {
        RegionRouter::GetInstance().UpdateRegionEntry(response.error().store_region_info());
        DINGO_LOG(INFO) << "QueryRegionEntry region_id: " << request.context().region_id();
        auto region_entry = RegionRouter::GetInstance().QueryRegionEntry(request.context().region_id());
        if (region_entry == nullptr) {
          return butil::Status(dingodb::pb::error::EREGION_NOT_FOUND, "Not found region %lu",
                               request.context().region_id());
        }
        *request.mutable_context() = region_entry->GenConext();
      } else {
        return status;
      }
    }
    bthread_usleep(1000 * 500);
  }
}

template <typename Request, typename Response>
butil::Status InteractionManager::AllSendRequestWithoutContext(const std::string& service_name,
                                                               const std::string& api_name, const Request& request,
                                                               Response& response) {
  dingodb::RWLockReadGuard guard(&rw_lock_);
  if (service_name == "CoordinatorService" || service_name == "MetaService") {
    return coordinator_interaction_->AllSendRequest(service_name, api_name, request, response);
  } else if (service_name == "StoreService") {
    return store_interaction_->AllSendRequest(service_name, api_name, request, response);
  } else if (service_name == "IndexService") {
    return index_interaction_->AllSendRequest(service_name, api_name, request, response);
  } else if (service_name == "DocumentService") {
    return document_interaction_->AllSendRequest(service_name, api_name, request, response);
  } else {
    DINGO_LOG(FATAL) << "Unknown service name: " << service_name;
  }

  return butil::Status();
}

template <typename Request, typename Response>
butil::Status InteractionManager::AllSendRequestWithContext(const std::string& service_name,
                                                            const std::string& api_name, const Request& request,
                                                            Response& response) {
  ServerInteractionPtr interaction;
  dingodb::RWLockReadGuard guard(&rw_lock_);
  if (service_name == "StoreService") {
    interaction = store_interaction_;
  } else if (service_name == "IndexService") {
    interaction = index_interaction_;
  } else if (service_name == "DocumentService") {
    interaction = document_interaction_;
  } else {
    DINGO_LOG(FATAL) << "Unknown service name: " << service_name;
  }

  if (interaction == nullptr) {
    butil::Status status;
    if (service_name == "StoreService") {
      status = CreateStoreInteraction(request.context().region_id());
    } else if (service_name == "IndexService") {
      status = CreateIndexInteraction(request.context().region_id());
    } else if (service_name == "DocumentService") {
      status = CreateDocumentInteraction(request.context().region_id());
    }
    if (!status.ok()) {
      return status;
    }
  }

  {
    dingodb::RWLockReadGuard guard(&rw_lock_);
    if (service_name == "StoreService") {
      interaction = store_interaction_;
    } else if (service_name == "IndexService") {
      interaction = index_interaction_;
    } else if (service_name == "DocumentService") {
      interaction = document_interaction_;
    } else {
      DINGO_LOG(FATAL) << "Unknown service name: " << service_name;
    }
  }

  for (;;) {
    {
      dingodb::RWLockReadGuard guard(&rw_lock_);
      auto status = interaction->AllSendRequest(service_name, api_name, request, response);
      if (status.ok()) {
        return status;
      }

      if (response.error().errcode() == dingodb::pb::error::EREGION_VERSION) {
        RegionRouter::GetInstance().UpdateRegionEntry(response.error().store_region_info());
        auto region_entry = RegionRouter::GetInstance().QueryRegionEntry(request.context().region_id());
        if (region_entry == nullptr) {
          return butil::Status(dingodb::pb::error::EREGION_NOT_FOUND, "Not found region %lu",
                               request.context().region_id());
        }
        *request.mutable_context() = region_entry->GenConext();
      } else {
        return status;
      }
    }
    bthread_usleep(1000 * 500);
  }
}
}  // namespace br

#endif  // DINGODB_BR_INTERATION_MANAGER_H_