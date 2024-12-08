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

#include "br/backup_meta.h"

#include <memory>
#include <string>

#include "br/helper.h"
#include "fmt/core.h"

namespace br {

BackupMeta::BackupMeta(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                       ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                       std::string backupts, int64_t backuptso_internal, std::string storage,
                       std::string storage_internal)
    : coordinator_interaction_(coordinator_interaction),
      store_interaction_(store_interaction),
      index_interaction_(index_interaction),
      document_interaction_(document_interaction),
      backupts_(backupts),
      backuptso_internal_(backuptso_internal),
      storage_(storage),
      storage_internal_(storage_internal) {}

BackupMeta::~BackupMeta() = default;

std::shared_ptr<BackupMeta> BackupMeta::GetSelf() { return shared_from_this(); }

butil::Status BackupMeta::GetPresentIdsFromCoordinator() {
  dingodb::pb::meta::SaveIdEpochTypeRequest request;
  dingodb::pb::meta::SaveIdEpochTypeResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  auto status = coordinator_interaction_->SendRequest("MetaService", "SaveIdEpochType", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << response.error().errmsg();
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  id_epoch_type_and_value_ =
      std::make_shared<dingodb::pb::meta::IdEpochTypeAndValue>(response.id_epoch_type_and_value());

  return butil::Status::OK();
}

}  // namespace br