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

#include "br/backup_sdk_meta.h"

#include <memory>
#include <string>

#include "br/helper.h"
#include "fmt/core.h"

namespace br {

BackupSdkMeta::BackupSdkMeta(ServerInteractionPtr coordinator_interaction)
    : coordinator_interaction_(coordinator_interaction) {}

BackupSdkMeta::~BackupSdkMeta() = default;

std::shared_ptr<BackupSdkMeta> BackupSdkMeta::GetSelf() { return shared_from_this(); }

butil::Status BackupSdkMeta::GetSdkMetaFromCoordinator() {
  dingodb::pb::meta::ExportMetaRequest request;
  dingodb::pb::meta::ExportMetaResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  auto status = coordinator_interaction_->SendRequest("MetaService", "ExportMeta", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << response.error().errmsg();
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  meta_all_ = std::make_shared<dingodb::pb::meta::MetaALL>(response.meta_all());

  return butil::Status::OK();
}

}  // namespace br