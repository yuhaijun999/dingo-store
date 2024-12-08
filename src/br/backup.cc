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

#include "br/backup.h"

#include <cstdint>
#include <memory>
#include <string>

#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/utils.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"

namespace br {

Backup::Backup(const BackupParams& params)
    : is_gc_stop_(false), is_gc_enable_after_finish_(false), is_need_exit_(false) {
  coor_url_ = params.coor_url;
  br_type_ = params.br_type;
  br_backup_type_ = params.br_backup_type;
  backupts_ = params.backupts;
  backuptso_internal_ = params.backuptso_internal;
  storage_ = params.storage;
  storage_internal_ = params.storage_internal;
}

Backup::~Backup() = default;

std::shared_ptr<Backup> Backup::GetSelf() { return shared_from_this(); }

butil::Status Backup::Init() {
  butil::Status status;
  status = ParamsCheck();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  return butil::Status::OK();
}

butil::Status Backup::Run() { return butil::Status::OK(); }

butil::Status Backup::Finish() { return butil::Status::OK(); }

butil::Status Backup::ParamsCheck() {
  butil::Status status;
  status = ParamsCheckForStorage();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  return butil::Status::OK();
}

butil::Status Backup::ParamsCheckForStorage() {
  butil::Status status;
  status = Utils::DirExists(storage_internal_);
  if (status.ok()) {
    std::string lock_path = storage_internal_ + "/" + kBackupFileLock;
    status = Utils::FileExistsAndRegular(lock_path);
    if (status.ok()) {
      std::string s = fmt::format("Backup is running, please wait or delete lock file : {}", lock_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::EFILE_EXIST, s);
    } else if (status.error_code() != dingodb::pb::error::EFILE_NOT_EXIST) {
      std::string s = fmt::format("Check lock file : {} failed: {}", lock_path, status.error_cstr());
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::EINTERNAL, s);
    }

  } else if (status.error_code() != dingodb::pb::error::EFILE_NOT_EXIST) {
    std::string s = fmt::format("Check storage : {} storage_internal_ : {} failed: {}", storage_, storage_internal_,
                                status.error_cstr());
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }
  return butil::Status::OK();
}

butil::Status Backup::GetGcSafePoint() {
  dingodb::pb::coordinator::GetGCSafePointRequest request;
  dingodb::pb::coordinator::GetGCSafePointResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_get_all_tenant(true);
  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "GetGCSafePoint", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to get GC safe point, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to get GC safe point, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  int64_t max_tenant_safe_points;
  int64_t min_tenant_resolve_lock_safe_points;

  max_tenant_safe_points = response.safe_point();
  min_tenant_resolve_lock_safe_points = response.resolve_lock_safe_point();

  for (const auto& [id, safe_point] : response.tenant_safe_points()) {
    if (safe_point > max_tenant_safe_points) {
      max_tenant_safe_points = safe_point;
    }
  }

  for (const auto& [id, safe_point] : response.tenant_resolve_lock_safe_points()) {
    if (safe_point < min_tenant_resolve_lock_safe_points) {
      min_tenant_resolve_lock_safe_points = safe_point;
    }
  }

  // compare safe points
  if (backuptso_internal_ > max_tenant_safe_points && backuptso_internal_ <= min_tenant_resolve_lock_safe_points) {
    DINGO_LOG(INFO) << "Backup safe point is " << backuptso_internal_;
  } else {
    std::string s = fmt::format(
        "Backup safe point is {}, but max tenant safe point is {}, min tenant resolve lock safe point is {}",
        backuptso_internal_, max_tenant_safe_points, min_tenant_resolve_lock_safe_points);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EILLEGAL_PARAMTETERS, s);
  }

  if (response.gc_stop()) {
    is_gc_stop_ = true;
    is_gc_enable_after_finish_ = false;
    DINGO_LOG(INFO) << "GC is already stopped. Backup will not enable  if backup is finished.";
  }

  return butil::Status::OK();
}

butil::Status Backup::SetGcStop() {
  if (is_gc_stop_) {
    return butil::Status::OK();
  }

  DINGO_LOG(INFO) << "Set GC stop ...";

  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_gc_flag(
      ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_STOP);
  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "UpdateGCSafePoint", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set GC stop, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set GC stop, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  is_gc_stop_ = true;
  is_gc_enable_after_finish_ = true;

  DINGO_LOG(INFO) << "GC is stopped. Backup will enable GC.  if backup is finished.";

  return butil::Status::OK();
}

butil::Status Backup::SetGcStart() {
  if (!is_gc_enable_after_finish_) {
    return butil::Status::OK();
  }
  DINGO_LOG(INFO) << "Set GC start ...";

  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_gc_flag(
      ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_START);
  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "UpdateGCSafePoint", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set GC stop, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set GC stop, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  is_gc_stop_ = false;
  is_gc_enable_after_finish_ = false;

  DINGO_LOG(INFO) << "Set GC start success.";

  return butil::Status::OK();
}

}  // namespace br