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
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "common/uuid.h"
#include "fmt/core.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"

namespace br {

#ifndef ENABLE_BACKUP_PTHREAD
#define ENABLE_BACKUP_PTHREAD
#endif

#undef ENABLE_BACKUP_PTHREAD

Backup::Backup(const BackupParams& params)
    : is_gc_stop_(false),
      is_gc_enable_after_finish_(false),
      is_need_exit_(false),
      is_already_register_backup_to_coordinator_(false) {
  coor_url_ = params.coor_url;
  br_type_ = params.br_type;
  br_backup_type_ = params.br_backup_type;
  backupts_ = params.backupts;
  backuptso_internal_ = params.backuptso_internal;
  storage_ = params.storage;
  storage_internal_ = params.storage_internal;

  bthread_mutex_init(&mutex_, nullptr);
}

Backup::~Backup() { bthread_mutex_destroy(&mutex_); };

std::shared_ptr<Backup> Backup::GetSelf() { return shared_from_this(); }

butil::Status Backup::Init() {
  butil::Status status;
  status = ParamsCheck();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  backup_task_id_ = dingodb::UUIDGenerator::GenerateUUID();

  auto lambda_exit_function = [this, &status]() {
    if (!status.ok()) {
      if (is_already_register_backup_to_coordinator_) {
        UnregisterBackupToCoordinator(br::InteractionManager::GetInstance().GetCoordinatorInteraction());
      }

      if (is_gc_enable_after_finish_) {
        SetGcStart();
      }
      last_error_ = status;
    };
  };

  dingodb::ON_SCOPE_EXIT(lambda_exit_function);

  // try to register backup task
  bool is_first = true;
  status = RegisterBackupToCoordinator(is_first, br::InteractionManager::GetInstance().GetCoordinatorInteraction());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  is_already_register_backup_to_coordinator_ = true;

  // set gc stop
  if (!is_gc_stop_) {
    status = SetGcStop();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // TODO : Prohibit region split and merge. As well as capacity expansion and contraction

  status = Utils::DirExists(storage_internal_);
  if (status.ok()) {
    // clean backup dir
    status = Utils::ClearDir(storage_internal_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  } else if (dingodb::pb::error::EFILE_NOT_EXIST == status.error_code()) {
    // create backup dir
    status = Utils::CreateDirRecursion(storage_internal_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  } else {  // error
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // create backup lock file
  std::ofstream writer;
  std::string lock_path = storage_internal_ + "/" + kBackupFileLock;
  status = Utils::CreateFile(writer, lock_path);
  if (!status.ok()) {
    if (writer.is_open()) {
      writer.close();
    }
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  writer << "DO NOT DELETE" << std::endl;
  writer << "This file exists to remind other backup jobs won't use this path" << std::endl;

  if (writer.is_open()) {
    writer.close();
  }

  return butil::Status::OK();
}

butil::Status Backup::Run() {
  butil::Status status;

  auto lambda_exit_function = [this, &status]() {
    if (!status.ok()) {
      if (is_already_register_backup_to_coordinator_) {
        UnregisterBackupToCoordinator(br::InteractionManager::GetInstance().GetCoordinatorInteraction());
      }

      if (is_gc_enable_after_finish_) {
        SetGcStart();
      }

      last_error_ = status;

      is_need_exit_ = true;
    };
  };

  dingodb::ON_SCOPE_EXIT(lambda_exit_function);

  std::vector<std::string> coordinator_addrs =
      br::InteractionManager::GetInstance().GetCoordinatorInteraction()->GetAddrs();

  std::vector<std::string> store_addrs = br::InteractionManager::GetInstance().GetStoreInteraction()->GetAddrs();

  std::vector<std::string> index_addrs = br::InteractionManager::GetInstance().GetIndexInteraction()->GetAddrs();

  std::vector<std::string> document_addrs = br::InteractionManager::GetInstance().GetDocumentInteraction()->GetAddrs();

  // create register backup task to coordinator
  {
    std::shared_ptr<br::ServerInteraction> coordinator_interaction;

    status = CreateCoordinatorInteraction(coordinator_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    // register backup task to coordinator
    status = DoAsyncRegisterBackupToCoordinator(coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  DINGO_LOG(INFO) << "Backup task " << backup_task_id_ << " is registered to coordinator Periodicity.";

  // create backup sql meta
  std::vector<int64_t> sql_meta_regions;
  {
    std::shared_ptr<br::ServerInteraction> coordinator_interaction;
    status = CreateCoordinatorInteraction(coordinator_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> store_interaction;
    status = CreateStoreInteraction(store_addrs, store_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    backup_sql_meta_ = std::make_shared<BackupSqlMeta>(coordinator_interaction, store_interaction, backupts_,
                                                       backuptso_internal_, storage_, storage_internal_);
    status = backup_sql_meta_->GetSqlMetaRegionFromCoordinator();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    backup_sql_meta_->GetSqlMetaRegionList(sql_meta_regions);
    DINGO_LOG(INFO) << "Backup task " << backup_task_id_ << " has " << sql_meta_regions.size() << " sql meta regions.";
  }

  // create backup sdk meta
  {
    std::shared_ptr<br::ServerInteraction> coordinator_interaction;
    status = CreateCoordinatorInteraction(coordinator_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    backup_sdk_meta_ = std::make_shared<BackupSdkMeta>(coordinator_interaction, storage_internal_);
    status = backup_sdk_meta_->GetSdkMetaFromCoordinator();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // create backup meta
  {
    std::shared_ptr<br::ServerInteraction> coordinator_interaction;
    status = CreateCoordinatorInteraction(coordinator_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> store_interaction;
    status = CreateStoreInteraction(store_addrs, store_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> index_interaction;
    status = CreateIndexInteraction(index_addrs, index_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> document_interaction;
    status = CreateDocumentInteraction(document_addrs, document_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    backup_meta_ =
        std::make_shared<BackupMeta>(coordinator_interaction, store_interaction, index_interaction,
                                     document_interaction, backupts_, backuptso_internal_, storage_, storage_internal_);
    // TODO : get present ids from coordinator later.
    // status = backup_meta_->GetPresentIdsFromCoordinator();
    // if (!status.ok()) {
    //   DINGO_LOG(ERROR) << status.error_cstr();
    //   return status;
    // }
  }

  // create backup data
  {
    std::shared_ptr<br::ServerInteraction> coordinator_interaction;
    status = CreateCoordinatorInteraction(coordinator_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> store_interaction;
    status = CreateStoreInteraction(store_addrs, store_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> index_interaction;
    status = CreateIndexInteraction(index_addrs, index_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> document_interaction;
    status = CreateDocumentInteraction(document_addrs, document_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    backup_data_ =
        std::make_shared<BackupData>(coordinator_interaction, store_interaction, index_interaction,
                                     document_interaction, backupts_, backuptso_internal_, storage_, storage_internal_);

    backup_data_->GetAllRegionMapFromCoordinator();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // create backup sql data
  {
    std::shared_ptr<br::ServerInteraction> coordinator_interaction;
    status = CreateCoordinatorInteraction(coordinator_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> store_interaction;
    status = CreateStoreInteraction(store_addrs, store_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> index_interaction;
    status = CreateIndexInteraction(index_addrs, index_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> document_interaction;
    status = CreateDocumentInteraction(document_addrs, document_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    backup_sql_data_ = std::make_shared<BackupSqlData>(coordinator_interaction, store_interaction, index_interaction,
                                                       document_interaction, backupts_, backuptso_internal_, storage_,
                                                       storage_internal_);

    backup_sql_data_->SetRegionMap(backup_data_->GetRegionMap());

    status = backup_sql_data_->RemoveSqlMeta(sql_meta_regions);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    status = backup_sql_data_->Filter();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    status = backup_sql_data_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // create backup sdk data
  {
    std::shared_ptr<br::ServerInteraction> coordinator_interaction;
    status = CreateCoordinatorInteraction(coordinator_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> store_interaction;
    status = CreateStoreInteraction(store_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> index_interaction;
    status = CreateIndexInteraction(index_addrs, index_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> document_interaction;
    status = CreateDocumentInteraction(document_addrs, document_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    backup_sdk_data_ = std::make_shared<BackupSdkData>(coordinator_interaction, store_interaction, index_interaction,
                                                       document_interaction, backupts_, backuptso_internal_, storage_,
                                                       storage_internal_);

    backup_sdk_data_->SetRegionMap(backup_data_->GetRegionMap());

    backup_sdk_data_->SetRegionMap(backup_data_->GetRegionMap());

    status = backup_sdk_data_->Filter();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    status = backup_sdk_data_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return DoRun();
}

butil::Status Backup::Finish() { return butil::Status::OK(); }

butil::Status Backup::DoRun() {
  DINGO_LOG(INFO) << "Backup task " << backup_task_id_ << " is running.";
  DINGO_LOG(INFO) << "Backup sql data " << backup_task_id_ << " is running.";

  return butil::Status::OK();
}

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

butil::Status Backup::RegisterBackupToCoordinator(bool is_first, ServerInteractionPtr coordinator_interaction) {
  dingodb::pb::coordinator::RegisterBackupRequest request;
  dingodb::pb::coordinator::RegisterBackupResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  request.set_backup_name(backup_task_id_);
  request.set_backup_path(storage_internal_);
  int64_t current_now_s = dingodb::Helper::Timestamp();
  if (is_first) {
    request.set_backup_start_timestamp(current_now_s);
  }
  request.set_backup_current_timestamp(current_now_s);
  request.set_backup_timeout_s(FLAGS_backup_task_timeout_s);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "RegisterBackup", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set RegisterBackup, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set RegisterBackup, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status Backup::UnregisterBackupToCoordinator(ServerInteractionPtr coordinator_interaction) {
  dingodb::pb::coordinator::UnRegisterBackupRequest request;
  dingodb::pb::coordinator::UnRegisterBackupResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  request.set_backup_name(backup_task_id_);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "UnRegisterBackup", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set UnRegisterBackup, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set UnRegisterBackup, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status Backup::DoAsyncRegisterBackupToCoordinator(ServerInteractionPtr coordinator_interaction) {
  std::shared_ptr<Backup> self = GetSelf();
  auto lambda_call = [self, coordinator_interaction]() {
    self->DoRegisterBackupToCoordinatorInternal(coordinator_interaction);
  };

#if defined(ENABLE_BACKUP_PTHREAD)
  std::thread th(lambda_call);
  th.detach();
#else

  std::function<void()>* call = new std::function<void()>;
  *call = lambda_call;
  bthread_t th;

  int ret = bthread_start_background(
      &th, nullptr,
      [](void* arg) -> void* {
        auto* call = static_cast<std::function<void()>*>(arg);
        (*call)();
        delete call;
        return nullptr;
      },
      call);
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("bthread_start_background fail");
    return butil::Status(dingodb::pb::error::EINTERNAL, "bthread_start_background fail");
  }
#endif  // #if defined(ENABLE_BACKUP_PTHREAD)

  return butil::Status::OK();
}

butil::Status Backup::DoRegisterBackupToCoordinatorInternal(ServerInteractionPtr coordinator_interaction) {
  butil::Status status;
  bool is_first = false;
  bool is_error_occur = true;
  while (!is_need_exit_) {
    uint32_t retry_times = FLAGS_backup_task_max_retry;
    do {
      status = RegisterBackupToCoordinator(is_first, coordinator_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      } else {  // success
        is_error_occur = false;
        break;
      }
      sleep(FLAGS_backup_watch_interval_s);
    } while (is_need_exit_ && retry_times-- > 0);

    if (is_error_occur) {
      if (!is_need_exit_) {
        is_need_exit_ = true;
      }

      {
        BAIDU_SCOPED_LOCK(mutex_);
        last_error_ = status;
      }
      break;
    }

    sleep(FLAGS_backup_watch_interval_s);
  }
  return butil::Status::OK();
}

butil::Status Backup::CreateCoordinatorInteraction(const std::vector<std::string>& coordinator_addrs,
                                                   ServerInteractionPtr& coordinator_interaction) {
  butil::Status status;
  coordinator_interaction = std::make_shared<br::ServerInteraction>();
  if (!coordinator_interaction->Init(coordinator_addrs)) {
    std::string s = fmt::format("Fail to init coordinator_interaction, addrs");
    for (const auto& addr : coordinator_addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    status = butil::Status(dingodb::pb::error::EINTERNAL, s);
    return status;
  }

  return butil::Status::OK();
}

butil::Status Backup::CreateStoreInteraction(const std::vector<std::string>& store_addrs,
                                             ServerInteractionPtr& store_interaction) {
  butil::Status status;
  store_interaction = std::make_shared<br::ServerInteraction>();
  if (!store_interaction->Init(store_addrs)) {
    std::string s = fmt::format("Fail to init store_interaction, addrs");
    for (const auto& addr : store_addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    status = butil::Status(dingodb::pb::error::EINTERNAL, s);
    return status;
  }

  return butil::Status::OK();
}
butil::Status Backup::CreateIndexInteraction(const std::vector<std::string>& index_addrs,
                                             ServerInteractionPtr& index_interaction) {
  butil::Status status;
  index_interaction = std::make_shared<br::ServerInteraction>();
  if (!index_interaction->Init(index_addrs)) {
    std::string s = fmt::format("Fail to init index_interaction, addrs");
    for (const auto& addr : index_addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    status = butil::Status(dingodb::pb::error::EINTERNAL, s);
    return status;
  }

  return butil::Status::OK();
}
butil::Status Backup::CreateDocumentInteraction(const std::vector<std::string>& document_addrs,
                                                ServerInteractionPtr& document_interaction) {
  butil::Status status;
  document_interaction = std::make_shared<br::ServerInteraction>();
  if (!document_interaction->Init(document_addrs)) {
    std::string s = fmt::format("Fail to init document_interaction, addrs");
    for (const auto& addr : document_addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    status = butil::Status(dingodb::pb::error::EINTERNAL, s);
    return status;
  }

  return butil::Status::OK();
}

}  // namespace br