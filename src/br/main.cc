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

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "br/backup.h"
#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/interation.h"
#include "br/parameter.h"
#include "br/utils.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "coordinator/tso_control.h"
#include "gflags/gflags.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"

// DEFINE_string(coor_url, "", "coordinator url");

// DEFINE_string(br_type, "backup", "backup restore type. default: backup");

// DEFINE_string(br_backup_type, "full", "backup  type. default: full.");

// DEFINE_string(backupts, "", "backup ts. like: 2022-09-08 13:30:00 +08:00");
// DEFINE_int64(backuptso_internal, 0, "backup tso. like: convert 2022-09-08 13:30:00 +08:00 to tso");

// DEFINE_string(storage, "", "storage. like: local:///br_data");
// DEFINE_string(storage_internal, "", "storage. like: /br_data. remove local://");

const std::string kProgramName = "dingodb_br";

static void InitLog(const std::string& log_dir) {
  if (!dingodb::Helper::IsExistPath(log_dir)) {
    dingodb::Helper::CreateDirectories(log_dir);
  }

  FLAGS_logbufsecs = 0;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;
  FLAGS_logtostdout = false;
  FLAGS_logtostderr = false;
  FLAGS_alsologtostderr = false;

  google::InitGoogleLogging(kProgramName.c_str());
  google::SetLogDestination(google::GLOG_INFO, fmt::format("{}/{}.info.log.", log_dir, kProgramName).c_str());
  google::SetLogDestination(google::GLOG_WARNING, fmt::format("{}/{}.warn.log.", log_dir, kProgramName).c_str());
  google::SetLogDestination(google::GLOG_ERROR, fmt::format("{}/{}.error.log.", log_dir, kProgramName).c_str());
  google::SetLogDestination(google::GLOG_FATAL, fmt::format("{}/{}.fatal.log.", log_dir, kProgramName).c_str());
  google::SetStderrLogging(google::GLOG_FATAL);
}

static butil::Status SetStoreInteraction() {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.add_filter_store_types(::dingodb::pb::common::StoreType::NODE_TYPE_STORE);
  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "GetStoreMap", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to get store map, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  std::vector<std::string> addrs;

  for (int i = 0; i < response.storemap().stores_size(); i++) {
    const dingodb::pb::common::Store& store = response.storemap().stores(i);
    const auto& location = store.server_location();
    DINGO_LOG(INFO) << "store_id=" << store.id() << ", host=" << location.host() << ",  " << location.port();
    addrs.push_back(fmt::format("{}:{}", location.host(), location.port()));
  }

  std::shared_ptr<br::ServerInteraction> store_interaction = std::make_shared<br::ServerInteraction>();
  if (!store_interaction->Init(addrs)) {
    std::string s = fmt::format("Fail to init store_interaction, addrs");
    for (const auto& addr : addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  br::InteractionManager::GetInstance().SetStoreInteraction(store_interaction);
  return butil::Status();
}

static butil::Status SetIndexInteraction() {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.add_filter_store_types(::dingodb::pb::common::StoreType::NODE_TYPE_INDEX);
  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "GetStoreMap", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to get index map, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  std::vector<std::string> addrs;

  for (int i = 0; i < response.storemap().stores_size(); i++) {
    const dingodb::pb::common::Store& store = response.storemap().stores(i);
    const auto& location = store.server_location();
    DINGO_LOG(INFO) << "index_id=" << store.id() << ", host=" << location.host() << ",  " << location.port();
    addrs.push_back(fmt::format("{}:{}", location.host(), location.port()));
  }

  std::shared_ptr<br::ServerInteraction> index_interaction = std::make_shared<br::ServerInteraction>();
  if (!index_interaction->Init(addrs)) {
    std::string s = fmt::format("Fail to init index_interaction, addrs");
    for (const auto& addr : addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  br::InteractionManager::GetInstance().SetIndexInteraction(index_interaction);
  return butil::Status();
}

static butil::Status SetDocumentInteraction() {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.add_filter_store_types(::dingodb::pb::common::StoreType::NODE_TYPE_DOCUMENT);
  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "GetStoreMap", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to get document map, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  std::vector<std::string> addrs;

  for (int i = 0; i < response.storemap().stores_size(); i++) {
    const dingodb::pb::common::Store& store = response.storemap().stores(i);
    const auto& location = store.server_location();
    DINGO_LOG(INFO) << "document_id=" << store.id() << ", host=" << location.host() << ",  " << location.port();
    addrs.push_back(fmt::format("{}:{}", location.host(), location.port()));
  }

  std::shared_ptr<br::ServerInteraction> document_interaction = std::make_shared<br::ServerInteraction>();
  if (!document_interaction->Init(addrs)) {
    std::string s = fmt::format("Fail to init document_interaction, addrs");
    for (const auto& addr : addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  br::InteractionManager::GetInstance().SetDocumentInteraction(document_interaction);
  return butil::Status();
}

static int64_t ConvertToMilliseconds(const std::string& datetime) {
  std::istringstream ss(datetime);

  std::tm tm = {};
  ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

  if (ss.fail()) {
    throw std::runtime_error("Failed to parse date-time");
  }

  std::chrono::system_clock::time_point tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));

  auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();

  return milliseconds;
}

// std::string backup_ts = "2022-09-08 13:30:00 +08:00";
// TODO : Ignore the time zone for now and use Boost.DateTime or date.h provided by Howard Hinnant later.
static butil::Status ConvertBackupTsToTso(const std::string& backup_ts, int64_t& tso) {
  int64_t milliseconds = 0;
  try {
    milliseconds = ConvertToMilliseconds(backup_ts);

  } catch (const std::exception& e) {
    std::string s = fmt::format("Failed to parse backup_ts, {}", e.what());
    DINGO_LOG(ERROR) << e.what() << std::endl;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  int64_t internal_milliseconds = milliseconds - dingodb::kBaseTimestampMs;
  tso = 0;
  tso = internal_milliseconds << dingodb::kLogicalBits;

  return butil::Status();
}

int main(int argc, char* argv[]) {
  InitLog("./log");

  if (dingodb::Helper::IsExistPath("conf/gflags.conf")) {
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
  }

  google::ParseCommandLineFlags(&argc, &argv, true);

  if (dingodb::FLAGS_show_version || argc == 1) {
    dingodb::DingoShowVerion();
    exit(-1);
  }

  if (br::FLAGS_coor_url.empty()) {
    DINGO_LOG(WARNING) << "coordinator url is empty, try to use file://./coor_list";
    br::FLAGS_coor_url = "file://./coor_list";
  }

  butil::Status status;
  if (!br::FLAGS_coor_url.empty()) {
    std::string path = br::FLAGS_coor_url;
    path = path.replace(path.find("file://"), 7, "");
    auto addrs = br::Helper::GetAddrsFromFile(path);
    if (addrs.empty()) {
      DINGO_LOG(ERROR) << "coor_url not find addr, path=" << path;
      return -1;
    }

    std::shared_ptr<br::ServerInteraction> coordinator_interaction = std::make_shared<br::ServerInteraction>();
    if (!coordinator_interaction->Init(addrs)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --coor_url="
                       << br::FLAGS_coor_url;
      return -1;
    }

    br::InteractionManager::GetInstance().SetCoordinatorInteraction(coordinator_interaction);

    status = SetStoreInteraction();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return -1;
    }

    status = SetIndexInteraction();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return -1;
    }

    status = SetDocumentInteraction();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return -1;
    }

  } else {
    DINGO_LOG(ERROR) << "coordinator url is empty, please check parameter --coor_url=" << br::FLAGS_coor_url;
    return -1;
  }

  // command parse
  if (br::FLAGS_br_type == "backup") {
    if (br::FLAGS_br_backup_type == "full") {
      // br::BackupFull(FLAGS_backuptso_internal);
    } else {
      DINGO_LOG(ERROR) << "backup type not support, please check parameter --br_backup_type="
                       << br::FLAGS_br_backup_type;
      return -1;
    }
  } else if (br::FLAGS_br_type == "restore") {
    // TODO : restore
    DINGO_LOG(ERROR) << "br type not support, please check parameter --br_type=" << br::FLAGS_br_type;
    return -1;
  } else {
    DINGO_LOG(ERROR) << "br type not support, please check parameter --br_type=" << br::FLAGS_br_type;
    return -1;
  }

  status = ConvertBackupTsToTso(br::FLAGS_backupts, br::FLAGS_backuptso_internal);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return -1;
  }

  if (br::FLAGS_storage.empty()) {
    DINGO_LOG(ERROR) << "storage is empty, please check parameter --storage=" << br::FLAGS_storage;
    return -1;
  }

  if (std::string(br::FLAGS_storage).find("local://") == 0) {
    std::string path = br::FLAGS_storage.replace(br::FLAGS_storage.find("local://"), 7, "");
    if (!path.empty()) {
      if (path.back() == '/') {
        path.pop_back();
      }
    }

    if (path.empty()) {
      DINGO_LOG(ERROR) << "path is empty, please check parameter --storage=" << br::FLAGS_storage;
      return -1;
    }

    br::FLAGS_storage_internal = path;
  } else {
    DINGO_LOG(ERROR) << "storage not support, please check parameter --storage=" << br::FLAGS_storage;
    return -1;
  }

  // backup
  if (br::FLAGS_br_type == "backup") {
    br::BackupParams params;
    params.coor_url = br::FLAGS_coor_url;
    params.br_type = br::FLAGS_br_type;
    params.br_backup_type = br::FLAGS_br_backup_type;
    params.backupts = br::FLAGS_backupts;
    params.backuptso_internal = br::FLAGS_backuptso_internal;
    params.storage = br::FLAGS_storage;
    params.storage_internal = br::FLAGS_storage_internal;

    std::shared_ptr<br::Backup> backup = std::make_shared<br::Backup>(params);

    status = backup->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return -1;
    }
    status = backup->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return -1;
    }

    status = backup->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return -1;
    }

    DINGO_LOG(INFO) << "Backup finish";

  } else {
    DINGO_LOG(ERROR) << "br type not support, please check parameter --br_type=" << br::FLAGS_br_type;
    return -1;
  }

  return 0;
}
