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

#include "br/backup_meta_base.h"

#include <cstdint>
#include <memory>
#include <string>

#include "br/helper.h"
#include "br/sst_file_writer.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

BackupMetaBase::BackupMetaBase(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,

                               std::string backupts, int64_t backuptso_internal, std::string storage,
                               std::string storage_internal)
    : coordinator_interaction_(coordinator_interaction),
      store_interaction_(store_interaction),
      backupts_(backupts),
      backuptso_internal_(backuptso_internal),
      storage_(storage),
      storage_internal_(storage_internal),
      is_need_exit_(false),
      already_handle_store_regions_(0) {}

BackupMetaBase::~BackupMetaBase() = default;

void BackupMetaBase::SetRegionMap(std::shared_ptr<dingodb::pb::common::RegionMap> region_map) {
  region_map_ = region_map;
}

butil::Status BackupMetaBase::Filter() {
  if (!wait_for_handle_store_regions_) {
    wait_for_handle_store_regions_ = std::make_shared<std::vector<dingodb::pb::common::Region>>();
  }

  for (const auto& region : region_map_->regions()) {
    // only handle executor meta region
    if (dingodb::Helper::IsExecutorTxn(region.definition().range().start_key())) {
      auto iter = std::find(meta_region_list_.begin(), meta_region_list_.end(), region.id());
      if (iter != meta_region_list_.end()) {
        if (dingodb::pb::common::RegionType::STORE_REGION == region.region_type()) {
          wait_for_handle_store_regions_->push_back(region);
        }
      }
    }
  }
  return butil::Status::OK();
}

butil::Status BackupMetaBase::ReserveSqlMeta(std::vector<int64_t>& meta_region_list) {
  meta_region_list_ = meta_region_list;
  return butil::Status::OK();
}

butil::Status BackupMetaBase::Run() {
  butil::Status status;
  if (!save_store_region_map_) {
    save_store_region_map_ =
        std::make_shared<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>>();
  }

  int64_t total_store_regions_count = wait_for_handle_store_regions_->size();

  while (!is_need_exit_) {
    if (already_handle_store_regions_ >= total_store_regions_count) {
      break;
    }

    double percent = static_cast<double>(already_handle_store_regions_) / total_store_regions_count;

    // store
    status = DoBackupRegionInternal(store_interaction_, "StoreService", wait_for_handle_store_regions_,
                                    already_handle_store_regions_, save_store_region_map_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    // TODO: print progress
    sleep(1);
  }

  return butil::Status::OK();
}

butil::Status BackupMetaBase::Backup() {
  butil::Status status;

  if (!backup_meta_base_) {
    backup_meta_base_ = std::make_shared<std::vector<dingodb::pb::common::BackupMeta>>();
  }

  status = BackupRegion();
  if (!status.ok()) {
    is_need_exit_ = true;
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  status = BackupCfSstMeta();
  if (!status.ok()) {
    is_need_exit_ = true;
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

std::shared_ptr<std::vector<dingodb::pb::common::BackupMeta>> BackupMetaBase::GetBackupMeta() {
  return backup_meta_base_;
}

butil::Status BackupMetaBase::BackupRegion() {
  butil::Status status;

  std::string file_name;

  file_name = storage_internal_ + "/" + dingodb::Constant::kStoreRegionSqlMetaSstName;

  status = DoBackupRegion(wait_for_handle_store_regions_, file_name);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status BackupMetaBase::BackupCfSstMeta() {
  butil::Status status;
  std::string file_name;

  file_name = storage_internal_ + "/" + dingodb::Constant::kStoreCfSstMetaSqlMetaSstName;
  status = DoBackupCfSstMeta(save_store_region_map_, file_name);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status BackupMetaBase::DoBackupRegion(
    std::shared_ptr<std::vector<dingodb::pb::common::Region>> wait_for_handle_regions, const std::string& file_name) {
  butil::Status status;

  std::string file_path = storage_internal_ + "/" + file_name;

  if (!wait_for_handle_regions->empty()) {
    std::vector<dingodb::pb::common::KeyValue> kvs;
    for (const auto& region : *wait_for_handle_regions) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key(std::to_string(region.id()));
      kv.set_value(region.SerializeAsString());
      kvs.push_back(std::move(kv));
    }

    rocksdb::Options options;
    std::shared_ptr<SstFileWriter> sst = std::make_shared<SstFileWriter>(options);

    status = sst->SaveFile(kvs, file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    save_region_files_.push_back(file_path);

    std::string hash_code;
    status = dingodb::Helper::CalSha1CodeWithFileEx(file_path, hash_code);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    dingodb::pb::common::BackupMeta backup_meta;

    backup_meta.set_remark(
        "executor sql create tenants, schema, table, index etc. meta info. save dingodb::pb::common::Region. ");
    backup_meta.set_exec_node(dingodb::Constant::kCoordinatorRegionName);
    backup_meta.set_dir_name("");
    backup_meta.set_file_size(sst->GetSize());
    backup_meta.set_encryption(hash_code);
    backup_meta.set_file_name(file_name);

    backup_meta_base_->push_back(std::move(backup_meta));
  }
  return butil::Status::OK();
}

butil::Status BackupMetaBase::DoBackupCfSstMeta(
    std::shared_ptr<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>> save_region_map,
    const std::string& file_name) {
  butil::Status status;

  if (!save_region_map->empty()) {
    std::vector<dingodb::pb::common::KeyValue> kvs;
    for (const auto& [region_id, group] : *save_region_map) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key(std::to_string(region_id));
      kv.set_value(group.SerializeAsString());
      kvs.push_back(std::move(kv));
    }

    rocksdb::Options options;
    std::shared_ptr<SstFileWriter> sst = std::make_shared<SstFileWriter>(options);

    status = sst->SaveFile(kvs, file_name);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    save_cf_sst_meta_files_.push_back(file_name);

    std::string hash_code;
    status = dingodb::Helper::CalSha1CodeWithFileEx(file_name, hash_code);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    dingodb::pb::common::BackupMeta backup_meta;

    backup_meta.set_remark(
        "executor sql create tenants, schema, table, index etc. meta info. save "
        "dingodb::pb::common::BackupDataFileValueSstMetaGroup.");
    backup_meta.set_exec_node(dingodb::Constant::kStoreRegionName);
    backup_meta.set_dir_name("");
    backup_meta.set_file_size(sst->GetSize());
    backup_meta.set_encryption(hash_code);
    backup_meta.set_file_name(file_name);

    backup_meta_base_->push_back(std::move(backup_meta));
  }
  return butil::Status::OK();
}

butil::Status BackupMetaBase::DoBackupRegionInternal(
    ServerInteractionPtr interaction, const std::string& service_name,
    std::shared_ptr<std::vector<dingodb::pb::common::Region>> wait_for_handle_regions,
    std::atomic<int64_t>& already_handle_regions,
    std::shared_ptr<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>> save_region_map) {
  for (const auto& region : *wait_for_handle_regions) {
    if (is_need_exit_) {
      break;
    }
    dingodb::pb::store::BackupMetaRequest request;
    dingodb::pb::store::BackupMetaResponse response;

    request.mutable_request_info()->set_request_id(Helper::GetRandInt());
    request.mutable_context()->set_region_id(region.id());
    request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
    request.set_start_key(region.definition().range().start_key());
    request.set_end_key(region.definition().range().end_key());
    request.set_need_leader(true);
    request.set_region_type(region.region_type());
    request.set_backup_ts(backupts_);
    request.set_backup_tso(backuptso_internal_);
    request.set_storage_path(storage_);
    request.mutable_storage_backend()->mutable_local()->set_path(storage_internal_);

    butil::Status status = interaction->SendRequest(service_name, "BackupMeta", request, response);
    if (!status.ok()) {
      is_need_exit_ = true;
      std::string s = fmt::format("Fail to backup region, region_id={}, status={}", region.id(), status.error_cstr());
      DINGO_LOG(ERROR) << s;
      return status;
    }

    if (response.error().errcode() != dingodb::pb::error::OK) {
      is_need_exit_ = true;
      std::string s =
          fmt::format("Fail to backup region, region_id={}, error={}", region.id(), response.error().errmsg());
      DINGO_LOG(ERROR) << s;
      return butil::Status(response.error().errcode(), s);
    }

    save_region_map->insert({region.id(), response.sst_metas()});

    already_handle_regions++;
  }
  return butil::Status::OK();
}

}  // namespace br