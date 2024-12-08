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

#include "br/backup_sql_data.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>

#include "common/helper.h"
#include "fmt/core.h"

namespace br {

BackupSqlData::BackupSqlData(ServerInteractionPtr coordinator_interaction, std::string backupts,
                             int64_t backuptso_internal, std::string storage, std::string storage_internal)
    : coordinator_interaction_(coordinator_interaction),
      backupts_(backupts),
      backuptso_internal_(backuptso_internal),
      storage_(storage),
      storage_internal_(storage_internal) {}

BackupSqlData::~BackupSqlData() = default;

std::shared_ptr<BackupSqlData> BackupSqlData::GetSelf() { return shared_from_this(); }

void BackupSqlData::SetRegionMap(std::shared_ptr<dingodb::pb::common::RegionMap> region_map) {
  region_map_ = region_map;
}

butil::Status BackupSqlData::Filter() {
  if (!wait_for_handle_regions_) {
    wait_for_handle_regions_ = std::make_shared<std::vector<dingodb::pb::common::Region>>();
  }

  for (const auto& region : region_map_->regions()) {
    // only handle executor txn region
    // remove meta(remove_region_list_) region from region_map_
    if (dingodb::Helper::IsExecutorTxn(region.definition().range().start_key())) {
      auto iter = std::find(remove_region_list_.begin(), remove_region_list_.end(), region.id());
      if (iter == remove_region_list_.end()) {
        wait_for_handle_regions_->push_back(region);
      }
    }
  }
  return butil::Status::OK();
}

butil::Status BackupSqlData::RemoveSqlMeta(std::vector<int64_t>& meta_region_list) {
  remove_region_list_ = meta_region_list;
  return butil::Status::OK();
}

}  // namespace br