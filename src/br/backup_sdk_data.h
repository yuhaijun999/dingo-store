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

#ifndef DINGODB_BR_BACKUP_SDK_DATA_H_
#define DINGODB_BR_BACKUP_SDK_DATA_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class BackupSdkData : public std::enable_shared_from_this<BackupSdkData> {
 public:
  BackupSdkData(ServerInteractionPtr coordinator_interaction, std::string backupts, int64_t backuptso_internal,
                std::string storage, std::string storage_internal);
  ~BackupSdkData();

  BackupSdkData(const BackupSdkData&) = delete;
  const BackupSdkData& operator=(const BackupSdkData&) = delete;
  BackupSdkData(BackupSdkData&&) = delete;
  BackupSdkData& operator=(BackupSdkData&&) = delete;

  std::shared_ptr<BackupSdkData> GetSelf();

  void SetRegionMap(std::shared_ptr<dingodb::pb::common::RegionMap> region_map);

  butil::Status Filter();

 protected:
 private:
  ServerInteractionPtr coordinator_interaction_;
  std::shared_ptr<dingodb::pb::common::RegionMap> region_map_;
  std::shared_ptr<std::vector<dingodb::pb::common::Region>> wait_for_handle_regions_;
  std::string backupts_;
  int64_t backuptso_internal_;
  std::string storage_;
  std::string storage_internal_;
};

}  // namespace br

#endif  // DINGODB_BR_BACKUP_SDK_DATA_H_