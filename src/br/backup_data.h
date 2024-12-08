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

#ifndef DINGODB_BR_BACKUP_DATA_H_
#define DINGODB_BR_BACKUP_DATA_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

class BackupData : public std::enable_shared_from_this<BackupData> {
 public:
  BackupData(ServerInteractionPtr coordinator_interaction);
  ~BackupData();

  BackupData(const BackupData&) = delete;
  const BackupData& operator=(const BackupData&) = delete;
  BackupData(BackupData&&) = delete;
  BackupData& operator=(BackupData&&) = delete;

  std::shared_ptr<BackupData> GetSelf();

  butil::Status GetAllRegionMapFromCoordinator();

  std::shared_ptr<dingodb::pb::common::RegionMap> GetRegionMap() const { return region_map_; }

 protected:
 private:
  ServerInteractionPtr coordinator_interaction_;
  std::shared_ptr<dingodb::pb::common::RegionMap> region_map_;
};

}  // namespace br

#endif  // DINGODB_BR_BACKUP_SDK_DATA_H_