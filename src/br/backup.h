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

#ifndef DINGODB_BR_BACKUP_H_
#define DINGODB_BR_BACKUP_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "br/parameter.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class Backup : public std::enable_shared_from_this<Backup> {
 public:
  Backup(const BackupParams& params);
  ~Backup();

  Backup(const Backup&) = delete;
  const Backup& operator=(const Backup&) = delete;
  Backup(Backup&&) = delete;
  Backup& operator=(Backup&&) = delete;

  std::shared_ptr<Backup> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  butil::Status ParamsCheck();
  butil::Status ParamsCheckForStorage();
  butil::Status GetGcSafePoint();
  butil::Status SetGcStop();
  butil::Status SetGcStart();
  // butil::Status RegisterBackupToCoordination();
  std::string coor_url_;
  std::string br_type_;
  std::string br_backup_type_;
  std::string backupts_;
  int64_t backuptso_internal_;
  std::string storage_;
  std::string storage_internal_;

  // gc is stop or not
  bool is_gc_stop_ = false;

  // gc is enable after finish or not
  bool is_gc_enable_after_finish_ = true;

  // notify other threads to exit
  std::atomic<bool> is_need_exit_;
};

}  // namespace br

#endif  // DINGODB_BR_BACKUP_H_