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

#include "br/utils.h"

#include <filesystem>
#include <memory>
#include <string>

#include "common/logging.h"
#include "fmt/core.h"
#include "proto/error.pb.h"

namespace br {

butil::Status Utils::FileExistsAndRegular(const std::string& file_path) {
  std::filesystem::path file_path_check(file_path);
  if (std::filesystem::exists(file_path_check)) {
    std::error_code ec;
    if (std::filesystem::is_symlink(file_path)) {
      file_path_check = std::filesystem::read_symlink(file_path);
    }
    if (!std::filesystem::is_regular_file(file_path_check, ec)) {
      std::string s = fmt::format("file_path : {} is not regular file : {} {}", file_path, ec.value(), ec.message());
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_REGULAR, s);
    }

    auto perms = std::filesystem::status(file_path_check, ec).permissions();
    if (ec) {
      std::string s = fmt::format("file_path : {} does not have owner read permission", file_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // Check if the file has owner read permission
    bool has_owner_read = (perms & std::filesystem::perms::owner_read) != std::filesystem::perms::none;
    if (!has_owner_read) {
      std::string s = fmt::format("file_path : {} does not have owner read permission", file_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // TODO : Check if the file has owner write permission
  } else {  // data_path not exist
    std::string s = fmt::format("file_path : {} not exist", file_path);
    // DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_EXIST, s);
  }
  return butil::Status::OK();
}
butil::Status Utils::DirExists(const std::string& dir_path) {
  std::filesystem::path dir_path_check(dir_path);
  if (std::filesystem::exists(dir_path_check)) {
    std::error_code ec;
    if (std::filesystem::is_symlink(dir_path)) {
      dir_path_check = std::filesystem::read_symlink(dir_path);
    }

    if (!std::filesystem::is_directory(dir_path_check, ec)) {
      std::string s = fmt::format("dir_path : {} is not directory, {}, {}", dir_path, ec.value(), ec.message());
      // DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_DIRECTORY, s);
    }
    auto perms = std::filesystem::status(dir_path_check, ec).permissions();
    if (ec) {
      std::string s = fmt::format("dir_path : {} does not have owner read permission", dir_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // Check if the file has owner read permission
    bool has_owner_read = (perms & std::filesystem::perms::owner_read) != std::filesystem::perms::none;
    if (!has_owner_read) {
      std::string s = fmt::format("dir_path : {} does not have owner read permission", dir_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // TODO : Check if the file has owner write permission
  } else {  // index_path_prefix not exist
    std::string s = fmt::format("dir_path : {} not exist", dir_path);
    // DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_EXIST, s);
  }
  return butil::Status::OK();
}

}  // namespace br