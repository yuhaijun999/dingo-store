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

#ifndef DINGO_SERIAL_BASE_SCHEMA_H_
#define DINGO_SERIAL_BASE_SCHEMA_H_

#include <cstdint>

namespace dingodb {

class BaseSchema {
 protected:
  const uint8_t k_null = 0;
  const uint8_t k_not_null = 1;

 public:
  virtual ~BaseSchema() = default;
  enum Type { kBool, kInteger, kLong, kDouble, kString };
  virtual Type GetType() = 0;
  virtual bool AllowNull() = 0;
  virtual int GetLength() = 0;
  virtual bool IsKey() = 0;
  virtual int GetIndex() = 0;
};

}  // namespace dingodb

#endif