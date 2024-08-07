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

#ifndef DINGODB_MVCC_READER_H_
#define DINGODB_MVCC_READER_H_

#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "engine/raw_engine.h"

namespace dingodb {

namespace mvcc {

class Reader {
 public:
  virtual ~Reader() = default;

  // key is plain key, not encode key
  // output value is plain value, not include ext field
  virtual butil::Status KvGet(const std::string& cf_name, int64_t ts, const std::string& plain_key,
                              std::string& plain_value) = 0;

  // start_key and end_key is plain key
  // output plain_kvs is plain key
  virtual butil::Status KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                               const std::string& plain_end_key, std::vector<pb::common::KeyValue>& plain_kvs) = 0;

  virtual butil::Status KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                               const std::string& plain_end_key,
                               std::function<bool(const std::string& plain_key, const std::string& value)> func) = 0;
  // start_key and end_key is user key
  virtual butil::Status KvCount(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                const std::string& plain_end_key, int64_t& count) = 0;

  virtual butil::Status KvMinKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                 const std::string& plain_end_key, std::string& plain_key) = 0;

  virtual butil::Status KvMaxKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                 const std::string& plain_end_key, std::string& plain_key) = 0;

  virtual dingodb::IteratorPtr NewIterator(const std::string& cf_name, int64_t ts, IteratorOptions options) = 0;
};
using ReaderPtr = std::shared_ptr<Reader>;

class KvReader : public Reader {
 public:
  KvReader(RawEngine::ReaderPtr reader) : reader_(reader) {}

  static std::shared_ptr<KvReader> New(RawEngine::ReaderPtr reader) { return std::make_shared<KvReader>(reader); }

  // key is plain key, not encode key
  // output value is plain value, not include ext field
  butil::Status KvGet(const std::string& cf_name, int64_t ts, const std::string& plain_key,
                      std::string& plain_value) override;

  // start_key and end_key is plain key
  // output plain_kvs is plain key
  butil::Status KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                       const std::string& end_key, std::vector<pb::common::KeyValue>& plain_kvs) override;

  butil::Status KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                       const std::string& plain_end_key,
                       std::function<bool(const std::string& plain_key, const std::string& value)> func) override;

  // start_key and end_key is plain key
  butil::Status KvCount(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                        const std::string& plain_end_key, int64_t& count) override;

  butil::Status KvMinKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                         const std::string& plain_end_key, std::string& plain_key) override;

  butil::Status KvMaxKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                         const std::string& plain_end_key, std::string& plain_key) override;

  dingodb::IteratorPtr NewIterator(const std::string& cf_name, int64_t ts, IteratorOptions options) override;

 private:
  RawEngine::ReaderPtr reader_;
};

class VectorReader : public Reader {
 public:
  VectorReader(RawEngine::ReaderPtr reader) : reader_(reader) {}

  static std::shared_ptr<VectorReader> New(RawEngine::ReaderPtr reader) {
    return std::make_shared<VectorReader>(reader);
  }

  // key is plain key, not encode key
  // output value is plain value, not include ext field
  butil::Status KvGet(const std::string& cf_name, int64_t ts, const std::string& plain_key,
                      std::string& plain_value) override;

  // start_key and end_key is plain key
  // output plain_kvs is plain key
  butil::Status KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                       const std::string& plain_end_key, std::vector<pb::common::KeyValue>& plain_kvs) override;

  butil::Status KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                       const std::string& plain_end_key,
                       std::function<bool(const std::string& plain_key, const std::string& value)> func) override;

  // start_key and end_key is plain key
  butil::Status KvCount(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                        const std::string& plain_end_key, int64_t& count) override;

  butil::Status KvMinKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                         const std::string& plain_end_key, std::string& plain_key) override;

  butil::Status KvMaxKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                         const std::string& plain_end_key, std::string& plain_key) override;

  dingodb::IteratorPtr NewIterator(const std::string& cf_name, int64_t ts, IteratorOptions options) override;

 private:
  RawEngine::ReaderPtr reader_;
};

class DocumentReader : public Reader {
 public:
  DocumentReader(RawEngine::ReaderPtr reader) : reader_(reader) {}

  static std::shared_ptr<DocumentReader> New(RawEngine::ReaderPtr reader) {
    return std::make_shared<DocumentReader>(reader);
  }

  // key is plain key, not encode key
  // output value is plain value, not include ext field
  butil::Status KvGet(const std::string& cf_name, int64_t ts, const std::string& plain_key,
                      std::string& plain_value) override;

  // start_key and end_key is plain key
  // output plain_kvs is plain key
  butil::Status KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                       const std::string& plain_end_key, std::vector<pb::common::KeyValue>& plain_kvs) override;

  butil::Status KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                       const std::string& plain_end_key,
                       std::function<bool(const std::string& plain_key, const std::string& value)> func) override;

  // start_key and end_key is plain key
  butil::Status KvCount(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                        const std::string& plain_end_key, int64_t& count) override;

  butil::Status KvMinKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                         const std::string& plain_end_key, std::string& plain_key) override;

  butil::Status KvMaxKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                         const std::string& plain_end_key, std::string& plain_key) override;

  dingodb::IteratorPtr NewIterator(const std::string& cf_name, int64_t ts, IteratorOptions options) override;

 private:
  RawEngine::ReaderPtr reader_;
};

}  // namespace mvcc

}  // namespace dingodb

#endif  // DINGODB_MVCC_READER_H_