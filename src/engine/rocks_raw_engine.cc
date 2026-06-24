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

#include "engine/rocks_raw_engine.h"

#include <elf.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "config/config_helper.h"
#include "engine/mvcc_properties_collector.h"
#include "engine/raw_engine.h"
#include "engine/snapshot.h"
#include "engine/write_compaction_filter.h"
#include "metrics/rocksdb_event_listener.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/write_batch.h"

namespace dingodb {
DEFINE_bool(enable_rocksdb_sync, false, "enable rocksdb sync");

// [GC-Tombstone baseline step-3] periodic_compaction_seconds for the txn write CF, used to feed cold
// SSTs through the compaction filter (step-2) periodically. Semantics: 0 means "do not override
// RocksDB's own default" (i.e. the feature is OFF and behavior is unchanged); a value > 0 enables it
// and is used as the period in seconds. The full GC tombstone benefit needs this so that versions in
// cold / no-longer-written regions still get evaporated by the filter. For perf testing set it to a
// few minutes (e.g. --gc_periodic_compaction_seconds=300).
DEFINE_int64(gc_periodic_compaction_seconds, 0,
             "periodic_compaction_seconds for the txn write CF (0 = off / do not override RocksDB "
             "default); feeds cold SSTs through the compaction filter");

// [GC-Tombstone baseline step-7] Use DeleteFilesInRange (file-level SST drop, 0 range tombstone) plus a
// per-key fallback for large-range deletes (drop region / TxnDeleteRange / snapshot cleanup), instead of
// RocksDB DeleteRange (which writes a range tombstone that slows every scan crossing the range). Default
// OFF (false) = keep the current DeleteRange behavior byte-for-byte. The lock CF ALWAYS uses DeleteRange
// regardless of this flag (a stray file-level drop of lock records is unsafe -- see DoDeleteRangeOneCf).
DEFINE_bool(gc_use_delete_files_in_range, false,
            "use DeleteFilesInRange + per-key fallback for large-range deletes (false = keep DeleteRange); "
            "the lock CF always uses DeleteRange regardless of this flag");

namespace rocks {

ColumnFamily::ColumnFamily(const std::string& cf_name, const ColumnFamilyConfig& config,
                           rocksdb::ColumnFamilyHandle* handle)
    : name_(cf_name), config_(config), handle_(handle) {}

ColumnFamily::ColumnFamily(const std::string& cf_name, const ColumnFamilyConfig& config)
    : ColumnFamily(cf_name, config, nullptr) {}

ColumnFamily::~ColumnFamily() {
  delete handle_;
  handle_ = nullptr;
}

void ColumnFamily::SetConfItem(const std::string& name, const std::string& value) {
  auto it = config_.find(name);
  if (it == config_.end()) {
    config_.insert(std::make_pair(name, value));
  } else {
    it->second = value;
  }
}

std::string ColumnFamily::GetConfItem(const std::string& name) {
  auto it = config_.find(name);
  return it == config_.end() ? "" : it->second;
}

void ColumnFamily::Dump() {
  for (const auto& [name, value] : config_) {
    DINGO_LOG(INFO) << fmt::format("[rocksdb.dump][column_family({})] {} : {}", Name(), name, value);
  }

  DINGO_LOG(INFO) << fmt::format("[rocksdb.dump][column_family({})] end.....................", Name());
}

bool Iterator::Valid() const {
  if (!iter_->Valid()) {
    return false;
  }

  if (!options_->upper_bound.empty()) {
    auto upper_bound = rocksdb::Slice(options_->upper_bound);
    if (upper_bound.compare(iter_->key()) <= 0) {
      return false;
    }
  }
  if (!options_->lower_bound.empty()) {
    auto lower_bound = rocksdb::Slice(options_->lower_bound);
    if (lower_bound.compare(iter_->key()) > 0) {
      return false;
    }
  }

  return true;
}

butil::Status Iterator::Status() const {
  if (iter_->status().ok()) {
    return butil::Status();
  }
  return butil::Status(pb::error::EINTERNAL, "Internal iterator error");
}

butil::Status SstFileWriter::SaveFile(const std::map<std::string, std::string>& kvs, const std::string& filename) {
  auto status = sst_writer_->Open(filename);
  if (!status.ok()) {
    return butil::Status(status.code(), status.ToString());
  }

  for (const auto& [key, value] : kvs) {
    status = sst_writer_->Put(key, value);
    if (!status.ok()) {
      return butil::Status(status.code(), status.ToString());
    }
  }

  status = sst_writer_->Finish();
  if (!status.ok()) {
    return butil::Status(status.code(), status.ToString());
  }

  return butil::Status();
}

butil::Status SstFileWriter::SaveFile(const std::vector<pb::common::KeyValue>& kvs, const std::string& filename) {
  auto status = sst_writer_->Open(filename);
  if (!status.ok()) {
    return butil::Status(status.code(), status.ToString());
  }

  for (const auto& kv : kvs) {
    status = sst_writer_->Put(kv.key(), kv.value());
    if (!status.ok()) {
      return butil::Status(status.code(), status.ToString());
    }
  }

  status = sst_writer_->Finish();
  if (!status.ok()) {
    return butil::Status(status.code(), status.ToString());
  }

  return butil::Status();
}

butil::Status SstFileWriter::SaveFile(std::shared_ptr<dingodb::Iterator> iter, const std::string& filename) {
  auto status = sst_writer_->Open(filename);
  if (!status.ok()) {
    return butil::Status(status.code(), status.ToString());
  }

  for (; iter->Valid(); iter->Next()) {
    status = sst_writer_->Put(iter->Key(), iter->Value());
    if (!status.ok()) {
      sst_writer_->Finish();
      return butil::Status(status.code(), status.ToString());
    }
  }

  status = sst_writer_->Finish();
  if (!status.ok()) {
    return butil::Status((status.code() == rocksdb::Status::Code::kInvalidArgument &&
                          status.ToString().find("no entries") != std::string::npos)
                             ? pb::error::ENO_ENTRIES
                             : static_cast<int>(status.code()),
                         status.ToString());
  }

  return butil::Status();
}

std::shared_ptr<RocksRawEngine> Checkpoint::GetRawEngine() {
  auto raw_engine = raw_engine_.lock();
  if (raw_engine == nullptr) {
    DINGO_LOG(FATAL) << "[rocksdb] get raw engine failed.";
  }

  return raw_engine;
}

std::shared_ptr<rocksdb::DB> Checkpoint::GetDB() { return GetRawEngine()->GetDB(); }
std::vector<rocks::ColumnFamilyPtr> Checkpoint::GetColumnFamilies(const std::vector<std::string>& cf_names) {
  return GetRawEngine()->GetColumnFamilies(cf_names);
}

butil::Status Checkpoint::Create(const std::string& dirpath) {
  // std::unique_ptr<rocksdb::Checkpoint> checkpoint = std::make_unique<rocksdb::Checkpoint>();
  rocksdb::Checkpoint* checkpoint = nullptr;
  auto status = rocksdb::Checkpoint::Create(GetDB().get(), &checkpoint);
  if (!status.ok()) {
    delete checkpoint;
    return butil::Status(status.code(), status.ToString());
  }

  status = checkpoint->CreateCheckpoint(dirpath);
  if (!status.ok()) {
    delete checkpoint;
    return butil::Status(status.code(), status.ToString());
  }

  delete checkpoint;
  return butil::Status();
}

butil::Status Checkpoint::Create(const std::string& dirpath, const std::vector<std::string>& cf_names,
                                 std::vector<pb::store_internal::SstFileInfo>& sst_files) {
  rocksdb::Checkpoint* checkpoint = nullptr;
  auto status = rocksdb::Checkpoint::Create(GetDB().get(), &checkpoint);
  if (!status.ok()) {
    delete checkpoint;
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] create checkpoint failed, error: {}.", status.ToString());
    return butil::Status(status.code(), status.ToString());
  }

  status = GetDB()->DisableFileDeletions();
  if (!status.ok()) {
    delete checkpoint;
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] disable file deletion failed, error: {}.", status.ToString());
    return butil::Status(status.code(), status.ToString());
  }

  status = checkpoint->CreateCheckpoint(dirpath);
  if (!status.ok()) {
    GetDB()->EnableFileDeletions();
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] export column family checkpoint failed, error: {}.", status.ToString());
    delete checkpoint;
    return butil::Status(status.code(), status.ToString());
  }

  std::vector<rocksdb::ColumnFamilyMetaData> meta_datas;

  auto column_families = GetColumnFamilies(cf_names);
  for (const auto& column_family : column_families) {
    rocksdb::ColumnFamilyMetaData meta_data;
    GetDB()->GetColumnFamilyMetaData(column_family->GetHandle(), &meta_data);
    meta_datas.push_back(meta_data);
  }

  status = GetDB()->EnableFileDeletions();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] enable file deletion failed, error: {}.", status.ToString());
    return butil::Status(status.code(), status.ToString());
  }

  if (column_families.size() != meta_datas.size()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] column_families.size() != meta_datas.size()") << column_families.size()
                     << " != " << meta_datas.size();
    return butil::Status(pb::error::EINTERNAL, "Internal error");
  }

  for (int i = 0; i < column_families.size(); i++) {
    auto& meta_data = meta_datas[i];
    auto& column_family = column_families[i];

    for (auto& level : meta_data.levels) {
      for (const auto& file : level.files) {
        std::string filepath = dirpath + file.name;
        if (!Helper::IsExistPath(filepath)) {
          DINGO_LOG(INFO) << fmt::format("[rocksdb] checkpoint not contain sst file: {}", filepath);
          continue;
        }

        pb::store_internal::SstFileInfo sst_file;
        sst_file.set_level(level.level);
        sst_file.set_name(file.name);
        sst_file.set_path(filepath);
        sst_file.set_start_key(file.smallestkey);
        sst_file.set_end_key(file.largestkey);
        sst_file.set_cf_name(column_family->Name());

        DINGO_LOG(DEBUG) << "checkpoint add sst_file: " << sst_file.ShortDebugString();

        sst_files.emplace_back(std::move(sst_file));
      }
    }
  }

  pb::store_internal::SstFileInfo sst_file;
  sst_file.set_level(-1);
  sst_file.set_name("CURRENT");
  sst_file.set_path(dirpath + "/CURRENT");
  sst_files.push_back(sst_file);

  std::string manifest_name = Helper::FindFileInDirectory(dirpath, "MANIFEST");
  sst_file.set_level(-1);
  sst_file.set_name(manifest_name);
  sst_file.set_path(dirpath + "/" + manifest_name);
  sst_files.push_back(sst_file);

  std::string options_name = Helper::FindFileInDirectory(dirpath, "OPTIONS");
  sst_file.set_level(-1);
  sst_file.set_name(options_name);
  sst_file.set_path(dirpath + "/" + options_name);
  sst_files.push_back(sst_file);

  delete checkpoint;
  return butil::Status();
}

std::shared_ptr<RocksRawEngine> Reader::GetRawEngine() {
  auto raw_engine = raw_engine_.lock();
  if (raw_engine == nullptr) {
    DINGO_LOG(FATAL) << "[rocksdb] get raw engine failed.";
  }

  return raw_engine;
}

dingodb::SnapshotPtr Reader::GetSnapshot() { return GetRawEngine()->GetSnapshot(); }

std::shared_ptr<rocksdb::DB> Reader::GetDB() { return GetRawEngine()->GetDB(); }

ColumnFamilyPtr Reader::GetColumnFamily(const std::string& cf_name) { return GetRawEngine()->GetColumnFamily(cf_name); }

ColumnFamilyPtr Reader::GetDefaultColumnFamily() { return GetRawEngine()->GetDefaultColumnFamily(); }

butil::Status Reader::KvGet(const std::string& cf_name, const std::string& key, std::string& value) {
  return KvGet(GetColumnFamily(cf_name), GetSnapshot(), key, value);
}

butil::Status Reader::KvGet(const std::string& cf_name, std::shared_ptr<dingodb::Snapshot> snapshot,
                            const std::string& key, std::string& value) {
  auto column_family = GetColumnFamily(cf_name);
  return KvGet(column_family, snapshot, key, value);
}

butil::Status Reader::KvGet(ColumnFamilyPtr column_family, dingodb::SnapshotPtr snapshot, const std::string& key,
                            std::string& value) {
  if (BAIDU_UNLIKELY(key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  rocksdb::ReadOptions read_option;
  read_option.snapshot = static_cast<const rocksdb::Snapshot*>(snapshot->Inner());
  rocksdb::Status s = GetDB()->Get(read_option, column_family->GetHandle(), rocksdb::Slice(key), &value);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found key");
    }
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] get key failed, error: {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal get error");
  }

  return butil::Status();
}

butil::Status Reader::KvScan(ColumnFamilyPtr column_family, std::shared_ptr<dingodb::Snapshot> snapshot,
                             const std::string& start_key, const std::string& end_key,
                             std::vector<pb::common::KeyValue>& kvs) {
  if (BAIDU_UNLIKELY(start_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty start_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  if (BAIDU_UNLIKELY(end_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty end_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  rocksdb::ReadOptions read_option;
  read_option.auto_prefix_mode = true;
  read_option.async_io = true;
  read_option.adaptive_readahead = true;
  read_option.snapshot = static_cast<const rocksdb::Snapshot*>(snapshot->Inner());
  rocksdb::Slice upper_bound(end_key);
  read_option.iterate_upper_bound = &upper_bound;

  std::string_view end_key_view(end_key);
  rocksdb::Iterator* it = GetDB()->NewIterator(read_option, column_family->GetHandle());
  for (it->Seek(start_key); it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
    pb::common::KeyValue kv;
    kv.set_key(it->key().data(), it->key().size());
    kv.set_value(it->value().data(), it->value().size());

    kvs.emplace_back(std::move(kv));
  }
  delete it;

  return butil::Status();
}

butil::Status Reader::KvScan(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                             std::vector<pb::common::KeyValue>& kvs) {
  return KvScan(GetColumnFamily(cf_name), GetSnapshot(), start_key, end_key, kvs);
}

butil::Status Reader::KvScan(const std::string& cf_name, std::shared_ptr<dingodb::Snapshot> snapshot,
                             const std::string& start_key, const std::string& end_key,
                             std::vector<pb::common::KeyValue>& kvs) {
  return KvScan(GetColumnFamily(cf_name), snapshot, start_key, end_key, kvs);
}

butil::Status Reader::KvCount(ColumnFamilyPtr column_family, dingodb::SnapshotPtr snapshot,
                              const std::string& start_key, const std::string& end_key, int64_t& count) {
  if (BAIDU_UNLIKELY(start_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty start_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  if (BAIDU_UNLIKELY(end_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty end_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  rocksdb::ReadOptions read_options;
  read_options.auto_prefix_mode = true;
  read_options.async_io = true;
  read_options.adaptive_readahead = true;
  read_options.snapshot = static_cast<const rocksdb::Snapshot*>(snapshot->Inner());
  rocksdb::Slice upper_bound(end_key);
  read_options.iterate_upper_bound = &upper_bound;

  std::string_view end_key_view(end_key.data(), end_key.size());
  rocksdb::Iterator* it = GetDB()->NewIterator(read_options, column_family->GetHandle());
  for (it->Seek(start_key), count = 0; it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
    ++count;
  }
  delete it;

  return butil::Status();
}

butil::Status Reader::KvCount(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                              int64_t& count) {
  return KvCount(GetColumnFamily(cf_name), GetSnapshot(), start_key, end_key, count);
}

butil::Status Reader::KvCount(const std::string& cf_name, dingodb::SnapshotPtr snapshot, const std::string& start_key,
                              const std::string& end_key, int64_t& count) {
  return KvCount(GetColumnFamily(cf_name), snapshot, start_key, end_key, count);
}

dingodb::IteratorPtr Reader::NewIterator(ColumnFamilyPtr column_family, dingodb::SnapshotPtr snapshot,
                                         IteratorOptions options) {
  IteratorOptionsPtr inner_option = std::make_shared<IteratorOptions>(options.lower_bound, options.upper_bound);

  rocksdb::ReadOptions read_options;
  if (snapshot != nullptr) {
    read_options.snapshot = static_cast<const rocksdb::Snapshot*>(snapshot->Inner());
  }
  read_options.auto_prefix_mode = true;
  read_options.async_io = true;
  read_options.adaptive_readahead = true;
  if (!inner_option->upper_bound.empty()) {
    inner_option->extension = new rocksdb::Slice(inner_option->upper_bound);
    read_options.iterate_upper_bound = (rocksdb::Slice*)inner_option->extension;
  }

  return std::make_shared<Iterator>(inner_option, GetDB()->NewIterator(read_options, column_family->GetHandle()),
                                    snapshot);
}

dingodb::IteratorPtr Reader::NewIterator(const std::string& cf_name, IteratorOptions options) {
  return NewIterator(GetColumnFamily(cf_name), GetSnapshot(), options);
}

dingodb::IteratorPtr Reader::NewIterator(const std::string& cf_name, dingodb::SnapshotPtr snapshot,
                                         IteratorOptions options) {
  return NewIterator(GetColumnFamily(cf_name), snapshot, options);
}

std::shared_ptr<RocksRawEngine> Writer::GetRawEngine() {
  auto raw_engine = raw_engine_.lock();
  if (raw_engine == nullptr) {
    DINGO_LOG(FATAL) << "[rocksdb] get raw engine failed.";
  }

  return raw_engine;
}

std::shared_ptr<rocksdb::DB> Writer::GetDB() { return GetRawEngine()->GetDB(); }

ColumnFamilyPtr Writer::GetColumnFamily(const std::string& cf_name) { return GetRawEngine()->GetColumnFamily(cf_name); }

ColumnFamilyPtr Writer::GetDefaultColumnFamily() { return GetRawEngine()->GetDefaultColumnFamily(); }

butil::Status Writer::KvPut(const std::string& cf_name, const pb::common::KeyValue& kv) {
  if (BAIDU_UNLIKELY(kv.key().empty())) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  rocksdb::WriteOptions write_options;
  write_options.sync = FLAGS_enable_rocksdb_sync;

  rocksdb::Status s = GetDB()->Put(write_options, GetColumnFamily(cf_name)->GetHandle(), rocksdb::Slice(kv.key()),
                                   rocksdb::Slice(kv.value()));
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] put failed, error: {}.", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal put error");
  }

  return butil::Status();
}

butil::Status Writer::KvBatchPut(const std::string& cf_name, const std::vector<pb::common::KeyValue>& kvs) {
  if (BAIDU_UNLIKELY(kvs.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty keys.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  auto column_family = GetColumnFamily(cf_name);

  rocksdb::WriteBatch batch;
  for (const auto& kv : kvs) {
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty key.");
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    } else {
      rocksdb::Status s = batch.Put(column_family->GetHandle(), kv.key(), kv.value());
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("[rocksdb] batch put failed, error: {}.", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal put error");
      }
    }
  }

  rocksdb::WriteOptions write_options;
  write_options.sync = FLAGS_enable_rocksdb_sync;

  rocksdb::Status s = GetDB()->Write(write_options, &batch);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] write failed, error: {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal write error");
  }

  return butil::Status();
}

butil::Status Writer::KvBatchPutAndDelete(const std::string& cf_name,
                                          const std::vector<pb::common::KeyValue>& kvs_to_put,
                                          const std::vector<std::string>& keys_to_delete) {
  if (BAIDU_UNLIKELY(kvs_to_put.empty() && keys_to_delete.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty keys.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  auto column_family = GetColumnFamily(cf_name);

  rocksdb::WriteBatch batch;
  for (const auto& kv : kvs_to_put) {
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty key.");
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    } else {
      rocksdb::Status s = batch.Put(column_family->GetHandle(), kv.key(), kv.value());
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("[rocksdb] batch put failed, error: {}.", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal put error");
      }
    }
  }

  for (const auto& key : keys_to_delete) {
    if (BAIDU_UNLIKELY(key.empty())) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty key.");
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    } else {
      rocksdb::Status s = batch.Delete(column_family->GetHandle(), key);
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("[rocksdb] batch delete failed, error: {}.", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal delete error");
      }
    }
  }
  rocksdb::WriteOptions write_options;
  write_options.sync = FLAGS_enable_rocksdb_sync;

  rocksdb::Status s = GetDB()->Write(write_options, &batch);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] write failed, error: {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal write error");
  }

  return butil::Status();
}

butil::Status Writer::KvBatchPutAndDelete(
    const std::map<std::string, std::vector<pb::common::KeyValue>>& kv_puts_with_cf,
    const std::map<std::string, std::vector<std::string>>& kv_deletes_with_cf) {
  DINGO_LOG(DEBUG) << fmt::format("[rocksdb] KvBatchPutAndDelete put kv size: {} delete kv size: {}",
                                  kv_puts_with_cf.size(), kv_deletes_with_cf.size());

  rocksdb::WriteBatch batch;
  for (const auto& [cf_name, kv_puts] : kv_puts_with_cf) {
    if (BAIDU_UNLIKELY(kv_puts.empty())) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] keys empty not support");
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    auto column_family = GetColumnFamily(cf_name);
    for (const auto& kv : kv_puts) {
      if (BAIDU_UNLIKELY(kv.key().empty())) {
        DINGO_LOG(ERROR) << fmt::format("[rocksdb] key empty not support");
        return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
      }

      rocksdb::Status s = batch.Put(column_family->GetHandle(), kv.key(), kv.value());
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("[rocksdb] put failed, error: {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal put error");
      }
    }
  }

  for (const auto& [cf_name, kv_deletes] : kv_deletes_with_cf) {
    if (BAIDU_UNLIKELY(kv_deletes.empty())) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] keys empty not support");
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    auto column_family = GetColumnFamily(cf_name);
    for (const auto& key : kv_deletes) {
      if (BAIDU_UNLIKELY(key.empty())) {
        DINGO_LOG(ERROR) << fmt::format("[rocksdb] key empty not support");
        return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
      }

      rocksdb::Status s = batch.Delete(column_family->GetHandle(), key);
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("[rocksdb] put failed, error: {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal delete error");
      }
    }
  }

  rocksdb::WriteOptions write_options;
  write_options.sync = FLAGS_enable_rocksdb_sync;

  rocksdb::Status s = GetDB()->Write(write_options, &batch);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] write failed, error: {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, fmt::format("rocksdb::DB::Write failed : {}", s.ToString()));
  }

  return butil::Status::OK();
}

butil::Status Writer::KvDelete(const std::string& cf_name, const std::string& key) {
  if (BAIDU_UNLIKELY(key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] not support empty key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  rocksdb::WriteOptions write_options;
  write_options.sync = FLAGS_enable_rocksdb_sync;

  rocksdb::Status const s =
      GetDB()->Delete(write_options, GetColumnFamily(cf_name)->GetHandle(), rocksdb::Slice(key.data(), key.size()));
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] delete failed, error: {}.", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal delete error");
  }

  return butil::Status();
}

// [GC-Tombstone baseline step-7] Delete one (cf, range). Pre-conditions: the range is already validated
// (non-empty start/end, start < end) and start/end are already-encoded physical keys. The range is
// right-open [start, end).
//
// When gc_use_delete_files_in_range is OFF, or the CF is the lock CF, this keeps the original behavior
// (a single DeleteRange committed in a WriteBatch). Otherwise it uses DeleteFilesInRange (file-level
// whole-SST drop, no range tombstone) plus a per-key fallback that scans the leftover keys -- from L0
// files and partially-overlapping SSTs, which DeleteFilesInRange does NOT remove -- and point-deletes
// them, so the range ends up fully cleared.
//
// The lock CF is intentionally excluded: its records carry transaction locks, and a file-level drop
// (which leaves L0 / partial-overlap residue until the fallback runs) is unsafe for lock correctness, so
// the lock CF always uses DeleteRange regardless of the flag.
static butil::Status DoDeleteRangeOneCf(const std::shared_ptr<rocksdb::DB>& db, rocksdb::ColumnFamilyHandle* handle,
                                        const std::string& cf_name, const pb::common::Range& range) {
  rocksdb::WriteOptions write_options;
  write_options.sync = FLAGS_enable_rocksdb_sync;

  // Original path: DeleteRange. Used when the feature is off, or for the lock CF (always).
  if (!FLAGS_gc_use_delete_files_in_range || cf_name == Constant::kTxnLockCF) {
    rocksdb::WriteBatch batch;
    rocksdb::Status s = batch.DeleteRange(handle, range.start_key(), range.end_key());
    if (!s.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] delete range failed, error: {}.", s.ToString());
      return butil::Status(pb::error::EINTERNAL, "Internal delete range error");
    }
    s = db->Write(write_options, &batch);
    if (!s.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] write failed, error: {}.", s.ToString());
      return butil::Status(pb::error::EINTERNAL, "Internal write error");
    }
    return butil::Status();
  }

  // DeleteFilesInRange path (non-lock CF + feature on).
  const std::string& start = range.start_key();
  const std::string& end = range.end_key();
  rocksdb::Slice begin_slice(start);
  rocksdb::Slice end_slice(end);

  // (a) File-level delete: drops only the SSTs that fall entirely inside [start, end). include_end=false
  // keeps it consistent with the right-open range semantics. It leaves L0 files and partially-overlapping
  // SSTs untouched -- those are cleaned up by the per-key fallback below.
  rocksdb::Status s =
      rocksdb::DeleteFilesInRange(db.get(), handle, &begin_slice, &end_slice, /*include_end=*/false);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] delete files in range failed, cf: {}, error: {}.", cf_name,
                                    s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal delete files in range error");
  }

  // (b) Per-key fallback: scan whatever keys are still visible in [start, end) (L0 residue, partial-overlap
  // SST residue, memtable) and point-delete them so the range is fully cleared. iterate_upper_bound
  // enforces the right-open end (key < end), matching include_end=false above; Seek(begin) includes start.
  rocksdb::ReadOptions read_options;
  read_options.fill_cache = false;  // one-shot bulk scan, do not pollute the block cache
  rocksdb::Slice upper_bound_slice(end);
  read_options.iterate_upper_bound = &upper_bound_slice;

  rocksdb::WriteBatch del_batch;
  std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read_options, handle));
  for (it->Seek(begin_slice); it->Valid(); it->Next()) {
    rocksdb::Status ds = del_batch.Delete(handle, it->key());
    if (!ds.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] fallback delete failed, cf: {}, error: {}.", cf_name,
                                      ds.ToString());
      return butil::Status(pb::error::EINTERNAL, "Internal delete range error");
    }
  }
  if (!it->status().ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] fallback iterate failed, cf: {}, error: {}.", cf_name,
                                    it->status().ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal iterate error");
  }

  if (del_batch.Count() > 0) {
    s = db->Write(write_options, &del_batch);
    if (!s.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] fallback write failed, cf: {}, error: {}.", cf_name,
                                      s.ToString());
      return butil::Status(pb::error::EINTERNAL, "Internal write error");
    }
  }

  return butil::Status();
}

butil::Status Writer::KvDeleteRange(const std::string& cf_name, const pb::common::Range& range) {
  if (range.start_key().empty() || range.end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is empty");
  }
  if (range.start_key() >= range.end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is wrong");
  }

  return DoDeleteRangeOneCf(GetDB(), GetColumnFamily(cf_name)->GetHandle(), cf_name, range);
}

butil::Status Writer::KvBatchDeleteRange(const std::map<std::string, std::vector<pb::common::Range>>& range_with_cfs) {
  // Note: with gc_use_delete_files_in_range on, DeleteFilesInRange is a direct DB operation that cannot be
  // batched into a single WriteBatch, so each (cf, range) is committed independently here (the previous
  // single-atomic-WriteBatch grouping is dropped). Callers (drop region / TxnDeleteRange / snapshot
  // cleanup) clear whole regions and do not rely on cross-CF atomicity; the operations are idempotent and
  // re-driven by raft apply / region retry. With the flag off, the visible effect matches the original
  // per-range DeleteRange (only the commit boundary changes).
  for (const auto& [cf_name, ranges] : range_with_cfs) {
    auto column_family = GetColumnFamily(cf_name);
    for (const auto& range : ranges) {
      if (range.start_key().empty() || range.end_key().empty()) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is empty");
      }
      if (range.start_key() >= range.end_key()) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is wrong");
      }

      auto status = DoDeleteRangeOneCf(GetDB(), column_family->GetHandle(), cf_name, range);
      if (!status.ok()) {
        return status;
      }
    }
  }

  return butil::Status();
}

}  // namespace rocks

RocksRawEngine::RocksRawEngine() : db_(nullptr), column_families_({}) {}

RocksRawEngine::~RocksRawEngine() = default;

static rocks::ColumnFamilyMap GenColumnFamilyByDefaultConfig(const std::vector<std::string>& column_family_names) {
  rocks::ColumnFamily::ColumnFamilyConfig default_config;
  default_config.emplace(Constant::kBlockSize, Constant::kBlockSizeDefaultValue);
  default_config.emplace(Constant::kBlockCache, ConfigHelper::GetBlockCacheValue());
  default_config.emplace(Constant::kArenaBlockSize, Constant::kArenaBlockSizeDefaultValue);
  default_config.emplace(Constant::kMinWriteBufferNumberToMerge, Constant::kMinWriteBufferNumberToMergeDefaultValue);
  default_config.emplace(Constant::kMaxWriteBufferNumber, Constant::kMaxWriteBufferNumberDefaultValue);
  default_config.emplace(Constant::kMaxCompactionBytes, Constant::kMaxCompactionBytesDefaultValue);
  default_config.emplace(Constant::kWriteBufferSize, Constant::kWriteBufferSizeDefaultValue);
  default_config.emplace(Constant::kPrefixExtractor, Constant::kPrefixExtractorDefaultValue);
  default_config.emplace(Constant::kMaxBytesForLevelBase, Constant::kMaxBytesForLevelBaseDefaultValue);
  default_config.emplace(Constant::kTargetFileSizeBase, Constant::kTargetFileSizeBaseDefaultValue);
  default_config.emplace(Constant::kMaxBytesForLevelMultiplier, Constant::kMaxBytesForLevelMultiplierDefaultValue);

  rocks::ColumnFamilyMap column_families;
  for (const auto& cf_name : column_family_names) {
    column_families.emplace(cf_name, rocks::ColumnFamily::New(cf_name, default_config));
  }

  return column_families;
}

static void SetColumnFamilyCustomConfig(const std::shared_ptr<Config>& config,
                                        rocks::ColumnFamilyMap& column_families) {
  // store.base config
  const auto base_cf_config = config->GetStringMap(Constant::kBaseColumnFamily);
  auto base_column_family_names = config->GetStringList(Constant::kColumnFamilies);
  for (const auto& cf_name : base_column_family_names) {
    auto it = column_families.find(cf_name);
    if (it == column_families.end()) {
      continue;
    }
    auto& column_family = it->second;
    for (const auto& [name, value] : base_cf_config) {
      column_family->SetConfItem(name, value);
    }
  }

  // // store.[cf_name] config
  for (auto& [cf_name, column_family] : column_families) {
    std::string config_item("store." + cf_name);
    const auto cf_config = config->GetStringMap(config_item);
    if (cf_config.empty()) {
      continue;
    }

    for (const auto& [name, value] : cf_config) {
      column_family->SetConfItem(name, value);
    }
  }
}

template <typename T>
static bool CastValue(std::string value, T& dst_value) {
  if (value.empty()) {
    DINGO_LOG(FATAL) << fmt::format("[rocksdb] value is empty.");
    return false;
  }

  try {
    if (std::is_same_v<size_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoul(value);
    } else if (std::is_same_v<int32_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoi(value);
    } else if (std::is_same_v<uint32_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoi(value);
    } else if (std::is_same_v<int64_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoll(value);
    } else if (std::is_same_v<int64_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoul(value);
    } else if (std::is_same_v<int, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoi(value);
    } else if (std::is_same_v<float, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stof(value);
    } else if (std::is_same_v<double, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stod(value);
    } else {
      DINGO_LOG(FATAL) << fmt::format("[rocksdb] not match type failed, value: {}.", value);
      return false;
    }
  } catch (const std::invalid_argument& e) {
    DINGO_LOG(FATAL) << fmt::format("[rocksdb] cast type failed, value: {} error: {}.", value, e.what());
    return false;
  } catch (const std::out_of_range& e) {
    DINGO_LOG(FATAL) << fmt::format("[rocksdb] cast type failed, value: {} error: {}.", value, e.what());
    return false;
  }

  return true;
}

template <>
bool CastValue(std::string value, std::string& dst_value) {
  dst_value = value;
  return true;
}

// set cf config
static rocksdb::ColumnFamilyOptions GenRocksDBColumnFamilyOptions(rocks::ColumnFamilyPtr column_family) {
  rocksdb::ColumnFamilyOptions family_options;
  rocksdb::BlockBasedTableOptions table_options;

  // block_size
  CastValue(column_family->GetConfItem(Constant::kBlockSize), table_options.block_size);

  // block_cache
  {
    size_t option_value = 0;
    CastValue(column_family->GetConfItem(Constant::kBlockCache), option_value);

    table_options.block_cache = rocksdb::NewLRUCache(option_value);  // LRUcache
  }

  // arena_block_size
  CastValue(column_family->GetConfItem(Constant::kArenaBlockSize), family_options.arena_block_size);

  // min_write_buffer_number_to_merge
  CastValue(column_family->GetConfItem(Constant::kMinWriteBufferNumberToMerge),
            family_options.min_write_buffer_number_to_merge);

  // max_write_buffer_number
  CastValue(column_family->GetConfItem(Constant::kMaxWriteBufferNumber), family_options.max_write_buffer_number);

  // max_compaction_bytes
  CastValue(column_family->GetConfItem(Constant::kMaxCompactionBytes), family_options.max_compaction_bytes);

  // write_buffer_size
  CastValue(column_family->GetConfItem(Constant::kWriteBufferSize), family_options.write_buffer_size);

  // max_bytes_for_level_multiplier
  CastValue(column_family->GetConfItem(Constant::kMaxBytesForLevelMultiplier),
            family_options.max_bytes_for_level_multiplier);

  // prefix_extractor
  {
    size_t value = 0;
    CastValue(column_family->GetConfItem(Constant::kPrefixExtractor), value);

    family_options.prefix_extractor.reset(rocksdb::NewCappedPrefixTransform(value));
  }

  // max_bytes_for_level_base
  CastValue(column_family->GetConfItem(Constant::kMaxBytesForLevelBase), family_options.max_bytes_for_level_base);

  // target_file_size_base
  CastValue(column_family->GetConfItem(Constant::kTargetFileSizeBase), family_options.target_file_size_base);

  family_options.compression_per_level = {
      rocksdb::CompressionType::kNoCompression,  rocksdb::CompressionType::kNoCompression,
      rocksdb::CompressionType::kLZ4Compression, rocksdb::CompressionType::kLZ4Compression,
      rocksdb::CompressionType::kLZ4Compression, rocksdb::CompressionType::kZSTD,
      rocksdb::CompressionType::kZSTD,
  };

  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10.0, false));
  table_options.whole_key_filtering = true;

  rocksdb::TableFactory* table_factory = NewBlockBasedTableFactory(table_options);
  family_options.table_factory.reset(table_factory);

  // [GC-Tombstone baseline step-2] Register the write CF compaction filter (default OFF via gflag).
  // Only the txn write CF gets the filter; when gc_enable_compaction_filter is false this is a no-op
  // and the CF options are byte-for-byte identical to before. The factory is stateless and shared.
  if (FLAGS_gc_enable_compaction_filter && column_family->Name() == Constant::kTxnWriteCF) {
    family_options.compaction_filter_factory = std::make_shared<WriteCompactionFilterFactory>();
  }

  // [GC-Tombstone baseline step-B4] Register the write CF MVCC properties collector (default OFF via
  // gflag). Only the txn write CF gets it; when gc_enable_mvcc_properties_collector is false this is a
  // no-op and SST flush/compaction output is byte-for-byte identical to before. The collector persists
  // MVCC stats into SST user properties so the phase-B active-compaction scheduler can estimate the
  // discardable garbage of a region. The factory is stateless and shared; RocksDB creates one collector
  // instance per output SST.
  if (FLAGS_gc_enable_mvcc_properties_collector && column_family->Name() == Constant::kTxnWriteCF) {
    family_options.table_properties_collector_factories.emplace_back(std::make_shared<MvccPropertiesCollectorFactory>());
  }

  // [GC-Tombstone baseline step-3] Enable periodic compaction on the txn write CF so cold SSTs are
  // re-compacted (and thus pass through the filter) periodically. Default 0 -> do not touch the option
  // at all, keeping CF options byte-for-byte identical to before. Only the write CF is configured
  // (the filter is only registered there; periodic on data CF would be pure write amplification).
  if (FLAGS_gc_periodic_compaction_seconds > 0 && column_family->Name() == Constant::kTxnWriteCF) {
    family_options.periodic_compaction_seconds = static_cast<uint64_t>(FLAGS_gc_periodic_compaction_seconds);
  }

  return family_options;
}

rocksdb::DB* RocksRawEngine::InitDB(const std::string& db_path, rocks::ColumnFamilyMap& column_families) {
  // Cast ColumnFamily to rocksdb::ColumnFamilyOptions
  std::vector<rocksdb::ColumnFamilyDescriptor> column_family_descs;
  for (auto [cf_name, column_family] : column_families) {
    column_family->Dump();
    rocksdb::ColumnFamilyOptions family_options = GenRocksDBColumnFamilyOptions(column_family);
    column_family_descs.push_back(rocksdb::ColumnFamilyDescriptor(cf_name, family_options));
  }

  rocksdb::DBOptions db_options;
  db_options.create_if_missing = true;
  db_options.create_missing_column_families = true;
  db_options.max_background_jobs = ConfigHelper::GetRocksDBBackgroundThreadNum();
  db_options.max_subcompactions = db_options.max_background_jobs / 4 * 3;
  db_options.stats_dump_period_sec = ConfigHelper::GetRocksDBStatsDumpPeriodSec();
  // Cap the info LOG (diagnostic LOG file) so it does not grow unbounded (the periodic stats dump above
  // keeps appending to it even on an idle DB). Configurable via the role yaml; defaults to 256MB x 10.
  db_options.max_log_file_size = ConfigHelper::GetRocksDBMaxLogFileSize();
  db_options.keep_log_file_num = ConfigHelper::GetRocksDBKeepLogFileNum();
  db_options.log_file_time_to_roll = ConfigHelper::GetRocksDBLogFileTimeToRollSec();
  db_options.use_direct_io_for_flush_and_compaction = true;
  db_options.statistics=rocksdb::CreateDBStatistics();
  // Observability-only event listener: real-time flush/compaction/stall/error metrics (see
  // RocksdbEventListener). Off by default; kept alive by db_options.listeners for the DB's lifetime.
  if (FLAGS_enable_rocksdb_event_listener) {
    db_options.listeners.emplace_back(std::make_shared<RocksdbEventListener>("store"));
  }

  DINGO_LOG(INFO) << fmt::format("[rocksdb] config max_background_jobs({}) max_subcompactions({})",
                                 db_options.max_background_jobs, db_options.max_subcompactions);

  rocksdb::DB* db;
  std::vector<rocksdb::ColumnFamilyHandle*> family_handles;
  rocksdb::Status s = rocksdb::DB::Open(db_options, db_path, column_family_descs, &family_handles, &db);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] open db failed, error: {}", s.ToString());
    return nullptr;
  }

  // Set family handle
  int i = 0;
  for (auto [_, column_family] : column_families) {
    column_family->SetHandle(family_handles[i++]);
  }

  return db;
}

// load rocksdb config from config file
bool RocksRawEngine::Init(std::shared_ptr<Config> config, const std::vector<std::string>& cf_names) {
  DINGO_LOG(INFO) << "Init rocksdb raw engine...";
  if (BAIDU_UNLIKELY(!config)) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] config empty not support!");
    return false;
  }

  std::string db_path = config->GetString(Constant::kStorePathConfigName) + "/rocksdb";
  if (BAIDU_UNLIKELY(db_path.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] can not find: {}/rocksdb", Constant::kStorePathConfigName);
    return false;
  }

  db_path_ = db_path;
  DINGO_LOG(INFO) << fmt::format("[rocksdb] db path: {}", db_path_);

  // Column family config priority custom(store.$cf_name) > custom(store.base) > default.
  auto column_families = GenColumnFamilyByDefaultConfig(cf_names);
  SetColumnFamilyCustomConfig(config, column_families);

  rocksdb::DB* db = InitDB(db_path_, column_families);
  if (db == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] open failed, path: {}", db_path_);
    return false;
  }
  column_families_ = column_families;
  db_.reset(db);

  reader_ = std::make_shared<rocks::Reader>(GetSelfPtr());
  writer_ = std::make_shared<rocks::Writer>(GetSelfPtr());

  DINGO_LOG(INFO) << fmt::format("[rocksdb] open success, path: {}", db_path_);

  return true;
}

std::shared_ptr<RocksRawEngine> RocksRawEngine::GetSelfPtr() {
  return std::dynamic_pointer_cast<RocksRawEngine>(shared_from_this());
}

std::string RocksRawEngine::GetName() { return pb::common::RawEngine_Name(pb::common::RAW_ENG_ROCKSDB); }

pb::common::RawEngine RocksRawEngine::GetRawEngineType() { return pb::common::RawEngine::RAW_ENG_ROCKSDB; }

std::string RocksRawEngine::DbPath() { return db_path_; }

std::shared_ptr<rocksdb::DB> RocksRawEngine::GetDB() { return db_; }

rocks::ColumnFamilyPtr RocksRawEngine::GetDefaultColumnFamily() { return GetColumnFamily(Constant::kStoreDataCF); }

rocks::ColumnFamilyPtr RocksRawEngine::GetColumnFamily(const std::string& cf_name) {
  auto it = column_families_.find(cf_name);
  if (it == column_families_.end()) {
    DINGO_LOG(FATAL) << fmt::format("[rocksdb] Not found column family {}", cf_name);
  }

  return it->second;
}

std::vector<rocks::ColumnFamilyPtr> RocksRawEngine::GetColumnFamilies(const std::vector<std::string>& cf_names) {
  std::vector<rocks::ColumnFamilyPtr> column_families;
  column_families.reserve(cf_names.size());
  for (const auto& cf_name : cf_names) {
    auto column_family = GetColumnFamily(cf_name);
    if (column_family != nullptr) {
      column_families.push_back(column_family);
    }
  }

  return column_families;
}

dingodb::SnapshotPtr RocksRawEngine::GetSnapshot() {
  return std::make_shared<rocks::Snapshot>(db_->GetSnapshot(), db_);
}

RawEngine::ReaderPtr RocksRawEngine::Reader() { return reader_; }

RawEngine::WriterPtr RocksRawEngine::Writer() { return writer_; }

rocks::SstFileWriterPtr RocksRawEngine::NewSstFileWriter() {
  return std::make_shared<rocks::SstFileWriter>(rocksdb::Options());
}

RawEngine::CheckpointPtr RocksRawEngine::NewCheckpoint() { return std::make_shared<rocks::Checkpoint>(GetSelfPtr()); }

butil::Status RocksRawEngine::MergeCheckpointFiles(const std::string& path, const pb::common::Range& range,
                                                   const std::vector<std::string>& cf_names,
                                                   std::vector<std::string>& merge_sst_paths) {
  rocksdb::Options options;
  options.create_if_missing = false;

  if (cf_names.size() != merge_sst_paths.size()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[rocksdb] merge checkpoint files failed, cf_names size: {}, merge_sst_paths size: {}", cf_names.size(),
        merge_sst_paths.size());
    return butil::Status(pb::error::EINTERNAL,
                         fmt::format("merge checkpoint files failed, cf_names size: {}, merge_sst_paths size: {}",
                                     cf_names.size(), merge_sst_paths.size()));
  }

  if (cf_names.empty()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] merge checkpoint files failed, cf_names empty");
    return butil::Status(pb::error::EINTERNAL, "merge checkpoint files failed, cf_names empty");
  }

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.reserve(cf_names.size());
  for (const auto& cf_name : cf_names) {
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(cf_name, rocksdb::ColumnFamilyOptions()));
  }

  // Due to delete other region sst file, so need repair db, or rocksdb::DB::Open will fail.
  auto status = rocksdb::RepairDB(path, options, column_families);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[rocksdb] repair db failed, path: {} error: {}", path, status.ToString());
    return butil::Status(pb::error::EINTERNAL, fmt::format("Rocksdb Repair db failed, {}", status.ToString()));
  }

  auto default_cf_desc = rocksdb::ColumnFamilyDescriptor(Constant::kStoreDataCF, rocksdb::ColumnFamilyOptions());

  for (int i = 0; i < cf_names.size(); i++) {
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
    cf_descs.push_back(default_cf_desc);
    if (cf_names[i] != Constant::kStoreDataCF) {
      cf_descs.push_back(rocksdb::ColumnFamilyDescriptor(cf_names[i], rocksdb::ColumnFamilyOptions()));
    }

    // Open snapshot db.
    rocksdb::DB* snapshot_db = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    status = rocksdb::DB::OpenForReadOnly(options, path, cf_descs, &handles, &snapshot_db);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] open checkpoint failed, path: {} error: {}", path, status.ToString());
      // return butil::Status(pb::error::EINTERNAL, fmt::format("Rocksdb open checkpoint failed, {}",
      // status.ToString()));
      merge_sst_paths[i] = "";
      continue;
    }

    DINGO_LOG(INFO) << fmt::format("[rocksdb] open checkpoint success, path: {} cf_name: {}", path, cf_names[i]);

    // Create iterator
    IteratorOptionsPtr iter_options = std::make_shared<IteratorOptions>();
    iter_options->upper_bound = range.end_key();

    rocksdb::ReadOptions read_options;
    read_options.auto_prefix_mode = true;

    auto& merge_sst_path = merge_sst_paths[i];
    auto* handle = handles[0];
    if (handles.size() > 1) {
      handle = handles[1];
    }

    butil::Status ret_status = butil::Status::OK();
    {
      auto iter = std::make_shared<rocks::Iterator>(iter_options, snapshot_db->NewIterator(read_options, handle));
      if (iter == nullptr) {
        DINGO_LOG(ERROR) << fmt::format("[rocksdb] merge checkpoint files failed, create iterator failed");
        ret_status = butil::Status(pb::error::EINTERNAL, "merge checkpoint files failed, create iterator failed");
      } else {
        iter->Seek(range.start_key());
        auto ret = NewSstFileWriter()->SaveFile(iter, merge_sst_path);
        if (ret.error_code() == pb::error::Errno::ENO_ENTRIES) {
          DINGO_LOG(WARNING) << "[rocksdb] merge checkpoint files no entries, file_name=" << merge_sst_path;
          merge_sst_paths[i] = "";
        } else if (!ret.ok()) {
          DINGO_LOG(ERROR) << fmt::format("[rocksdb] merge checkpoint files failed, save file failed")
                           << ", error: " << ret.error_str();
          ret_status = butil::Status(pb::error::EINTERNAL, "merge checkpoint files failed, save file failed");
        }

        DINGO_LOG(INFO) << fmt::format("[rocksdb] merge checkpoint files success, path: {} cf_name: {}", path,
                                       cf_names[i]);
      }
    }

    // Close snapshot db.
    try {
      CancelAllBackgroundWork(snapshot_db, true);
      snapshot_db->DropColumnFamilies(handles);
      for (auto& handle : handles) {
        snapshot_db->DestroyColumnFamilyHandle(handle);
      }
      snapshot_db->Close();
      delete snapshot_db;
    } catch (std::exception& e) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] close snapshot db failed, path: {} error: {}", path, e.what());
      ret_status = butil::Status(pb::error::EINTERNAL, fmt::format("Rocksdb close snapshot db failed, {}", e.what()));
    }

    if (!ret_status.ok()) {
      return ret_status;
    }
  }

  return butil::Status::OK();
}

butil::Status RocksRawEngine::IngestExternalFile(const std::string& cf_name, const std::vector<std::string>& files) {
  rocksdb::IngestExternalFileOptions options;
  options.write_global_seqno = false;
  auto status = db_->IngestExternalFile(GetColumnFamily(cf_name)->GetHandle(), files, options);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] ingest external fille failed, error: {}", status.ToString());
    return butil::Status(status.code(), status.ToString());
  }

  return butil::Status();
}

void RocksRawEngine::Flush(const std::string& cf_name) {
  if (db_) {
    rocksdb::FlushOptions flush_options;
    db_->Flush(flush_options, GetColumnFamily(cf_name)->GetHandle());
  }
}

butil::Status RocksRawEngine::Compact(const std::string& cf_name) {
  DINGO_LOG(INFO) << fmt::format("[rocksdb] compact column family {}", cf_name);
  if (db_ != nullptr) {
    rocksdb::CompactRangeOptions options;
    options.exclusive_manual_compaction = true;
    options.allow_write_stall = true;
    auto status = db_->CompactRange(options, GetColumnFamily(cf_name)->GetHandle(), nullptr, nullptr);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[rocksdb] compact failed, column family {}", cf_name);
      return butil::Status(pb::error::EINTERNAL, "Compact column family %s failed", cf_name.c_str());
    }
  }

  return butil::Status();
}

// [GC-Tombstone baseline step-4] Manual compaction over a single CF key range. Used by the active
// compaction driver (step 5) to feed cold SSTs of a region through the registered compaction filter.
butil::Status RocksRawEngine::CompactRange(const std::string& cf_name, const std::string& start_key,
                                           const std::string& end_key, bool force_bottommost) {
  if (db_ == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "db is null");
  }

  rocksdb::CompactRangeOptions options;
  // Unlike the legacy Compact() (which uses exclusive=true / allow_write_stall=true for a debug-style
  // forced full-CF compaction), the active driver runs periodically in the background, so it must NOT
  // block background compactions and must yield when the DB is near a write stall.
  options.exclusive_manual_compaction = false;
  options.allow_write_stall = false;
  options.max_subcompactions = 1;
  // With the write CF filter registered, kIfHaveCompactionFilter pulls the bottommost level INTO this
  // compaction (where versions are physically reclaimed). kForce compacts the bottommost every time at the
  // cost of large write amplification, used only when the caller explicitly asks.
  // [M3] CAUTION: neither option makes the CompactionFilter Context.is_full_compaction true for a per-range
  // (sub-key-range) CompactRange. is_full_compaction means "this compaction includes ALL table files of the
  // CF" (rocksdb/compaction_filter.h Context), which a region sub-range compaction does not. Pulling in /
  // forcing the bottommost LEVEL is not the same as including ALL files of the CF. Therefore, when the
  // WriteCompactionFilter gates Delete-marker removal on is_full_compaction
  // (gc_compaction_filter_tombstone_require_full_compaction=true), this per-range CompactRange reclaims old
  // MVCC versions but does NOT physically delete Delete tombstones; tombstone reclamation relies on a
  // periodic full compaction or raft GC.
  options.bottommost_level_compaction = force_bottommost
                                            ? rocksdb::BottommostLevelCompaction::kForce
                                            : rocksdb::BottommostLevelCompaction::kIfHaveCompactionFilter;

  // start_key / end_key are already-encoded (EncodeBytes) physical keys; an empty string means an open
  // bound (nullptr) on that side. The Slice objects must outlive the CompactRange call.
  rocksdb::Slice begin_slice(start_key);
  rocksdb::Slice end_slice(end_key);
  const rocksdb::Slice* begin = start_key.empty() ? nullptr : &begin_slice;
  const rocksdb::Slice* end = end_key.empty() ? nullptr : &end_slice;

  auto status = db_->CompactRange(options, GetColumnFamily(cf_name)->GetHandle(), begin, end);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] compact range failed, column family {}, status {}", cf_name,
                                    status.ToString());
    return butil::Status(pb::error::EINTERNAL, "CompactRange column family %s failed", cf_name.c_str());
  }

  return butil::Status();
}

// [GC-Tombstone baseline step-B1] Parse the MvccPropertiesCollector (step-B4) user-collected
// properties from one SST's TableProperties and accumulate them into `out`. Values are decimal ASCII
// integers under the dingo.mvcc.* keys (see mvcc_properties_collector.h). Missing keys (old SSTs, or
// the collector gflag is off) are skipped, leaving the phase-B fields at 0. Never throws / FATALs:
// a malformed value is treated as 0 for that key.
static void ParseMvccUserProperties(const rocksdb::UserCollectedProperties& props, RangeTableProperties* out) {
  auto get_u64 = [&props](const char* key, uint64_t* dst) {
    auto it = props.find(key);
    if (it == props.end()) {
      return;
    }
    errno = 0;
    char* end = nullptr;
    unsigned long long v = std::strtoull(it->second.c_str(), &end, 10);  // NOLINT(runtime/int)
    if (errno == 0 && end != it->second.c_str()) {
      *dst += static_cast<uint64_t>(v);
    }
  };
  // Track min (oldest) / max (newest) ts across SSTs; ignore 0 (means "none").
  auto get_ts_min = [&props](const char* key, int64_t* dst) {
    auto it = props.find(key);
    if (it == props.end()) {
      return;
    }
    char* end = nullptr;
    long long v = std::strtoll(it->second.c_str(), &end, 10);  // NOLINT(runtime/int)
    if (end != it->second.c_str() && v > 0 && (*dst == 0 || v < *dst)) {
      *dst = static_cast<int64_t>(v);
    }
  };
  auto get_ts_max = [&props](const char* key, int64_t* dst) {
    auto it = props.find(key);
    if (it == props.end()) {
      return;
    }
    char* end = nullptr;
    long long v = std::strtoll(it->second.c_str(), &end, 10);  // NOLINT(runtime/int)
    if (end != it->second.c_str() && v > *dst) {
      *dst = static_cast<int64_t>(v);
    }
  };

  get_u64(kMvccPropNumVersions, &out->num_versions);
  get_u64(kMvccPropNumRows, &out->num_rows);
  get_u64(kMvccPropNumDeletes, &out->num_deletes);
  get_ts_min(kMvccPropOldestDeleteTs, &out->oldest_delete_ts);
  get_ts_max(kMvccPropNewestDeleteTs, &out->newest_delete_ts);
  get_ts_min(kMvccPropOldestStaleVersionTs, &out->oldest_stale_version_ts);
  get_ts_max(kMvccPropNewestStaleVersionTs, &out->newest_stale_version_ts);
}

butil::Status RocksRawEngine::GetRangeTableProperties(const std::string& cf_name, const std::string& start_key,
                                                      const std::string& end_key, RangeTableProperties* out) {
  if (out == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "out is null");
  }
  if (db_ == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "db is null");
  }

  auto column_family = GetColumnFamily(cf_name);
  if (column_family == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "column family %s not found", cf_name.c_str());
  }

  // start_key / end_key are already-encoded (EncodeBytes) physical keys. An empty start is a valid open
  // lower bound (empty Slice == smallest key). The Slice objects must outlive the call below.
  //
  // CAUTION: RocksDB's Range is [start, limit); an empty `limit` Slice is the SMALLEST key, NOT a max
  // sentinel, so [start, "") is an EMPTY range and would match no SST. The last region (end_key == "")
  // therefore cannot be expressed as an open upper bound here. We log it and proceed with the (empty)
  // range -> the caller gets zero properties and conservatively will NOT select that region (safe
  // direction: never over-compacts; only risk is the rightmost region not being score-selected via this
  // path -- it is still covered by RocksDB natural compaction and the periodic compaction fallback).
  // [TODO] cover the open-upper last region precisely (needs a reliable max-key sentinel for the encoded
  // keyspace, or GetPropertiesOfAllTables filtered by start).
  if (end_key.empty()) {
    DINGO_LOG(WARNING) << fmt::format(
        "[rocksdb] GetRangeTableProperties open upper bound (last region) on cf {} is not range-queryable; "
        "returning empty properties (region will not be score-selected this round)",
        cf_name);
  }
  rocksdb::Slice start_slice(start_key);
  rocksdb::Slice limit_slice(end_key);
  rocksdb::Range range(start_slice, limit_slice);

  rocksdb::TablePropertiesCollection collection;
  auto status = db_->GetPropertiesOfTablesInRange(column_family->GetHandle(), &range, 1, &collection);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[rocksdb] get properties of tables in range failed, column family {}, status {}",
                                    cf_name, status.ToString());
    return butil::Status(pb::error::EINTERNAL, "GetPropertiesOfTablesInRange column family %s failed", cf_name.c_str());
  }

  for (const auto& [file_name, tp] : collection) {
    if (tp == nullptr) {
      continue;
    }
    // ---- Phase-A: built-in TableProperties ----
    out->num_entries += tp->num_entries;
    out->num_deletions += tp->num_deletions;
    out->num_range_deletions += tp->num_range_deletions;
    out->sst_file_count += 1;
    // This RocksDB build's TableProperties has no oldest_ancester_time field; creation_time is its
    // equivalent (oldest ancestor key time, falling back to the compaction output file creation time).
    // Use file_creation_time as a secondary fallback. Track the min non-zero value across SSTs.
    uint64_t cold_time = tp->creation_time != 0 ? tp->creation_time : tp->file_creation_time;
    if (cold_time != 0 && (out->oldest_ancester_time == 0 || cold_time < out->oldest_ancester_time)) {
      out->oldest_ancester_time = cold_time;
    }

    // ---- Phase-B: MvccPropertiesCollector user properties (skipped if absent) ----
    ParseMvccUserProperties(tp->user_collected_properties, out);
  }

  return butil::Status();
}

void RocksRawEngine::Destroy() { rocksdb::DestroyDB(db_path_, rocksdb::Options()); }

void RocksRawEngine::Close() {
  if (db_) {
    CancelAllBackgroundWork(db_.get(), true);

    std::vector<rocksdb::ColumnFamilyHandle*> column_family_handles;
    for (auto& [_, column_family] : column_families_) {
      column_family_handles.push_back(column_family->GetHandle());
    }
    db_->DropColumnFamilies(column_family_handles);
    for (auto& handle : column_family_handles) {
      db_->DestroyColumnFamilyHandle(handle);
    }
    for (auto& [_, column_family] : column_families_) {
      column_family->SetHandle(nullptr);
    }

    db_->Close();
    db_ = nullptr;
  }

  DINGO_LOG(INFO) << fmt::format("[rocksdb] close db.");
}

std::vector<int64_t> RocksRawEngine::GetApproximateSizes(const std::string& cf_name,
                                                         std::vector<pb::common::Range>& ranges) {
  rocksdb::SizeApproximationOptions options;

  rocksdb::Range inner_ranges[ranges.size()];
  for (int i = 0; i < ranges.size(); ++i) {
    inner_ranges[i].start = ranges[i].start_key();
    inner_ranges[i].limit = ranges[i].end_key();
  }

  uint64_t sizes[ranges.size()];
  db_->GetApproximateSizes(options, GetColumnFamily(cf_name)->GetHandle(), inner_ranges, ranges.size(), sizes);

  std::vector<int64_t> result;
  result.reserve(ranges.size());
  for (int i = 0; i < ranges.size(); ++i) {
    result.push_back(sizes[i]);
  }

  return result;
}

}  // namespace dingodb
