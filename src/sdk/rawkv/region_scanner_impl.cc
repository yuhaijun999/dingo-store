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
#include "sdk/rawkv/region_scanner_impl.h"

#include <memory>

#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/store/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

RegionScannerImpl::RegionScannerImpl(const ClientStub& stub, std::shared_ptr<Region> region)
    : RegionScannerImpl(stub, std::move(region), region->Range().start_key(), region->Range().end_key()) {}

RegionScannerImpl::RegionScannerImpl(const ClientStub& stub, std::shared_ptr<Region> region, std::string start_key,
                                     std::string end_key)
    : RegionScanner(stub, std::move(region)),
      start_key_(std::move(start_key)),
      end_key_(std::move(end_key)),
      opened_(false),
      has_more_(false),
      batch_size_(kScanBatchSize) {}

RegionScannerImpl::~RegionScannerImpl() { Close(); }

void RegionScannerImpl::PrepareScanBegionRpc(KvScanBeginRpc& rpc) {
  auto* request = rpc.MutableRequest();
  FillRpcContext(*request->mutable_context(), region->RegionId(), region->Epoch());
  auto* range_with_option = request->mutable_range();
  range_with_option->mutable_range()->set_start_key(start_key_);
  range_with_option->mutable_range()->set_end_key(end_key_);
  range_with_option->set_with_start(true);
  range_with_option->set_with_end(false);

  request->set_max_fetch_cnt(0);
  request->set_key_only(false);
  // TODO: maybe we should support scan keep_alive
  request->set_disable_auto_release(false);
  request->set_disable_coprocessor(true);
}

Status RegionScannerImpl::Open() {
  CHECK(!opened_);
  KvScanBeginRpc rpc;
  PrepareScanBegionRpc(rpc);

  StoreRpcController controller(stub, rpc, region);
  Status call = controller.Call();
  if (call.IsOK()) {
    CHECK_EQ(0, rpc.Response()->kvs_size());
    scan_id_ = rpc.Response()->scan_id();
    has_more_ = true;
    opened_ = true;
  } else {
    DINGO_LOG(WARNING) << "open scanner for region:" << region->RegionId() << ", fail:" << call.ToString();
  }

  return call;
}

void RegionScannerImpl::PrepareScanReleaseRpc(KvScanReleaseRpc& rpc) {
  auto* request = rpc.MutableRequest();
  FillRpcContext(*request->mutable_context(), region->RegionId(), region->Epoch());
  request->set_scan_id(scan_id_);
}

void RegionScannerImpl::Close() {
  if (opened_) {
    CHECK(!scan_id_.empty());
    KvScanReleaseRpc rpc;
    PrepareScanReleaseRpc(rpc);

    StoreRpcController controller(stub, rpc, region);
    Status call = controller.Call();
    if (!call.IsOK()) {
      DINGO_LOG(WARNING) << "release scan fail, scan_id:" << scan_id_ << ", status:" << call.ToString();
    }

    // TODO: we should support scan keep alive in case release scan fail
    opened_ = false;
  }
}

bool RegionScannerImpl::HasMore() const { return has_more_; }

void RegionScannerImpl::PrepareScanContinueRpc(KvScanContinueRpc& rpc) {
  auto* request = rpc.MutableRequest();
  FillRpcContext(*request->mutable_context(), region->RegionId(), region->Epoch());
  request->set_scan_id(scan_id_);
  request->set_max_fetch_cnt(batch_size_);
}

Status RegionScannerImpl::NextBatch(std::vector<KVPair>& kvs) {
  CHECK(opened_);
  CHECK(!scan_id_.empty());
  KvScanContinueRpc rpc;
  PrepareScanContinueRpc(rpc);

  StoreRpcController controller(stub, rpc, region);
  Status ret = controller.Call();
  if (ret.IsOK()) {
    const auto* response = rpc.Response();
    std::vector<KVPair> tmp_kvs;
    if (response->kvs_size() == 0) {
      // scan to region end_key
      has_more_ = false;
    } else {
      for (const auto& kv : response->kvs()) {
        if (kv.key() < end_key_) {
          tmp_kvs.push_back({kv.key(), kv.value()});
        } else {
          has_more_ = false;
        }
      }
    }

    kvs = std::move(tmp_kvs);
  } else {
    DINGO_LOG(WARNING) << "scanner_id:" << scan_id_ << " scan continue fail region:" << region->RegionId()
                       << ", fail:" << ret.ToString();
  }

  return ret;
}

Status RegionScannerImpl::SetBatchSize(int64_t size) {
  uint64_t to_size = size;
  if (size <= kMinScanBatchSize) {
    to_size = kMinScanBatchSize;
  }

  if (size > kMaxScanBatchSize) {
    to_size = kMaxScanBatchSize;
  }

  batch_size_ = to_size;
  return Status::OK();
}

RegionScannerFactoryImpl::RegionScannerFactoryImpl() = default;

RegionScannerFactoryImpl::~RegionScannerFactoryImpl() = default;

Status RegionScannerFactoryImpl::NewRegionScanner(const ClientStub& stub, std::shared_ptr<Region> region,
                                                  std::unique_ptr<RegionScanner>& scanner) {
  std::unique_ptr<RegionScanner> tmp(new RegionScannerImpl(stub, std::move(region)));
  scanner = std::move(tmp);

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb