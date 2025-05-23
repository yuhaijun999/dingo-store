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

#include "server/region_service.h"

#include <cstdint>
#include <ostream>
#include <string>

#include "brpc/builtin/common.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "common/helper.h"
#include "coprocessor/utils.h"
#include "document/codec.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "vector/codec.h"

namespace dingodb {

DECLARE_int32(dingo_max_print_html_table);

void RegionImpl::GetTabInfo(brpc::TabInfoList* info_list) const {
  brpc::TabInfo* info = info_list->add();
  info->tab_name = "region";
  info->path = "/region";
}

void RegionImpl::default_method(google::protobuf::RpcController* controller,
                                const pb::cluster::RegionRequest* /*request*/,
                                pb::cluster::RegionResponse* /*response*/, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  const brpc::Server* server = cntl->server();
  butil::IOBufBuilder os;
  const std::string& constraint = cntl->http_request().unresolved_path();
  const bool use_html = brpc::UseHTML(cntl->http_request());

  if (constraint.empty()) {
    cntl->http_response().set_content_type(use_html ? "text/html" : "text/plain");

    if (use_html) {
      os << "<!DOCTYPE html><html><head>\n"
         << brpc::gridtable_style() << "<script src=\"/js/sorttable\"></script>\n"
         << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
         << brpc::TabsHead();

      os << "<meta charset=\"UTF-8\">\n"
         << "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"
         << "<style>\n"
         << "  /* Define styles for different colors */\n"
         << "  .red-text {\n"
         << "    color: red;\n"
         << "  }\n"
         << "  .blue-text {\n"
         << "    color: blue;\n"
         << "  }\n"
         << "  .green-text {\n"
         << "    color: green;\n"
         << "  }\n"
         << "  .bold-text {"
         << "    font-weight: bold;"
         << "  }"
         << "  .part .full {\n"
         << "    visibility: hidden;\n"
         << "    width: 500px;\n"
         << "    background-color: gray;\n"
         << "    color: #fff;\n"
         << "    text-align: center;\n"
         << "    border-radius: 6px;\n"
         << "    padding: 5px 0;\n"
         << "    word-wrap: break-word;\n"
         << "    word-break:break-all;\n"
         << "    position: absolute;\n"
         << "    z-index: 1;\n"
         << "  }\n"

         << "  .part:hover .full {\n"
         << "    visibility: visible;\n"
         << "  }\n"
         << "</style>\n";

      os << brpc::TabsHead() << "</head><body>";
      server->PrintTabsBody(os, "region");
    } else {
      os << "# Use /region/<RegionId>\n";  // << butil::describe_resources<Socket>() << '\n';
    }

    os << "DINGO_STORE VERSION: " << std::string(GIT_VERSION) << '\n';

    int64_t epoch = 0;
    pb::common::Location coordinator_leader_location;
    std::vector<pb::common::Location> locations;
    pb::common::CoordinatorMap coordinator_map;
    coordinator_controller_->GetCoordinatorMap(0, epoch, coordinator_leader_location, locations, coordinator_map);

    if (coordinator_controller_->IsLeader()) {
      if (use_html) {
        os << (use_html ? "<br>\n" : "\n");
        os << "CoordinatorRole: <span class=\"blue-text bold-text\">LEADER</span>" << '\n';
      } else {
        os << (use_html ? "<br>\n" : "\n");
        os << "CoordinatorRole: LEADER ";
      }
    } else {
      os << (use_html ? "<br>\n" : "\n");
      if (use_html) {
        os << "CoordinatorRole: <span class=\"red-text bold-text\">FOLLOWER</span>" << '\n';

        os << (use_html ? "<br>\n" : "\n");
        os << "Coordinator Leader is <a class=\"red-text bold-text\" href=http://" +
                  coordinator_leader_location.host() + ":" + std::to_string(coordinator_leader_location.port()) +
                  "/dingo>" + coordinator_leader_location.host() + ":" +
                  std::to_string(coordinator_leader_location.port()) + "</a>"
           << '\n';
      } else {
        os << "CoordinatorRole: FOLLOWER " << '\n';
        os << (use_html ? "<br>\n" : "\n");
        os << "Coordinator Leader is " + coordinator_leader_location.host() + ":" +
                  std::to_string(coordinator_leader_location.port())
           << '\n';
      }
    }

    os << (use_html ? "<br>\n" : "\n");
    os << (use_html ? "<br>\n" : "\n");
    PrintRegions(os, use_html);
  } else {
    char* endptr = nullptr;
    int64_t region_id = strtoull(constraint.c_str(), &endptr, 10);
    if (*endptr == '\0' || *endptr == '/') {
      os << "RegionId=" << region_id << '\n';

      pb::common::Region region;
      auto ret = coordinator_controller_->QueryRegion(region_id, region);

      if (region.id() == 0) {
        os << "Region is not found" << '\n';
      } else {
        os << "================ Region: ================" << '\n';
        os << region.DebugString() << '\n';
      }

    } else {
      cntl->SetFailed(brpc::ENOMETHOD, "path=%s is not a RegionId", constraint.c_str());
    }
  }
  os.move_to(cntl->response_attachment());
}

static std::string GetPrimaryString(const dingodb::pb::meta::TableDefinition& table_definition,
                                    const std::vector<std::any>& values) {
  std::vector<std::string> result;
  for (int i = 0; i < values.size(); ++i) {
    if (strcmp(values[i].type().name(), "v") == 0) {
      continue;
    }
    const auto& column_definition = table_definition.columns().at(i);

    result.push_back(Helper::ConvertColumnValueToStringV2(column_definition, values[i]));
  }

  return dingodb::Helper::VectorToString(result);
}

pb::common::Range DecodeRangeToPlaintext(std::shared_ptr<CoordinatorControl> coordinator_controller,
                                         const pb::common::Region& region) {
  const pb::common::RegionDefinition& region_definition = region.definition();
  const auto& origin_range = region_definition.range();

  DINGO_LOG(INFO) << fmt::format(
      "decode range info, region_id({}) partition_id({}) table_id({}) region_type({}) index_type({}) prefix({})",
      region.id(), region_definition.part_id(),
      region_definition.index_id() > 0 ? region_definition.index_id() : region_definition.table_id(),
      pb::common::RegionType_Name(region.region_type()),
      pb::common::IndexType_Name(region.definition().index_parameter().index_type()),
      Helper::GetKeyPrefix(origin_range.start_key()));

  pb::common::Range plaintext_range;
  if (region.region_type() == pb::common::INDEX_REGION &&
      region.definition().index_parameter().has_vector_index_parameter()) {
    int64_t min_vector_id = 0, max_vector_id = 0;
    VectorCodec::DecodeRangeToVectorId(false, origin_range, min_vector_id, max_vector_id);

    plaintext_range.set_start_key(fmt::format("{}/{}/{}", Helper::GetKeyPrefix(origin_range.start_key()),
                                              VectorCodec::UnPackagePartitionId(origin_range.start_key()),
                                              min_vector_id));

    plaintext_range.set_end_key(fmt::format("{}/{}/{}", Helper::GetKeyPrefix(origin_range.end_key()),
                                            VectorCodec::UnPackagePartitionId(origin_range.end_key()), max_vector_id));

  } else if (region.region_type() == pb::common::DOCUMENT_REGION &&
             region.definition().index_parameter().has_document_index_parameter()) {
    int64_t min_document_id = 0, max_document_id = 0;
    DocumentCodec::DecodeRangeToDocumentId(false, origin_range, min_document_id, max_document_id);

    plaintext_range.set_start_key(fmt::format("{}/{}/{}", Helper::GetKeyPrefix(origin_range.start_key()),
                                              DocumentCodec::UnPackagePartitionId(origin_range.start_key()),
                                              min_document_id));

    plaintext_range.set_end_key(fmt::format("{}/{}/{}", Helper::GetKeyPrefix(origin_range.end_key()),
                                            DocumentCodec::UnPackagePartitionId(origin_range.end_key()),
                                            max_document_id));

  } else if (dingodb::Helper::IsExecutorRaw(origin_range.start_key()) ||
             dingodb::Helper::IsExecutorTxn(origin_range.start_key())) {
    // pb::meta::TableDefinitionWithId table_definition_with_id;

    // coordinator_controller->GetTable(region_definition.schema_id(), region_definition.table_id(),
    //                                  table_definition_with_id);
    // if (table_definition_with_id.table_id().entity_id() == 0) {
    //   coordinator_controller->GetIndex(region_definition.schema_id(), region_definition.index_id(), true,
    //                                    table_definition_with_id);
    // }

    // if (table_definition_with_id.table_id().entity_id() == 0) {
    //   DINGO_LOG(ERROR) << fmt::format(
    //       "Get table/index definition failed, table_id/index_id({}).",
    //       region_definition.index_id() > 0 ? region_definition.index_id() : region_definition.table_id());
    //   return plaintext_range;
    // }

    // if (origin_range.start_key().size() > Constant::kVectorKeyMinLenWithPrefix) {
    //   auto record_decoder = std::make_shared<RecordDecoder>(
    //       1, Utils::GenSerialSchema(table_definition_with_id.table_definition()), region_definition.part_id());
    //   std::vector<std::any> record;
    //   int ret = record_decoder->DecodeKey(origin_range.start_key(), record);
    //   if (ret != 0) {
    //     DINGO_LOG(ERROR) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
    //   }

    //   plaintext_range.set_start_key(fmt::format("{}/{}/{}", Helper::GetKeyPrefix(origin_range.start_key()),
    //                                             region_definition.part_id(),
    //                                             GetPrimaryString(table_definition_with_id.table_definition(),
    //                                             record)));
    // } else {
    //   plaintext_range.set_start_key(
    //       fmt::format("{}/{}", Helper::GetKeyPrefix(origin_range.start_key()), region_definition.part_id()));
    // }

    // if (origin_range.end_key().size() > Constant::kVectorKeyMinLenWithPrefix) {
    //   auto record_decoder = std::make_shared<RecordDecoder>(
    //       1, Utils::GenSerialSchema(table_definition_with_id.table_definition()), region_definition.part_id());
    //   std::vector<std::any> record;
    //   int ret = record_decoder->DecodeKey(origin_range.end_key(), record);
    //   if (ret != 0) {
    //     DINGO_LOG(ERROR) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
    //   }
    //   plaintext_range.set_end_key(fmt::format("{}/{}/{}", Helper::GetKeyPrefix(origin_range.end_key()),
    //                                           VectorCodec::UnPackagePartitionId(origin_range.end_key()),
    //                                           GetPrimaryString(table_definition_with_id.table_definition(),
    //                                           record)));
    // } else {
    //   plaintext_range.set_end_key(fmt::format("{}/{}", Helper::GetKeyPrefix(origin_range.end_key()),
    //                                           VectorCodec::UnPackagePartitionId(origin_range.end_key())));
    // }

  } else {
    std::string start_key = origin_range.start_key().substr(1, origin_range.start_key().size());
    plaintext_range.set_start_key(
        fmt::format("{}/{}", Helper::GetKeyPrefix(origin_range.start_key()), Helper::StringToHex(start_key)));

    std::string end_key = origin_range.end_key().substr(1, origin_range.end_key().size());
    plaintext_range.set_end_key(
        fmt::format("{}/{}", Helper::GetKeyPrefix(origin_range.end_key()), Helper::StringToHex(end_key)));
  }

  return plaintext_range;
}

void RegionImpl::PrintRegions(std::ostream& os, bool use_html) {
  std::vector<std::string> table_header;

  table_header.push_back("REGION_ID");
  table_header.push_back("REGION_NAME");
  table_header.push_back("EPOCH");
  table_header.push_back("REGION_TYPE");
  table_header.push_back("REGION_STATE");
  table_header.push_back("BRAFT_STATUS");
  table_header.push_back("REPLICA_STATUS");
  table_header.push_back("LEADER_ID");
  table_header.push_back("REPLICA");
  table_header.push_back("SCHEMA_ID");
  table_header.push_back("TENANT_ID");
  table_header.push_back("TABLE_ID");
  table_header.push_back("INDEX_ID");
  table_header.push_back("PART_ID");
  table_header.push_back("ENGINE");
  table_header.push_back("STORE_ENGINE");
  table_header.push_back("START_KEY");
  table_header.push_back("END_KEY");
  table_header.push_back("CREATE_TIME");
  table_header.push_back("UPDATE_TIME");
  table_header.push_back("REGION_ID");
  table_header.push_back("RAFT_STATE");
  table_header.push_back("READONLY");
  table_header.push_back("TERM");
  table_header.push_back("APPLIED_INDEX");
  table_header.push_back("COMITTED_INDEX");
  table_header.push_back("FIRST_INDEX");
  table_header.push_back("LAST_INDEX");
  table_header.push_back("DISK_INDEX");
  table_header.push_back("PENDING_INDEX");
  table_header.push_back("PENDING_QUEUE_SIZE");
  table_header.push_back("STABLE_FOLLOWERS");
  table_header.push_back("UNSTABLE_FOLLOWERS");
  table_header.push_back("REGION_ID");
  table_header.push_back("METIRCS_INDEX");
  table_header.push_back("REGION_SIZE");
  table_header.push_back("INDEX_TYPE");
  table_header.push_back("INDEX_SNAPSHOT_LOG_ID");
  table_header.push_back("INDEX_APPLY_LOG_ID");
  table_header.push_back("INDEX_BUILD_EPOCH");
  table_header.push_back("IS_STOP");
  table_header.push_back("IS_READY");
  table_header.push_back("IS_OWN_READY");
  table_header.push_back("IS_SWITCHING");
  table_header.push_back("BUILD_ERROR");
  table_header.push_back("REBUILD_ERROR");

  std::vector<int32_t> min_widths;

  min_widths.push_back(10);  // REGION_ID
  min_widths.push_back(20);  // REGION_NAME
  min_widths.push_back(6);   // EPOCH
  min_widths.push_back(10);  // REGION_TYPE
  min_widths.push_back(10);  // REGION_STATE
  min_widths.push_back(10);  // BRAFT_STATUS
  min_widths.push_back(10);  // REPLICA_STATUS
  min_widths.push_back(10);  // LEADER_ID
  min_widths.push_back(10);  // REPLICA
  min_widths.push_back(10);  // SCHEMA_ID
  min_widths.push_back(10);  // TENANT_ID
  min_widths.push_back(10);  // TABLE_ID
  min_widths.push_back(10);  // INDEX_ID
  min_widths.push_back(10);  // PART_ID
  min_widths.push_back(10);  // ENGINE
  min_widths.push_back(10);  // STOREENGINE
  min_widths.push_back(20);  // START_KEY
  min_widths.push_back(20);  // END_KEY
  min_widths.push_back(10);  // CREATE_TIME
  min_widths.push_back(10);  // UPDATE_TIME
  min_widths.push_back(10);  // REGION_ID
  min_widths.push_back(10);  // RAFT_STATE
  min_widths.push_back(10);  // READONLY
  min_widths.push_back(10);  // TERM
  min_widths.push_back(10);  // APPLIED_INDEX
  min_widths.push_back(10);  // COMITTED_INDEX
  min_widths.push_back(10);  // FIRST_INDEX
  min_widths.push_back(10);  // LAST_INDEX
  min_widths.push_back(10);  // DISK_INDEX
  min_widths.push_back(10);  // PENDING_INDEX
  min_widths.push_back(10);  // PENDING_QUEUE_SIZE
  min_widths.push_back(10);  // STABLE_FOLLOWERS
  min_widths.push_back(10);  // UNSTABLE_FOLLOWERS
  min_widths.push_back(10);  // REGION_ID
  min_widths.push_back(10);  // METIRCS_INDEX
  min_widths.push_back(10);  // REGION_SIZE
  min_widths.push_back(10);  // INDEX_TYPE
  min_widths.push_back(10);  // INDEX_SNAPSHOT_LOG_ID
  min_widths.push_back(10);  // INDEX_APPLY_LOG_ID
  min_widths.push_back(10);  // INDEX_BUILD_EPOCH
  min_widths.push_back(10);  // IS_STOP
  min_widths.push_back(10);  // IS_READY
  min_widths.push_back(10);  // IS_OWN_READY
  min_widths.push_back(10);  // IS_SWITCHING
  min_widths.push_back(10);  // BUILD_ERROR
  min_widths.push_back(10);  // REBUILD_ERROR

  std::vector<std::vector<std::string>> table_contents;
  std::vector<std::vector<std::string>> table_urls;

  pb::common::RegionMap region_map;
  coordinator_controller_->GetRegionMapFull(region_map);

  for (const auto& region : region_map.regions()) {
    std::vector<std::string> line;
    std::vector<std::string> url_line;

    line.push_back(std::to_string(region.id()));  // REGION_ID
    url_line.push_back(std::string("/region/"));

    line.push_back(region.definition().name());  // REGION_NAME
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().epoch().conf_version()) + "-" +
                   std::to_string(region.definition().epoch().version()));  // EPOCH
    url_line.push_back(std::string());

    line.push_back(pb::common::RegionType_Name(region.region_type()));  // REGION_TYPE
    url_line.push_back(std::string());

    line.push_back(pb::common::RegionState_Name(region.state()));  // REGION_STATE
    url_line.push_back(std::string());
    if (region.definition().store_engine() != pb::common::STORE_ENG_RAFT_STORE) {
      line.push_back("N/A");  // BRAFT_STATUS
      url_line.push_back(std::string());
    } else {
      line.push_back(pb::common::RegionRaftStatus_Name(region.status().raft_status()));  // BRAFT_STATUS
      url_line.push_back(std::string());
    }

    line.push_back(pb::common::ReplicaStatus_Name(region.status().replica_status()));  // REPLICA_STATUS
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.leader_store_id()));  // LEADER_ID
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().peers_size()));  // REPLICA
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().schema_id()));  // SCHEMA_ID
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().tenant_id()));  // TENANT_ID
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().table_id()));  // TABLE_ID
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().index_id()));  // INDEX_ID
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.definition().part_id()));  // PART_ID
    url_line.push_back(std::string());

    line.push_back(pb::common::RawEngine_Name(region.definition().raw_engine()));  // ENGINE
    url_line.push_back(std::string());

    line.push_back(pb::common::StorageEngine_Name(region.definition().store_engine()));  // STORE_ENGINE
    url_line.push_back(std::string());

    std::string start_key = Helper::StringToHex(region.definition().range().start_key());
    std::string end_key = Helper::StringToHex(region.definition().range().end_key());
    auto plaintext_range = DecodeRangeToPlaintext(coordinator_controller_, region);
    start_key += fmt::format("({})", plaintext_range.start_key());
    end_key += fmt::format("({})", plaintext_range.end_key());

    line.push_back(start_key);  // START_KEY
    url_line.push_back(std::string());

    line.push_back(end_key);  // END_KEY
    url_line.push_back(std::string());

    line.push_back(Helper::FormatMsTime(region.create_timestamp(), "%Y-%m-%d %H:%M:%S"));  // CREATE_TIME
    url_line.push_back(std::string());

    line.push_back(
        Helper::FormatMsTime(region.metrics().last_update_metrics_timestamp(), "%Y-%m-%d %H:%M:%S"));  // UPDATE_TIME
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.id()));  // REGION_ID
    url_line.push_back(std::string("/region/"));

    if (region.definition().store_engine() != pb::common::STORE_ENG_RAFT_STORE) {
      line.push_back("N/A");  // RAFT_STATE
      url_line.push_back(std::string());
    } else {
      line.push_back(pb::common::RaftNodeState_Name(region.metrics().braft_status().raft_state()));  // RAFT_STATE
      url_line.push_back(std::string());
    }

    line.push_back(std::to_string(region.metrics().braft_status().readonly()));  // READONLY
    url_line.push_back(std::string());
    if (region.definition().store_engine() != pb::common::STORE_ENG_RAFT_STORE) {
      line.push_back("N/A");  // TERM
      url_line.push_back(std::string());

      line.push_back("N/A");  // APPLIED_INDEX
      url_line.push_back(std::string());

      line.push_back("N/A");  // COMITTED_INDEX
      url_line.push_back(std::string());

      line.push_back("N/A");  // FIRST_INDEX
      url_line.push_back(std::string());

      line.push_back("N/A");  // LAST_INDEX
      url_line.push_back(std::string());

      line.push_back("N/A");  // DISK_INDEX
      url_line.push_back(std::string());

      line.push_back("N/A");  // PENDING_INDEX
      url_line.push_back(std::string());

      line.push_back("N/A");  // PENDING_QUEUE_SIZE
      url_line.push_back(std::string());

      line.push_back("N/A");  // STABLE_FOLLOWERS
      url_line.push_back(std::string());

      line.push_back("N/A");  // UNSTABLE_FOLLOWERS
      url_line.push_back(std::string());
    } else {
      line.push_back(std::to_string(region.metrics().braft_status().term()));  // TERM
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().braft_status().known_applied_index()));  // APPLIED_INDEX
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().braft_status().committed_index()));  // COMITTED_INDEX
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().braft_status().first_index()));  // FIRST_INDEX
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().braft_status().last_index()));  // LAST_INDEX
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().braft_status().disk_index()));  // DISK_INDEX
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().braft_status().pending_index()));  // PENDING_INDEX
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().braft_status().pending_queue_size()));  // PENDING_QUEUE_SIZE
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().braft_status().stable_followers().size()));  // STABLE_FOLLOWERS
      url_line.push_back(std::string());

      line.push_back(
          std::to_string(region.metrics().braft_status().unstable_followers().size()));  // UNSTABLE_FOLLOWERS
      url_line.push_back(std::string());
    }

    line.push_back(std::to_string(region.id()));  // REGION_ID
    url_line.push_back(std::string("/region/"));

    line.push_back(std::to_string(region.metrics().last_update_metrics_log_index()));  // METIRCS_INDEX
    url_line.push_back(std::string());

    line.push_back(std::to_string(region.metrics().region_size()));  // REGION_SIZE
    url_line.push_back(std::string());

    auto vector_index_type = region.definition().index_parameter().vector_index_parameter().vector_index_type();
    if (vector_index_type != pb::common::VECTOR_INDEX_TYPE_NONE) {
      line.push_back(pb::common::VectorIndexType_Name(vector_index_type));  // INDEX_TYPE
      url_line.push_back(std::string());

      line.push_back(
          std::to_string(region.metrics().vector_index_status().snapshot_log_id()));  // INDEX_SNAPSHOT_LOG_ID
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().apply_log_id()));  // INDEX_APPLY_LOG_ID
      url_line.push_back(std::string());

      line.push_back(
          std::to_string(region.metrics().vector_index_status().last_build_epoch_version()));  // INDEX_BUILD_EPOCH
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_stop()));  // IS_STOP
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_ready()));  // IS_READY
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_own_ready()));  // IS_OWN_READY
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_switching()));  // IS_SWITCHING
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_build_error()));  // BUILD_ERROR
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().vector_index_status().is_rebuild_error()));  // REBUILD_ERROR
      url_line.push_back(std::string());

    } else if (region.definition().index_parameter().has_document_index_parameter()) {
      line.push_back("DOCUMENT");  // INDEX_TYPE
      url_line.push_back(std::string());

      line.push_back(
          std::to_string(region.metrics().document_index_status().snapshot_log_id()));  // INDEX_SNAPSHOT_LOG_ID
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().document_index_status().apply_log_id()));  // INDEX_APPLY_LOG_ID
      url_line.push_back(std::string());

      line.push_back(
          std::to_string(region.metrics().document_index_status().last_build_epoch_version()));  // INDEX_BUILD_EPOCH
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().document_index_status().is_stop()));  // IS_STOP
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().document_index_status().is_ready()));  // IS_READY
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().document_index_status().is_own_ready()));  // IS_OWN_READY
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().document_index_status().is_switching()));  // IS_SWITCHING
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().document_index_status().is_build_error()));  // BUILD_ERROR
      url_line.push_back(std::string());

      line.push_back(std::to_string(region.metrics().document_index_status().is_rebuild_error()));  // REBUILD_ERROR
      url_line.push_back(std::string());

    } else {
      line.push_back("N/A");  // INDEX_TYPE
      url_line.push_back(std::string());

      line.push_back("N/A");  // INDEX_SNAPSHOT_LOG_ID
      url_line.push_back(std::string());

      line.push_back("N/A");  // INDEX_APPLY_LOG_ID
      url_line.push_back(std::string());

      line.push_back("N/A");  // INDEX_BUILD_EPOCH
      url_line.push_back(std::string());

      line.push_back("N/A");  // IS_STOP
      url_line.push_back(std::string());

      line.push_back("N/A");  // IS_READY
      url_line.push_back(std::string());

      line.push_back("N/A");  // IS_OWN_READY
      url_line.push_back(std::string());

      line.push_back("N/A");  // IS_SWITCHING
      url_line.push_back(std::string());

      line.push_back("N/A");  // BUILD_ERROR
      url_line.push_back(std::string());

      line.push_back("N/A");  // REBUILD_ERROR
      url_line.push_back(std::string());
    }

    table_contents.push_back(line);
    table_urls.push_back(url_line);
  }

  if (use_html) {
    os << "<span class=\"bold-text\">REGION: " << table_contents.size() << "</span>" << '\n';
  }

  // if region is too many, do not print html
  if (table_contents.size() > FLAGS_dingo_max_print_html_table) {
    Helper::PrintHtmlLines(os, use_html, table_header, min_widths, table_contents, table_urls);
  } else {
    Helper::PrintHtmlTable(os, use_html, table_header, min_widths, table_contents, table_urls);
  }
}

}  // namespace dingodb
