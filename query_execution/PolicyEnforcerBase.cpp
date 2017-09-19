/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

#include "query_execution/PolicyEnforcerBase.hpp"

#include <cstddef>
#include <memory>
#include <queue>
#include <unordered_map>
#include <vector>

#include "catalog/CatalogDatabase.hpp"
#include "catalog/CatalogRelation.hpp"
#include "catalog/PartitionScheme.hpp"
#include "query_execution/QueryExecutionMessages.pb.h"
#include "query_execution/QueryExecutionState.hpp"
#include "query_execution/QueryExecutionTypedefs.hpp"
#include "query_execution/QueryManagerBase.hpp"
#include "relational_operators/WorkOrder.hpp"
#include "storage/StorageBlockInfo.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

using std::size_t;

namespace quickstep {

namespace S = serialization;

DECLARE_bool(visualize_execution_dag);

DEFINE_bool(profile_and_report_workorder_perf, false,
    "If true, Quickstep will record the exceution time of all the individual "
    "normal work orders and report it at the end of query execution.");

PolicyEnforcerBase::PolicyEnforcerBase(CatalogDatabaseLite *catalog_database)
    : catalog_database_(catalog_database),
      profile_individual_workorders_(FLAGS_profile_and_report_workorder_perf || FLAGS_visualize_execution_dag) {
}

void PolicyEnforcerBase::processWorkOrderCompleteMessage(const S::WorkOrderCompletionMessage &proto) {
  decrementNumQueuedWorkOrders(proto);

  if (profile_individual_workorders_) {
    // Note: This proto message contains the time it took to execute the
    // WorkOrder. It can be accessed in this scope.
    recordTimeForWorkOrder(proto);
  }

  const size_t query_id = proto.query_id();
  DCHECK(admitted_queries_.find(query_id) != admitted_queries_.end());

  admitted_queries_[query_id]->processWorkOrderCompleteMessage(proto.operator_index(), proto.partition_id());

  processOnQueryCompletion(query_id);
}

void PolicyEnforcerBase::processRebuildWorkOrderCompleteMessage(const S::WorkOrderCompletionMessage &proto) {
  decrementNumQueuedWorkOrders(proto);

  const size_t query_id = proto.query_id();
  DCHECK(admitted_queries_.find(query_id) != admitted_queries_.end());

  admitted_queries_[query_id]->processRebuildWorkOrderCompleteMessage(proto.operator_index(), proto.partition_id());

  processOnQueryCompletion(query_id);
}

void PolicyEnforcerBase::processCatalogRelationNewBlockMessage(const S::CatalogRelationNewBlockMessage &proto) {
  const block_id block = proto.block_id();

  CatalogRelation *relation =
      static_cast<CatalogDatabase*>(catalog_database_)->getRelationByIdMutable(proto.relation_id());
  relation->addBlock(block);

  if (proto.has_partition_id()) {
    relation->getPartitionSchemeMutable()->addBlockToPartition(block, proto.partition_id());
  }
}

void PolicyEnforcerBase::processDataPipelineMessage(const size_t query_id,
                                                    const QueryManagerBase::dag_node_index op_index,
                                                    const block_id block, const relation_id rel_id,
                                                    const partition_id part_id) {
  DCHECK(admitted_queries_.find(query_id) != admitted_queries_.end());

  admitted_queries_[query_id]->processDataPipelineMessage(op_index, block, rel_id, part_id);
}

void PolicyEnforcerBase::processWorkOrderFeedbackMessage(const WorkOrder::FeedbackMessage &msg) {
  const auto &header = msg.header();
  const size_t query_id = header.query_id;
  DCHECK(admitted_queries_.find(query_id) != admitted_queries_.end());

  admitted_queries_[query_id]->processFeedbackMessage(header.rel_op_index, msg);
}

void PolicyEnforcerBase::removeQuery(const std::size_t query_id) {
  DCHECK(admitted_queries_.find(query_id) != admitted_queries_.end());
  if (!admitted_queries_[query_id]->getQueryExecutionState().hasQueryExecutionFinished()) {
    LOG(WARNING) << "Removing query with ID " << query_id
                 << " that hasn't finished its execution";
  }
  admitted_queries_.erase(query_id);
}

bool PolicyEnforcerBase::admitQueries(
    const std::vector<QueryHandle*> &query_handles) {
  DCHECK(!query_handles.empty());

  bool all_queries_admitted = true;
  for (QueryHandle *curr_query : query_handles) {
    if (all_queries_admitted) {
      all_queries_admitted = admitQuery(curr_query);
    } else {
      waiting_queries_.push(curr_query);
    }
  }
  return all_queries_admitted;
}

void PolicyEnforcerBase::recordTimeForWorkOrder(
    const serialization::WorkOrderCompletionMessage &proto) {
  const std::size_t query_id = proto.query_id();
  std::vector<WorkOrderTimeEntry> &workorder_time_entries
      = workorder_time_recorder_[query_id];
  workorder_time_entries.emplace_back();
  WorkOrderTimeEntry &entry = workorder_time_entries.back();
  entry.worker_id = proto.worker_thread_index(),
  entry.operator_id = proto.operator_index(),
  entry.start_time = proto.execution_start_time(),
  entry.end_time = proto.execution_end_time();
}

}  // namespace quickstep
