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

#include "query_execution/QueryManagerSingleNode.hpp"

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/CatalogDatabase.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/WorkerMessage.hpp"
#include "query_optimizer/QueryHandle.hpp"
#include "relational_operators/RebuildWorkOrder.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "storage/InsertDestination.hpp"
#include "storage/StorageBlock.hpp"
#include "utility/DAG.hpp"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace quickstep {

class WorkOrder;

QueryManagerSingleNode::QueryManagerSingleNode(
    const tmb::client_id foreman_client_id,
    const std::size_t num_numa_nodes,
    QueryHandle *query_handle,
    CatalogDatabaseLite *catalog_database,
    StorageManager *storage_manager,
    tmb::MessageBus *bus)
    : QueryManagerBase(query_handle),
      foreman_client_id_(foreman_client_id),
      storage_manager_(DCHECK_NOTNULL(storage_manager)),
      bus_(DCHECK_NOTNULL(bus)),
      query_context_(new QueryContext(query_handle->getQueryContextProto(),
                                      *catalog_database,
                                      storage_manager_,
                                      foreman_client_id_,
                                      bus_)),
      workorders_container_(
          new WorkOrdersContainer(num_operators_in_dag_, num_numa_nodes, input_num_partitions_,
                                  output_num_partitions_)),
      database_(static_cast<const CatalogDatabase&>(*catalog_database)) {
  // Collect all the workorders from all the non-blocking relational operators in the DAG.
  for (const dag_node_index index : non_dependent_operators_) {
    if (!fetchNormalWorkOrders(index)) {
      DCHECK(!checkRebuildRequired(index) || initiateRebuild(index));
      markOperatorFinished(index);
    }
  }
}

WorkerMessage* QueryManagerSingleNode::getNextWorkerMessage(
    const dag_node_index start_operator_index, const numa_node_id numa_node) {
  std::size_t operator_index;
  partition_id part_id;
  bool is_rebuild;
  WorkOrder *work_order = workorders_container_->getNextWorkOrder(&operator_index, &part_id, &is_rebuild);
  if (!work_order) {
    return nullptr;
  }

  DLOG(INFO) << "Dispatch " << (is_rebuild ? "Rebuild" : "") << "WorkOrderMessage for Operator "
             << operator_index << ", Partition " << part_id;
  if (is_rebuild) {
    return WorkerMessage::RebuildWorkOrderMessage(work_order, operator_index);
  }

  query_exec_state_->incrementNumQueuedWorkOrders(operator_index, part_id);
  return WorkerMessage::WorkOrderMessage(work_order, operator_index);
}

bool QueryManagerSingleNode::fetchNormalWorkOrders(const dag_node_index index) {
  // Do not fetch any work units until all blocking dependencies are met.
  // The releational operator is not aware of blocking dependencies for
  // uncorrelated scalar queries.
  DCHECK(checkAllBlockingDependenciesMet(index));
  // DCHECK(!query_exec_state_->hasDoneGenerationWorkOrders(index));

  const size_t num_pending_workorders_before =
      workorders_container_->getNumNormalWorkOrders(index);
  for (partition_id part_id = 0; part_id < input_num_partitions_[index]; ++part_id) {
    fetchNormalWorkOrders(index, part_id);
  }

  return (num_pending_workorders_before < workorders_container_->getNumNormalWorkOrders(index));
}

bool QueryManagerSingleNode::fetchNormalWorkOrders(const dag_node_index index,
                                                   const partition_id part_id) {
  const size_t num_pending_workorders_before =
      workorders_container_->getNumNormalWorkOrders(index, part_id);
  const bool done_generation =
      query_dag_->getNodePayloadMutable(index)->getAllWorkOrders(part_id,
                                                                 workorders_container_.get(),
                                                                 query_context_.get(),
                                                                 storage_manager_,
                                                                 foreman_client_id_,
                                                                 bus_);
  if (done_generation) {
    query_exec_state_->setDoneGenerationWorkOrders(index, part_id);
    if (!workorders_container_->hasEverNormalWorkOrders(index, part_id)) {
      query_exec_state_->setExecutionFinished(index, part_id);
    }
  }

  return (num_pending_workorders_before < workorders_container_->getNumNormalWorkOrders(index, part_id));
}

bool QueryManagerSingleNode::initiateRebuild(const dag_node_index index) {
  DCHECK(!workorders_container_->hasRebuildWorkOrder(index));
  DCHECK(checkRebuildRequired(index));
  DCHECK(!checkRebuildInitiated(index));

  const RelationalOperator &op = query_dag_->getNodePayload(index);
  const QueryContext::insert_destination_id insert_destination_index = op.getInsertDestinationID();
  DCHECK_NE(insert_destination_index, QueryContext::kInvalidInsertDestinationId);

  DCHECK(query_context_ != nullptr);
  InsertDestination *insert_destination = query_context_->getInsertDestination(insert_destination_index);
  DCHECK(insert_destination != nullptr);

  std::size_t total_num_rebuild_work_orders = 0;
  for (partition_id part_id = 0; part_id < output_num_partitions_[index]; ++part_id) {
    std::vector<MutableBlockReference> partially_filled_block_refs;
    insert_destination->getPartiallyFilledBlocksInPartition(part_id, &partially_filled_block_refs);

    for (std::size_t i = 0; i < partially_filled_block_refs.size(); ++i) {
      workorders_container_->addRebuildWorkOrder(
          new RebuildWorkOrder(query_id_,
                               std::move(partially_filled_block_refs[i]),
                               index,
                               op.getOutputRelationID(),
                               part_id,
                               foreman_client_id_,
                               bus_),
          index, part_id);
    }

    const std::size_t num_rebuild_work_orders = partially_filled_block_refs.size();
    query_exec_state_->setRebuildStatus(
        index, part_id, num_rebuild_work_orders);

    total_num_rebuild_work_orders += num_rebuild_work_orders;
  }

  return total_num_rebuild_work_orders == 0;
}

bool QueryManagerSingleNode::initiateRebuild(const dag_node_index index,
                                             const partition_id part_id) {
  DCHECK(!workorders_container_->hasRebuildWorkOrder(index, part_id));
  DCHECK(checkRebuildRequired(index));
  DCHECK(!checkRebuildInitiated(index, part_id));

  const RelationalOperator &op = query_dag_->getNodePayload(index);
  const QueryContext::insert_destination_id insert_destination_index = op.getInsertDestinationID();
  DCHECK_NE(insert_destination_index, QueryContext::kInvalidInsertDestinationId);

  DCHECK(query_context_ != nullptr);
  InsertDestination *insert_destination = query_context_->getInsertDestination(insert_destination_index);
  DCHECK(insert_destination != nullptr);

  std::vector<MutableBlockReference> partially_filled_block_refs;
  insert_destination->getPartiallyFilledBlocksInPartition(part_id, &partially_filled_block_refs);

  for (std::size_t i = 0; i < partially_filled_block_refs.size(); ++i) {
    workorders_container_->addRebuildWorkOrder(
        new RebuildWorkOrder(query_id_,
                             std::move(partially_filled_block_refs[i]),
                             index,
                             op.getOutputRelationID(),
                             part_id,
                             foreman_client_id_,
                             bus_),
        index, part_id);
  }

  const std::size_t num_rebuild_work_orders = partially_filled_block_refs.size();

  query_exec_state_->setRebuildStatus(
      index, part_id, num_rebuild_work_orders);

  return num_rebuild_work_orders == 0;
}


std::size_t QueryManagerSingleNode::getQueryMemoryConsumptionBytes() const {
  const std::size_t temp_relations_memory =
      getTotalTempRelationMemoryInBytes();
  const std::size_t temp_data_structures_memory =
      query_context_->getTempStructuresMemoryBytes();
  return temp_relations_memory + temp_data_structures_memory;
}

std::size_t QueryManagerSingleNode::getTotalTempRelationMemoryInBytes() const {
  std::vector<relation_id> temp_relation_ids;
  query_context_->getTempRelationIDs(&temp_relation_ids);
  std::size_t memory = 0;
  for (std::size_t rel_id : temp_relation_ids) {
    if (database_.hasRelationWithId(rel_id)) {
      memory += database_.getRelationById(rel_id)->getRelationSizeBytes();
    }
  }
  return memory;
}

}  // namespace quickstep
