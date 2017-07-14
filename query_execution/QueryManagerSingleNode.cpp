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

#include <algorithm>
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

using std::min;

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
          new WorkOrdersContainer(num_operators_in_dag_, num_numa_nodes, query_dag_)),
      database_(static_cast<const CatalogDatabase&>(*catalog_database)) {
  // Collect all the workorders from all the relational operators in the DAG.
  for (dag_node_index index = 0; index < num_operators_in_dag_; ++index) {
    RelationalOperator *op = query_dag_->getNodePayloadMutable(index);

    for (partition_id part_id = 0; part_id < input_num_partitions_[index]; ++part_id) {
      if (checkAllBlockingDependenciesMet(index, part_id)) {
        op->informAllBlockingDependenciesMet(part_id);
        processOperator(index, part_id, false);
      }
    }
  }
}

WorkerMessage* QueryManagerSingleNode::getNextWorkerMessage(
    const dag_node_index start_operator_index) {
  LOG(INFO) << workorders_container_->debugString();

  // Default policy: Operator with lowest index first.
  WorkOrder *work_order = nullptr;

  if (valid_recently_complete_work_order_info_) {
    valid_recently_complete_work_order_info_ = false;

    for (const dag_node_index dependent_op_index : pipelining_operators_[recently_complete_work_order_operator_]) {
      work_order = workorders_container_->getNormalWorkOrder(dependent_op_index,
                                                             recently_complete_work_order_partition_id_);
      if (work_order != nullptr) {
        query_exec_state_->incrementNumQueuedWorkOrders(dependent_op_index, recently_complete_work_order_partition_id_);
        LOG(INFO) << "NormalWorkOrder from Operator " << dependent_op_index
                  << " for partition " << recently_complete_work_order_partition_id_;
        return WorkerMessage::WorkOrderMessage(work_order, dependent_op_index);
      }
    }

    for (partition_id part_id = 0;
         part_id < min(recently_complete_work_order_partition_id_ + 1,
                       input_num_partitions_[recently_complete_work_order_operator_]);
         ++part_id) {
      work_order = workorders_container_->getNormalWorkOrder(recently_complete_work_order_operator_, part_id);
      if (work_order != nullptr) {
        LOG(INFO) << "NormalWorkOrder from Operator " << recently_complete_work_order_operator_
                  << " for partition " << part_id;
        query_exec_state_->incrementNumQueuedWorkOrders(recently_complete_work_order_operator_, part_id);
        return WorkerMessage::WorkOrderMessage(work_order, recently_complete_work_order_operator_);
      }
    }

    const auto cit = output_num_partitions_.find(recently_complete_work_order_operator_);
    if (cit != output_num_partitions_.end()) {
      for (partition_id part_id = 0;
           part_id < min(recently_complete_work_order_partition_id_ + 1, cit->second);
           ++part_id) {
        work_order = workorders_container_->getRebuildWorkOrder(recently_complete_work_order_operator_, part_id);
        if (work_order != nullptr) {
          LOG(INFO) << "RebuildWorkOrder from Operator " << recently_complete_work_order_operator_
                    << " for partition " << part_id;
          return WorkerMessage::RebuildWorkOrderMessage(work_order, recently_complete_work_order_operator_);
        }
      }
    }

    for (partition_id part_id = recently_complete_work_order_partition_id_ + 1;
         part_id < input_num_partitions_[recently_complete_work_order_operator_];
         ++part_id) {
      work_order = workorders_container_->getNormalWorkOrder(recently_complete_work_order_operator_, part_id);
      if (work_order != nullptr) {
        LOG(INFO) << "NormalWorkOrder from Operator " << recently_complete_work_order_operator_
                  << " for partition " << part_id;
        query_exec_state_->incrementNumQueuedWorkOrders(recently_complete_work_order_operator_, part_id);
        return WorkerMessage::WorkOrderMessage(work_order, recently_complete_work_order_operator_);
      }
    }

    if (cit != output_num_partitions_.end()) {
      for (partition_id part_id = recently_complete_work_order_partition_id_ + 1; part_id < cit->second; ++part_id) {
        work_order = workorders_container_->getRebuildWorkOrder(recently_complete_work_order_operator_, part_id);
        if (work_order != nullptr) {
          LOG(INFO) << "RebuildWorkOrder from Operator " << recently_complete_work_order_operator_
                    << " for partition " << part_id;
          return WorkerMessage::RebuildWorkOrderMessage(work_order, recently_complete_work_order_operator_);
        }
      }
    }
  }

  for (dag_node_index index = least_runable_operator_in_dag_; index < num_operators_in_dag_; ++index) {
    if (query_exec_state_->hasExecutionFinished(index)) {
      if (index == least_runable_operator_in_dag_) {
        ++least_runable_operator_in_dag_;
      }
      continue;
    }
    for (partition_id part_id = 0; part_id < input_num_partitions_[index]; ++part_id) {
      work_order = workorders_container_->getNormalWorkOrder(index, part_id);
      if (work_order != nullptr) {
        query_exec_state_->incrementNumQueuedWorkOrders(index, part_id);
        LOG(INFO) << "NormalWorkOrder from Operator " << index << " for partition " << part_id;
        return WorkerMessage::WorkOrderMessage(work_order, index);
      }
    }

    const auto cit = output_num_partitions_.find(index);
    if (cit != output_num_partitions_.end()) {
      for (partition_id part_id = 0; part_id < cit->second; ++part_id) {
        work_order = workorders_container_->getRebuildWorkOrder(index, part_id);
        if (work_order != nullptr) {
          LOG(INFO) << "RebuildWorkOrder from Operator " << index << " for partition " << part_id;
          return WorkerMessage::RebuildWorkOrderMessage(work_order, index);
        }
      }
    }
  }
  // No WorkOrders available right now.
  return nullptr;
}

bool QueryManagerSingleNode::fetchNormalWorkOrders(const dag_node_index index,
                                                   const partition_id part_id) {
  bool generated_new_workorders = false;
  if (!query_exec_state_->hasDoneGenerationWorkOrders(index, part_id)) {
    // Do not fetch any work units until all blocking dependencies are met.
    // The releational operator is not aware of blocking dependencies for
    // uncorrelated scalar queries.
    if (!checkAllBlockingDependenciesMet(index, part_id)) {
      return false;
    }
    const size_t num_pending_workorders_before =
        workorders_container_->getNumNormalWorkOrders(index, part_id);
    RelationalOperator *op = query_dag_->getNodePayloadMutable(index);
    const bool done_generation =
        op->getAllWorkOrders(part_id, workorders_container_.get(), query_context_.get(),
                             storage_manager_, foreman_client_id_, bus_);
    if (done_generation) {
      query_exec_state_->setDoneGenerationWorkOrders(index, part_id);
    }

    // TODO(shoban): It would be a good check to see if operator is making
    // useful progress, i.e., the operator either generates work orders to
    // execute or still has pending work orders executing. However, this will not
    // work if Foreman polls operators without feeding data. This check can be
    // enabled, if Foreman is refactored to call getAllWorkOrders() only when
    // pending work orders are completed or new input blocks feed.

    generated_new_workorders =
        (num_pending_workorders_before <
         workorders_container_->getNumNormalWorkOrders(index, part_id));
    LOG_IF(INFO, generated_new_workorders)
        << "Generated WorkOrder(s) from " << op->getName() << "(" << index << ") for Partition " << part_id
        << " in Query " << query_id_;
  }
  return generated_new_workorders;
}

bool QueryManagerSingleNode::initiateRebuild(const dag_node_index index,
                                             const partition_id part_id) {
  DCHECK(!workorders_container_->hasRebuildWorkOrder(index, part_id));
  DCHECK(checkRebuildRequired(index));
  DCHECK(!checkRebuildInitiated(index, part_id));

  getRebuildWorkOrders(index, part_id, workorders_container_.get());

  query_exec_state_->setRebuildStatus(
      index, part_id, workorders_container_->getNumRebuildWorkOrders(index, part_id), true);

  return (query_exec_state_->getNumRebuildWorkOrders(index, part_id) == 0);
}

bool QueryManagerSingleNode::initiateRebuild(const dag_node_index index) {
  bool rebuild_result = true;
  const auto cit = output_num_partitions_.find(index);
  DCHECK(cit != output_num_partitions_.end());
  for (partition_id output_part_id = 0; output_part_id < cit->second; ++output_part_id) {
    if (!initiateRebuild(index, output_part_id) && rebuild_result) {
      rebuild_result = false;
    }
  }

  return rebuild_result;
}

void QueryManagerSingleNode::getRebuildWorkOrders(const dag_node_index index,
                                                  const partition_id part_id,
                                                  WorkOrdersContainer *container) {
  const RelationalOperator &op = query_dag_->getNodePayload(index);
  const QueryContext::insert_destination_id insert_destination_index = op.getInsertDestinationID();
  DCHECK_NE(insert_destination_index, QueryContext::kInvalidInsertDestinationId);

  std::vector<MutableBlockReference> partially_filled_block_refs;

  DCHECK(query_context_ != nullptr);
  InsertDestination *insert_destination = query_context_->getInsertDestination(insert_destination_index);
  DCHECK(insert_destination != nullptr);

  insert_destination->getPartiallyFilledBlocksInPartition(part_id, &partially_filled_block_refs);

  const relation_id output_rel = op.getOutputRelationID();
  for (std::vector<MutableBlockReference>::size_type i = 0;
       i < partially_filled_block_refs.size();
       ++i) {
    container->addRebuildWorkOrder(
        new RebuildWorkOrder(query_id_,
                             std::move(partially_filled_block_refs[i]),
                             index,
                             output_rel,
                             part_id,
                             foreman_client_id_,
                             bus_),
        index, part_id);
  }
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
