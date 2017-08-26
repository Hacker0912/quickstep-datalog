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

#include "query_execution/QueryManagerBase.hpp"

#include <memory>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/QueryContext.hpp"
#include "query_optimizer/QueryHandle.hpp"
#include "query_optimizer/QueryPlan.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "relational_operators/WorkOrder.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "utility/EqualsAnyConstant.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

using std::pair;

namespace quickstep {

DEFINE_bool(visualize_execution_dag, false,
            "If true, visualize the execution plan DAG into a graph in DOT "
            "format (DOT is a plain text graph description language) which is "
            "then printed via stderr.");

const int QueryManagerBase::kInvalidPartitionGroup = -1;

QueryManagerBase::QueryManagerBase(QueryHandle *query_handle)
    : query_handle_(DCHECK_NOTNULL(query_handle)),
      query_id_(query_handle->query_id()),
      query_dag_(DCHECK_NOTNULL(
          DCHECK_NOTNULL(query_handle->getQueryPlanMutable())->getQueryPlanDAGMutable())),
      num_operators_in_dag_(query_dag_->size()),
      num_partitions_(num_operators_in_dag_),
      output_num_partitions_(num_operators_in_dag_),
      has_repartitions_(num_operators_in_dag_),
      output_consumers_(num_operators_in_dag_),
      blocking_dependencies_(num_operators_in_dag_),
      query_exec_state_(new QueryExecutionState(num_operators_in_dag_)),
      op_partition_groups_(num_operators_in_dag_, kInvalidPartitionGroup) {
  if (FLAGS_visualize_execution_dag) {
    dag_visualizer_ =
        std::make_unique<quickstep::ExecutionDAGVisualizer>(query_handle_->getQueryPlan());
  }

  bool has_repartition = false;
  for (dag_node_index node_index = 0;
       node_index < num_operators_in_dag_;
       ++node_index) {
    const RelationalOperator &op =
        query_dag_->getNodePayload(node_index);
    num_partitions_[node_index] = op.getNumPartitions();
    output_num_partitions_[node_index] = op.getOutputNumPartitions();
    has_repartitions_[node_index] = op.hasRepartition();
    if (has_repartitions_[node_index]) {
      has_repartition = true;
    }

    if (QUICKSTEP_EQUALS_ANY_CONSTANT(op.getOperatorType(),
                                      RelationalOperator::kDropTable,
                                      RelationalOperator::kDestroyAggregationState,
                                      RelationalOperator::kDestroyHash,
                                      RelationalOperator::kSaveBlocks)) {
      cleanup_ops_.insert(node_index);
    }

    const QueryContext::insert_destination_id insert_destination_index =
        op.getInsertDestinationID();
    if (insert_destination_index != QueryContext::kInvalidInsertDestinationId) {
      // Rebuild is necessary whenever InsertDestination is present.
      query_exec_state_->setRebuildRequired(node_index);
    }

    for (const pair<dag_node_index, bool> &dependent_link :
         query_dag_->getDependents(node_index)) {
      const dag_node_index dependent_op_index = dependent_link.first;
      if (query_dag_->getLinkMetadata(node_index, dependent_op_index)) {
        // The link is a pipeline-breaker. Streaming of blocks is not possible
        // between these two operators.
        blocking_dependencies_[dependent_op_index].push_back(node_index);
      } else {
        // The link is not a pipeline-breaker. Streaming of blocks is possible
        // between these two operators.
        output_consumers_[node_index].push_back(dependent_op_index);
      }
    }
  }

  if (has_repartition) {
    computePartitionGroups();

    for (size_t i = 0; i < partition_groups_.size(); ++i) {
      std::cerr << "Partition Group " << std::to_string(i) << " ("
                << std::to_string(partition_groups_info_[i]) << " partitions): ";
      const auto &partition_group = partition_groups_[i];
      auto cit = partition_group.begin();
      printOperatorInfo(*cit);

      for (++cit; cit != partition_group.end(); ++cit) {
        std::cerr << ", ";
        printOperatorInfo(*cit);
      }
      std::cerr << std::endl;
    }
  }
}

QueryManagerBase::QueryStatusCode QueryManagerBase::queryStatus(
    const dag_node_index op_index) {
  // As kQueryExecuted takes precedence over kOperatorExecuted, we first check
  // whether the query has finished its execution.
  if (query_exec_state_->hasQueryExecutionFinished()) {
    return QueryStatusCode::kQueryExecuted;
  }

  if (query_exec_state_->hasExecutionFinished(op_index)) {
    return QueryStatusCode::kOperatorExecuted;
  }

  return QueryStatusCode::kNone;
}

void QueryManagerBase::processFeedbackMessage(
    const dag_node_index op_index, const WorkOrder::FeedbackMessage &msg) {
  RelationalOperator *op =
      query_dag_->getNodePayloadMutable(op_index);
  op->receiveFeedbackMessage(msg);
}

void QueryManagerBase::processWorkOrderCompleteMessage(
    const dag_node_index op_index,
    const partition_id part_id) {
  query_exec_state_->decrementNumQueuedWorkOrders(op_index);

  // Check if new work orders are available and fetch them if so.
  fetchNormalWorkOrders(op_index);

  if (checkRebuildRequired(op_index)) {
    if (checkNormalExecutionOver(op_index)) {
      if (!checkRebuildInitiated(op_index)) {
        if (initiateRebuild(op_index)) {
          // Rebuild initiated and completed right away.
          markOperatorFinished(op_index);
        } else {
          // Rebuild under progress.
        }
      } else if (checkRebuildOver(op_index)) {
        // Rebuild was under progress and now it is over.
        markOperatorFinished(op_index);
      }
    } else {
      // Normal execution under progress for this operator.
    }
  } else if (checkOperatorExecutionOver(op_index)) {
    // Rebuild not required for this operator and its normal execution is
    // complete.
    markOperatorFinished(op_index);
  }

  for (const pair<dag_node_index, bool> &dependent_link :
       query_dag_->getDependents(op_index)) {
    const dag_node_index dependent_op_index = dependent_link.first;
    if (checkAllBlockingDependenciesMet(dependent_op_index)) {
      // Process the dependent operator (of the operator whose WorkOrder
      // was just executed) for which all the dependencies have been met.
      processOperator(dependent_op_index, true);
    }
  }
}

void QueryManagerBase::processRebuildWorkOrderCompleteMessage(const dag_node_index op_index,
                                                              const partition_id part_id) {
  query_exec_state_->decrementNumRebuildWorkOrders(op_index);

  if (checkRebuildOver(op_index)) {
    markOperatorFinished(op_index);

    for (const pair<dag_node_index, bool> &dependent_link :
         query_dag_->getDependents(op_index)) {
      const dag_node_index dependent_op_index = dependent_link.first;
      if (checkAllBlockingDependenciesMet(dependent_op_index)) {
        processOperator(dependent_op_index, true);
      }
    }
  }
}

void QueryManagerBase::processOperator(const dag_node_index index,
                                       const bool recursively_check_dependents) {
  if (fetchNormalWorkOrders(index)) {
    // Fetched work orders. Return to wait for the generated work orders to
    // execute, and skip the execution-finished checks.
    return;
  }

  if (checkNormalExecutionOver(index)) {
    if (checkRebuildRequired(index)) {
      if (!checkRebuildInitiated(index)) {
        // Rebuild hasn't started, initiate it.
        if (initiateRebuild(index)) {
          // Rebuild initiated and completed right away.
          markOperatorFinished(index);
        } else {
          // Rebuild WorkOrders have been generated.
          return;
        }
      } else if (checkRebuildOver(index)) {
        // Rebuild had been initiated and it is over.
        markOperatorFinished(index);
      }
    } else {
      // Rebuild is not required and normal execution over, mark finished.
      markOperatorFinished(index);
    }
    // If we reach here, that means the operator has been marked as finished.
    if (recursively_check_dependents) {
      for (const pair<dag_node_index, bool> &dependent_link :
           query_dag_->getDependents(index)) {
        const dag_node_index dependent_op_index = dependent_link.first;
        if (checkAllBlockingDependenciesMet(dependent_op_index)) {
          processOperator(dependent_op_index, true);
        }
      }
    }
  }
}

void QueryManagerBase::processDataPipelineMessage(const dag_node_index op_index,
                                                  const block_id block,
                                                  const relation_id rel_id,
                                                  const partition_id part_id) {
  for (const dag_node_index consumer_index :
       output_consumers_[op_index]) {
    // Feed the streamed block to the consumer. Note that 'output_consumers_'
    // only contain those dependents of operator with index = op_index which are
    // eligible to receive streamed input.
    query_dag_->getNodePayloadMutable(consumer_index)->feedInputBlock(block, rel_id, part_id);
    // Because of the streamed input just fed, check if there are any new
    // WorkOrders available and if so, fetch them.
    fetchNormalWorkOrders(consumer_index);
  }
}

void QueryManagerBase::markOperatorFinished(const dag_node_index index) {
  query_exec_state_->setExecutionFinished(index);

  RelationalOperator *op = query_dag_->getNodePayloadMutable(index);
  op->updateCatalogOnCompletion();

  const relation_id output_rel = op->getOutputRelationID();
  for (const pair<dag_node_index, bool> &dependent_link : query_dag_->getDependents(index)) {
    const dag_node_index dependent_op_index = dependent_link.first;
    RelationalOperator *dependent_op = query_dag_->getNodePayloadMutable(dependent_op_index);
    // Signal dependent operator that current operator is done feeding input blocks.
    if (output_rel >= 0) {
      dependent_op->doneFeedingInputBlocks(output_rel);
    }
  }
}

void QueryManagerBase::computePartitionGroups() {
  computePartitionGroupsHelper(0, partition_groups_.size());

  for (const std::size_t partition_group : op_partition_groups_) {
    DCHECK_NE(partition_group, kInvalidPartitionGroup);
  }
}

void QueryManagerBase::computePartitionGroupsHelper(const dag_node_index index,
                                                    const std::size_t partition_group_id) {
  if (op_partition_groups_[index] == kInvalidPartitionGroup) {
    op_partition_groups_[index] = partition_group_id;

    if (partition_group_id == partition_groups_.size()) {
      partition_groups_.push_back(std::set<dag_node_index>{ index });
      partition_groups_info_.push_back(num_partitions_[index]);
    } else {
      partition_groups_[partition_group_id].insert(index);
    }
  }

  for (const auto &dependent_link : query_dag_->getDependents(index)) {
    const dag_node_index dependent_op_index = dependent_link.first;
    if (op_partition_groups_[dependent_op_index] != kInvalidPartitionGroup) {
      continue;
    }

    if (areSamePartitionGroup(index, dependent_op_index)) {
      computePartitionGroupsHelper(dependent_op_index, partition_group_id);
    } else {
      computePartitionGroupsHelper(dependent_op_index, partition_groups_.size());
      partition_groups_dependencies_.emplace_back(partition_group_id, partition_groups_.size());
    }
  }

  for (const dag_node_index dependency_op_index : query_dag_->getDependencies(index)) {
    if (op_partition_groups_[dependency_op_index] != kInvalidPartitionGroup) {
      continue;
    }

    if (areSamePartitionGroup(dependency_op_index, index)) {
      computePartitionGroupsHelper(dependency_op_index, partition_group_id);
    } else {
      computePartitionGroupsHelper(dependency_op_index, partition_groups_.size());
      partition_groups_dependencies_.emplace_back(partition_groups_.size(), partition_group_id);
    }
  }
}

void QueryManagerBase::printOperatorInfo(const dag_node_index index) {
  std::cerr << query_dag_->getNodePayload(index).getName() << " ("
            << std::to_string(index) << ")";
}

}  // namespace quickstep
