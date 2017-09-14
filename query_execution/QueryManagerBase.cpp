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
#include <utility>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/QueryContext.hpp"
#include "query_optimizer/QueryHandle.hpp"
#include "query_optimizer/QueryPlan.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "relational_operators/WorkOrder.hpp"
#include "storage/StorageBlockInfo.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

using std::pair;

namespace quickstep {

DEFINE_bool(visualize_execution_dag, false,
            "If true, visualize the execution plan DAG into a graph in DOT "
            "format (DOT is a plain text graph description language) which is "
            "then printed via stderr.");

QueryManagerBase::QueryManagerBase(QueryHandle *query_handle)
    : query_handle_(DCHECK_NOTNULL(query_handle)),
      query_id_(query_handle->query_id()),
      query_dag_(DCHECK_NOTNULL(
          DCHECK_NOTNULL(query_handle->getQueryPlanMutable())->getQueryPlanDAGMutable())),
      num_operators_in_dag_(query_dag_->size()),
      input_num_partitions_(num_operators_in_dag_),
      output_num_partitions_(num_operators_in_dag_),
      has_repartitions_(num_operators_in_dag_),
      output_consumers_(num_operators_in_dag_),
      blocking_dependencies_(num_operators_in_dag_),
      blocking_dependents_(num_operators_in_dag_),
      non_blocking_dependencies_(num_operators_in_dag_) {
  if (FLAGS_visualize_execution_dag) {
    dag_visualizer_ =
        std::make_unique<quickstep::ExecutionDAGVisualizer>(query_handle_->getQueryPlan());
  }

  std::vector<dag_node_index> rebuild_ops;
  std::vector<std::unordered_set<dag_node_index>> blocking_dependencies(num_operators_in_dag_);
  for (dag_node_index node_index = 0;
       node_index < num_operators_in_dag_;
       ++node_index) {
    const RelationalOperator &op = query_dag_->getNodePayload(node_index);
    input_num_partitions_[node_index] = op.getNumPartitions();
    output_num_partitions_[node_index] = op.getOutputNumPartitions();
    has_repartitions_[node_index] = op.hasRepartition();

    LOG(INFO) << op.getName() << " ( " << node_index << " ) has input_num_partitions "
              << input_num_partitions_[node_index]
              << ", output_num_partitions " << output_num_partitions_[node_index]
              << ", has_repartitions " << has_repartitions_[node_index];

    const QueryContext::insert_destination_id insert_destination_index =
        op.getInsertDestinationID();
    if (insert_destination_index != QueryContext::kInvalidInsertDestinationId) {
      rebuild_ops.push_back(node_index);
    }

    if (query_dag_->getDependencies(node_index).empty()) {
      non_dependent_operators_.push_back(node_index);
    }

    for (const pair<dag_node_index, bool> &dependent_link :
         query_dag_->getDependents(node_index)) {
      const dag_node_index dependent_op_index = dependent_link.first;
      if (query_dag_->getLinkMetadata(node_index, dependent_op_index)) {
        // The link is a pipeline-breaker. Streaming of blocks is not possible
        // between these two operators.
        blocking_dependencies[dependent_op_index].insert(node_index);
        blocking_dependents_[node_index].push_back(dependent_op_index);
      } else {
        // The link is not a pipeline-breaker. Streaming of blocks is possible
        // between these two operators.
        non_blocking_dependencies_[dependent_op_index].insert(node_index);
        output_consumers_[node_index].insert(dependent_op_index);
      }
    }
  }

  for (dag_node_index node_index = 0; node_index < num_operators_in_dag_; ++node_index) {
    blocking_dependencies_[node_index].resize(input_num_partitions_[node_index]);
    for (const dag_node_index op_dependency_index : blocking_dependencies[node_index]) {
      for (partition_id part_id = 0; part_id < input_num_partitions_[node_index]; ++part_id) {
        blocking_dependencies_[node_index][part_id].insert(op_dependency_index);
      }
    }
  }

  query_exec_state_ = std::make_unique<QueryExecutionState>(input_num_partitions_);
  for (const dag_node_index node_index : rebuild_ops) {
    // Rebuild is necessary whenever InsertDestination is present.
    query_exec_state_->setRebuildRequired(node_index, output_num_partitions_[node_index]);
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

  if (query_exec_state_->hasDoneGenerationWorkOrders(op_index, 0u)) {
    return;
  }

  fetchNormalWorkOrders(op_index);
}

void QueryManagerBase::processWorkOrderCompleteMessage(
    const dag_node_index op_index,
    const partition_id part_id) {
  query_exec_state_->decrementNumQueuedWorkOrders(op_index, part_id);

  if (!checkNormalExecutionOver(op_index, part_id)) {
    // Normal execution under progress for this operator.
    return;
  }

  if (!checkRebuildRequired(op_index)) {
    markOperatorFinished(op_index, part_id);
    return;
  }

  if (has_repartitions_[op_index]) {
    LOG(INFO) << "initiateRebuild for Operator " << op_index << ", Partition " << part_id;
    for (partition_id part_id = 0; part_id < input_num_partitions_[op_index]; ++part_id) {
      if (!checkNormalExecutionOver(op_index, part_id)) {
        return;
      }
    }

    if (initiateRebuild(op_index)) {
      // Rebuild initiated and completed right away.
      markOperatorFinished(op_index);
    }
  } else {
    DCHECK(!checkRebuildInitiated(op_index, part_id));
    if (initiateRebuild(op_index, part_id)) {
      // Rebuild initiated and completed right away.
      markOperatorFinished(op_index, part_id);
    }
  }
}

void QueryManagerBase::processRebuildWorkOrderCompleteMessage(const dag_node_index op_index,
                                                              const partition_id part_id) {
  query_exec_state_->decrementNumRebuildWorkOrders(op_index, part_id);

  if (!checkRebuildOver(op_index, part_id)) {
    return;
  }

  if (!has_repartitions_[op_index]) {
    markOperatorFinished(op_index, part_id);
    return;
  }

  const std::size_t num_rebuild_workorders = query_exec_state_->getNumRebuildWorkOrders(op_index);
  if (num_rebuild_workorders == 0) {
    markOperatorFinished(op_index);
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
    if (checkAllBlockingDependenciesMet(consumer_index)) {
      fetchNormalWorkOrders(consumer_index, part_id);
    }
  }
}

void QueryManagerBase::markOperatorFinished(const dag_node_index index) {
  query_exec_state_->setExecutionFinished(index);
  markOperatorFinishedHelper(index);
}

void QueryManagerBase::markOperatorFinishedHelper(const dag_node_index index) {
  LOG(INFO) << "markOperatorFinished Operator " << index;
  for (const dag_node_index dependent_op_index : blocking_dependents_[index]) {
    for (partition_id part_id = 0; part_id < blocking_dependencies_[dependent_op_index].size(); ++part_id) {
      blocking_dependencies_[dependent_op_index][part_id].erase(index);
    }
  }

  for (const dag_node_index dependent_op_index : output_consumers_[index]) {
    non_blocking_dependencies_[dependent_op_index].erase(index);
  }

  RelationalOperator *op = query_dag_->getNodePayloadMutable(index);
  op->updateCatalogOnCompletion();

  const relation_id output_rel = op->getOutputRelationID();

  for (const pair<dag_node_index, bool> &dependent_link : query_dag_->getDependents(index)) {
    const dag_node_index dependent_op_index = dependent_link.first;

    if (output_rel >= 0) {
      RelationalOperator *dependent_op = query_dag_->getNodePayloadMutable(dependent_op_index);
      for (partition_id part_id = 0; part_id < output_num_partitions_[index]; ++part_id) {
        // Signal dependent operator that current operator is done feeding input blocks.
        dependent_op->doneFeedingInputBlocks(output_rel, part_id);
      }
    }

    if (checkAllBlockingDependenciesMet(dependent_op_index)) {
      // Process the dependent operator (of the operator whose WorkOrder
      // was just executed) for which all the dependencies have been met.
      if (!fetchNormalWorkOrders(dependent_op_index) &&
          non_blocking_dependencies_[dependent_op_index].empty() &&
          checkNormalExecutionOver(dependent_op_index) &&
          (!checkRebuildRequired(dependent_op_index) || initiateRebuild(dependent_op_index))) {
        markOperatorFinished(dependent_op_index);
      }
    }
  }
}

void QueryManagerBase::markOperatorFinished(const dag_node_index index,
                                            const partition_id part_id) {
  LOG(INFO) << "markOperatorFinished Operator " << index << ", Partition " << part_id;
  if (query_exec_state_->setExecutionFinished(index, part_id)) {
    markOperatorFinishedHelper(index);
    return;
  }

  RelationalOperator *op = query_dag_->getNodePayloadMutable(index);
  const relation_id output_rel = op->getOutputRelationID();

  for (const pair<dag_node_index, bool> &dependent_link : query_dag_->getDependents(index)) {
    const dag_node_index dependent_op_index = dependent_link.first;
    if (has_repartitions_[index] ||
        input_num_partitions_[index] != input_num_partitions_[dependent_op_index]) {
      continue;
    }

    if (output_rel >= 0) {
      // Signal dependent operator that current operator is done feeding input blocks.
      query_dag_->getNodePayloadMutable(dependent_op_index)->doneFeedingInputBlocks(output_rel, part_id);
    }

    const auto cit = blocking_dependencies_[dependent_op_index][part_id].find(index);
    if (cit != blocking_dependencies_[dependent_op_index][part_id].end()) {
      blocking_dependencies_[dependent_op_index][part_id].erase(cit);
    }
    if (checkAllBlockingDependenciesMet(dependent_op_index, part_id)) {
      fetchNormalWorkOrders(dependent_op_index, part_id);
    }
  }
}

}  // namespace quickstep
