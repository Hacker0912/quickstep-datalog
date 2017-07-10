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
      output_consumers_(num_operators_in_dag_),
      blocking_dependencies_(num_operators_in_dag_) {
  if (FLAGS_visualize_execution_dag) {
    dag_visualizer_ =
        std::make_unique<quickstep::ExecutionDAGVisualizer>(query_handle_->getQueryPlan());
  }

  for (dag_node_index node_index = 0;
       node_index < num_operators_in_dag_;
       ++node_index) {
    const RelationalOperator &op = query_dag_->getNodePayload(node_index);
    input_num_partitions_[node_index] = op.getNumPartitions();

    const QueryContext::insert_destination_id insert_destination_index =
        op.getInsertDestinationID();
    if (insert_destination_index != QueryContext::kInvalidInsertDestinationId) {
      // Rebuild is necessary whenever InsertDestination is present.
      output_num_partitions_.emplace(node_index, op.getOutputNumPartitions());
    }

    for (const pair<dag_node_index, bool> &dependent_link :
         query_dag_->getDependents(node_index)) {
      const dag_node_index dependent_op_index = dependent_link.first;
      if (!query_dag_->getLinkMetadata(node_index, dependent_op_index)) {
        // The link is not a pipeline-breaker. Streaming of blocks is possible
        // between these two operators.
        output_consumers_[node_index].push_back(dependent_op_index);
      } else {
        // The link is a pipeline-breaker. Streaming of blocks is not possible
        // between these two operators.
        blocking_dependencies_[dependent_op_index].push_back(node_index);
      }
    }
  }

  query_exec_state_ = std::make_unique<QueryExecutionState>(num_operators_in_dag_, input_num_partitions_);
  for (const auto output_num_partitions_pair : output_num_partitions_) {
    query_exec_state_->setRebuildRequired(output_num_partitions_pair.first,
                                          output_num_partitions_pair.second);
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

bool QueryManagerBase::processRebuild(const dag_node_index index,
                                      const partition_id part_id) {
  if (input_num_partitions_[index] == output_num_partitions_[index]) {
    if (!checkRebuildInitiated(index, part_id)) {
      if (initiateRebuild(index, part_id)) {
        // Rebuild initiated and completed right away.
        markOperatorFinished(index, part_id);
      } else {
        // Rebuild under progress.
        return true;
      }
    } else if (checkRebuildOver(index, part_id)) {
      // Rebuild was under progress and now it is over.
      markOperatorFinished(index, part_id);
    }
  } else if (checkNormalExecutionOver(index)) {
    if (!checkRebuildInitiated(index, 0)) {
      // Rebuild hasn't started, initiate it.
      if (initiateRebuild(index)) {
        // Rebuild initiated and completed right away.
        markOperatorFinished(index);
      } else {
        // Rebuild under progress.
        return true;
      }
    } else if (checkRebuildOver(index)) {
      // Rebuild had been initiated and it is over.
      markOperatorFinished(index);
    }
  }

  return false;
}

void QueryManagerBase::processWorkOrderCompleteMessage(
    const dag_node_index op_index,
    const partition_id part_id) {
  query_exec_state_->decrementNumQueuedWorkOrders(op_index, part_id);

  // Check if new work orders are available and fetch them if so.
  fetchNormalWorkOrders(op_index, part_id);

  if (checkRebuildRequired(op_index)) {
    if (checkNormalExecutionOver(op_index, part_id)) {
      if (processRebuild(op_index, part_id)) {
        return;
      }
    } else {
      // Normal execution under progress for this operator.
    }
  } else if (checkOperatorExecutionOver(op_index, part_id)) {
    // Rebuild not required for this operator and its normal execution is
    // complete.
    markOperatorFinished(op_index, part_id);
  }

  processDependentOperators(op_index);
}

void QueryManagerBase::processRebuildWorkOrderCompleteMessage(const dag_node_index op_index,
                                                              const partition_id part_id) {
  query_exec_state_->decrementNumRebuildWorkOrders(op_index, part_id);

  if (input_num_partitions_[op_index] == output_num_partitions_[op_index]) {
    if (checkRebuildOver(op_index, part_id)) {
      markOperatorFinished(op_index, part_id);

      processDependentOperators(op_index);
    }
  } else if (checkRebuildOver(op_index)) {
    // Rebuild had been initiated and it is over.
    markOperatorFinished(op_index);

    processDependentOperators(op_index);
  }
}

void QueryManagerBase::processOperator(const dag_node_index index,
                                       const partition_id part_id,
                                       const bool recursively_check_dependents) {
  if (fetchNormalWorkOrders(index, part_id)) {
    // Fetched work orders. Return to wait for the generated work orders to
    // execute, and skip the execution-finished checks.
    return;
  }

  if (checkNormalExecutionOver(index, part_id)) {
    if (checkRebuildRequired(index)) {
      if (processRebuild(index, part_id)) {
        return;
      }
    } else {
      // Rebuild is not required and normal execution over, mark finished.
      markOperatorFinished(index, part_id);
    }
    // If we reach here, that means the operator has been marked as finished.
    if (recursively_check_dependents) {
      processDependentOperators(index);
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
    fetchNormalWorkOrders(consumer_index, part_id);
  }
}

void QueryManagerBase::markOperatorFinished(const dag_node_index index,
                                            const partition_id part_id) {
  DCHECK_LT(index, num_operators_in_dag_);
  if (query_exec_state_->setExecutionFinished(index, part_id)) {
    RelationalOperator *op = query_dag_->getNodePayloadMutable(index);
    op->updateCatalogOnCompletion();

    const relation_id output_rel = op->getOutputRelationID();
    for (const pair<dag_node_index, bool> &dependent_link : query_dag_->getDependents(index)) {
      const dag_node_index dependent_op_index = dependent_link.first;
      RelationalOperator *dependent_op = query_dag_->getNodePayloadMutable(dependent_op_index);

      for (partition_id dependent_part_id = 0;
           dependent_part_id < input_num_partitions_[dependent_op_index];
           ++dependent_part_id) {
        // Signal dependent operator that current operator is done feeding input blocks.
        if (output_rel >= 0) {
          dependent_op->doneFeedingInputBlocks(output_rel, dependent_part_id);
        }
        if (checkAllBlockingDependenciesMet(dependent_op_index, dependent_part_id)) {
          dependent_op->informAllBlockingDependenciesMet(dependent_part_id);
        }
      }
    }
  } else {
    const relation_id output_rel = query_dag_->getNodePayload(index).getOutputRelationID();
    for (const pair<dag_node_index, bool> &dependent_link : query_dag_->getDependents(index)) {
      const dag_node_index dependent_op_index = dependent_link.first;

      if (input_num_partitions_[dependent_op_index] != input_num_partitions_[index]) {
        continue;
      }

      RelationalOperator *dependent_op = query_dag_->getNodePayloadMutable(dependent_op_index);

      // Signal dependent operator that current operator is done feeding input blocks.
      if (output_rel >= 0) {
        dependent_op->doneFeedingInputBlocks(output_rel, part_id);
      }
      if (checkAllBlockingDependenciesMet(dependent_op_index, part_id)) {
        dependent_op->informAllBlockingDependenciesMet(part_id);
      }
    }
  }
}

void QueryManagerBase::markOperatorFinished(const dag_node_index index) {
  DCHECK_LT(index, num_operators_in_dag_);
  for (partition_id part_id = 0; part_id < input_num_partitions_[index]; ++part_id) {
    query_exec_state_->setExecutionFinished(index, part_id);
  }

  RelationalOperator *op = query_dag_->getNodePayloadMutable(index);
  op->updateCatalogOnCompletion();

  const relation_id output_rel = op->getOutputRelationID();
  for (const pair<dag_node_index, bool> &dependent_link : query_dag_->getDependents(index)) {
    const dag_node_index dependent_op_index = dependent_link.first;
    RelationalOperator *dependent_op = query_dag_->getNodePayloadMutable(dependent_op_index);

    for (partition_id dependent_part_id = 0;
         dependent_part_id < input_num_partitions_[dependent_op_index];
         ++dependent_part_id) {
      // Signal dependent operator that current operator is done feeding input blocks.
      if (output_rel >= 0) {
        dependent_op->doneFeedingInputBlocks(output_rel, dependent_part_id);
      }
      if (checkAllBlockingDependenciesMet(dependent_op_index, dependent_part_id)) {
        dependent_op->informAllBlockingDependenciesMet(dependent_part_id);
      }
    }
  }
}

}  // namespace quickstep
