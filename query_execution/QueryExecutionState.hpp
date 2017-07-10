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

#ifndef QUICKSTEP_QUERY_EXECUTION_QUERY_EXECUTION_STATE_HPP_
#define QUICKSTEP_QUERY_EXECUTION_QUERY_EXECUTION_STATE_HPP_

#include <cstddef>
#include <unordered_map>

#include "query_optimizer/QueryOptimizerConfig.h"  // For QUICKSTEP_DISTRIBUTED.
#ifdef QUICKSTEP_DISTRIBUTED
#include <unordered_set>
#endif  // QUICKSTEP_DISTRIBUTED

#include <utility>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

/**
 * @brief A class that tracks the state of the execution of a query which
 *        includes status of operators, number of dispatched work orders etc.
 **/
class QueryExecutionState {
 public:
  /**
   * @brief Constructor.
   *
   * @param num_operators Number of relational operators in the query.
   **/
  QueryExecutionState(const std::size_t num_operators,
                      const std::vector<std::size_t> &input_num_partitions)
      : num_operators_(num_operators),
        num_operators_finished_(0),
        queued_workorders_per_op_(num_operators),
        rebuild_required_(num_operators, false),
        done_gen_(num_operators),
        execution_finished_(num_operators) {
    DCHECK_EQ(num_operators, input_num_partitions.size());
    for (std::size_t operator_index = 0; operator_index < num_operators; ++operator_index) {
      const std::size_t operator_num_partitions = input_num_partitions[operator_index];

      queued_workorders_per_op_[operator_index].resize(operator_num_partitions, 0);
      done_gen_[operator_index].resize(operator_num_partitions, false);
      execution_finished_[operator_index].resize(operator_num_partitions, false);
    }
  }

  /**
   * @brief Get the number of operators in the query.
   **/
  inline const std::size_t getNumOperators() const {
    return num_operators_;
  }

  /**
   * @brief Get the number of operators who have finished their execution.
   **/
  inline const std::size_t getNumOperatorsFinished() const {
    return num_operators_finished_;
  }

  /**
   * @brief Check if the query has finished its execution.
   *
   * @return True if the query has finished its execution, false otherwise.
   **/
  inline bool hasQueryExecutionFinished() const {
    return num_operators_finished_ == num_operators_;
  }

  /**
   * @brief Set the rebuild status of the given operator that includes the
   *        flag for whether the rebuild has been initiated and if so, the
   *        number of pending rebuild work orders.
   *
   * @param operator_index The index of the given operator.
   * @param num_rebuild_workorders The number of rebuild workorders of the given
   *        operator.
   * @param rebuild_initiated True if the rebuild has been initiated, false
   *        otherwise.
   **/
  inline void setRebuildStatus(const std::size_t operator_index,
                               const partition_id part_id,
                               const std::size_t num_rebuild_workorders,
                               const bool rebuild_initiated) {
    DCHECK(operator_index < num_operators_);
    auto search_res = rebuild_status_.find(operator_index);
    DCHECK(search_res != rebuild_status_.end());

    search_res->second.has_initiated[part_id] = rebuild_initiated;
    search_res->second.num_pending_workorders[part_id] = num_rebuild_workorders;
  }

#ifdef QUICKSTEP_DISTRIBUTED
  /**
   * @brief Update the rebuild status of the given operator the number of
   *        pending rebuild work orders, after the rebuild has been initiated.
   *
   * @param operator_index The index of the given operator.
   * @param num_rebuild_workorders The number of rebuild workorders of the given
   *        operator.
   * @param shiftboss_index The index of the Shiftboss that rebuilt.
   **/
  void updateRebuildStatus(const std::size_t operator_index,
                           const partition_id part_id,
                           const std::size_t num_rebuild_workorders,
                           const std::size_t shiftboss_index) {
    DCHECK_LT(operator_index, num_operators_);
    auto search_res = rebuild_status_.find(operator_index);
    DCHECK(search_res != rebuild_status_.end() && search_res->second.has_initiated[part_id]);
    search_res->second.num_pending_workorders[part_id] += num_rebuild_workorders;
    search_res->second.rebuilt_shiftboss_indexes.insert(shiftboss_index);
  }

  /**
   * @brief Check if the rebuild has been finished for the given operator.
   *
   * @param operator_index The index of the given operator.
   * @param num_shiftbosses The number of the Shiftbosses for rebuilt.
   *
   * @return True if the rebuild has been finished, false otherwise.
   **/
  inline bool hasRebuildFinished(const std::size_t operator_index,
                                 const partition_id part_id,
                                 const std::size_t num_shiftbosses) const {
    DCHECK_LT(operator_index, num_operators_);
    const auto search_res = rebuild_status_.find(operator_index);
    DCHECK(search_res != rebuild_status_.end());

    const auto &rebuild_status = search_res->second;
    DCHECK(rebuild_status.has_initiated[part_id]);

    return rebuild_status.rebuilt_shiftboss_indexes.size() == num_shiftbosses &&
           rebuild_status.num_pending_workorders[part_id] == 0u;
  }

#endif  // QUICKSTEP_DISTRIBUTED

  /**
   * @brief Check if the rebuild has been initiated for the given operator.
   *
   * @param operator_index The index of the given operator.
   *
   * @return True if the rebuild has been initiated, false otherwise.
   **/
  inline bool hasRebuildInitiated(const std::size_t operator_index,
                                  const partition_id part_id) const {
    DCHECK_LT(operator_index, num_operators_);
    const auto search_res = rebuild_status_.find(operator_index);
    if (search_res != rebuild_status_.end()) {
      return search_res->second.has_initiated[part_id];
    }
    return false;
  }

  /**
   * @brief Get the number of pending rebuild workorders for the given operator.
   *
   * @param operator_index The index of the given operator.
   *
   * @return The number of pending rebuild workorders for the given operator.
   **/
  inline const std::size_t getNumRebuildWorkOrders(
      const std::size_t operator_index,
      const partition_id part_id) const {
    DCHECK(operator_index < num_operators_);
    const auto search_res = rebuild_status_.find(operator_index);
    DCHECK(search_res != rebuild_status_.end());
    return search_res->second.num_pending_workorders[part_id];
  }

  /**
   * @brief Increment the number of rebuild WorkOrders for the given operator.
   *
   * @param operator_index The index of the given operator.
   * @param num_rebuild_workorders The number of rebuild workorders of the given
   *        operator.
   **/
  inline void incrementNumRebuildWorkOrders(const std::size_t operator_index,
                                            const partition_id part_id,
                                            const std::size_t num_rebuild_workorders) {
    DCHECK_LT(operator_index, num_operators_);
    auto search_res = rebuild_status_.find(operator_index);
    DCHECK(search_res != rebuild_status_.end())
        << "Called for an operator whose rebuild status does not exist.";
    DCHECK(search_res->second.has_initiated[part_id]);

    search_res->second.num_pending_workorders[part_id] += num_rebuild_workorders;
  }

  /**
   * @brief Decrement the number of rebuild WorkOrders for the given operator.
   *
   * @param operator_index The index of the given operator.
   **/
  inline void decrementNumRebuildWorkOrders(const std::size_t operator_index,
                                            const partition_id part_id) {
    DCHECK(operator_index < num_operators_);
    auto search_res = rebuild_status_.find(operator_index);
    CHECK(search_res != rebuild_status_.end())
        << "Called QueryExecutionState::decrementNumRebuildWorkOrders() for an "
           "operator whose rebuild entry doesn't exist.";

    DCHECK(search_res->second.has_initiated[part_id]);
    DCHECK_GE(search_res->second.num_pending_workorders[part_id], 1u);

    --(search_res->second.num_pending_workorders[part_id]);
  }

  /**
   * @brief Increment the number of queued (normal) WorkOrders for the given
   *        operator.
   *
   * @param operator_index The index of the given operator.
   **/
  inline void incrementNumQueuedWorkOrders(const std::size_t operator_index,
                                           const partition_id part_id) {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_LT(part_id, queued_workorders_per_op_[operator_index].size());
    ++queued_workorders_per_op_[operator_index][part_id];
  }

  /**
   * @brief Decrement the number of queued (normal) WorkOrders for the given
   *        operator.
   *
   * @param operator_index The index of the given operator.
   **/
  inline void decrementNumQueuedWorkOrders(const std::size_t operator_index,
                                           const partition_id part_id) {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_LT(part_id, queued_workorders_per_op_[operator_index].size());
    DCHECK_GT(queued_workorders_per_op_[operator_index][part_id], 0u);
    --queued_workorders_per_op_[operator_index][part_id];
  }

  /**
   * @brief Get the number of queued (normal) WorkOrders for the given operator.
   *
   * @note Queued WorkOrders mean those WorkOrders which have been dispatched
   *       for execution by the Foreman and haven't yet completed. These are
   *       different from pending WorkOrders which mean the WorkOrders that
   *       haven't been dispatched for execution yet.
   *
   * @param operator_index The index of the given operator.
   *
   * @return The number of queued (normal) WorkOrders for the given operators.
   **/
  inline const std::size_t getNumQueuedWorkOrders(
      const std::size_t operator_index,
      const partition_id part_id) const {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_LT(part_id, queued_workorders_per_op_[operator_index].size());
    return queued_workorders_per_op_[operator_index][part_id];
  }

  /**
   * @brief Set the rebuild required flag as true for the given operator.
   *
   * @param operator_index The index of the given operator.
   **/
  inline void setRebuildRequired(const std::size_t operator_index,
                                 const std::size_t output_num_partitions) {
    DCHECK_LT(operator_index, num_operators_);
    rebuild_required_[operator_index] = true;

    rebuild_status_.emplace(operator_index,
                            RebuildStatus(output_num_partitions));
  }

  /**
   * @brief Get the rebuild required flag for the given operator.
   *
   * @param operator_index The index of the given operator.
   **/
  inline bool isRebuildRequired(const std::size_t operator_index) const {
    DCHECK(operator_index < num_operators_);
    return rebuild_required_[operator_index];
  }

  /**
   * @brief Set the execution finished flag for the given operator as true.
   *
   * @note By default this flag is false.
   *
   * @param operator_index The index of the given operator.
   **/
  inline bool setExecutionFinished(const std::size_t operator_index,
                                   const partition_id part_id) {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_LT(part_id, execution_finished_[operator_index].size());
    execution_finished_[operator_index][part_id] = true;

    if (hasExecutionFinished(operator_index)) {
      ++num_operators_finished_;
      return true;
    }

    return false;
  }

  /**
   * @brief Get the execution finished flag for the given operator.
   *
   * @param operator_index The index of the given operator.
   **/
  inline bool hasExecutionFinished(const std::size_t operator_index) const {
    DCHECK_LT(operator_index, num_operators_);
    const std::size_t input_num_partitions = execution_finished_[operator_index].size();

    for (partition_id part_id = 0; part_id < input_num_partitions; ++part_id) {
      if (!execution_finished_[operator_index][part_id]) {
        return false;
      }
    }
    return true;
  }

  /**
   * @brief Get the execution finished flag for the given operator.
   *
   * @param operator_index The index of the given operator.
   **/
  inline bool hasExecutionFinished(const std::size_t operator_index,
                                   const partition_id part_id) const {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_LT(part_id, execution_finished_[operator_index].size());
    return execution_finished_[operator_index][part_id];
  }

  /**
   * @brief Set the "done generation of workorders" flag as true for the given
   *        operator.
   *
   * @note By default this flag is false.
   *
   * @param operator_index The index of the given operator.
   **/
  inline void setDoneGenerationWorkOrders(const std::size_t operator_index,
                                          const partition_id part_id) {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_LT(part_id, done_gen_[operator_index].size());
    done_gen_[operator_index][part_id] = true;
  }

  /**
   * @brief Get the "done generation of workorders" flag for the given operator.
   *
   * @param operator_index The index of the given operator.
   **/
  inline bool hasDoneGenerationWorkOrders(const std::size_t operator_index,
                                          const partition_id part_id) const {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_LT(part_id, done_gen_[operator_index].size());
    return done_gen_[operator_index][part_id];
  }

 private:
  // Total number of operators in the query.
  const std::size_t num_operators_;

  // Number of operators who've finished their execution.
  std::size_t num_operators_finished_;

  // A vector to track the number of partitioned normal WorkOrders in execution.
  std::vector<std::vector<std::size_t>> queued_workorders_per_op_;

  // The ith bit denotes if the operator with ID = i requires generation of
  // rebuild WorkOrders.
  std::vector<bool> rebuild_required_;

  // The ith bit denotes if the operator with ID = i has finished generating
  // work orders.
  std::vector<std::vector<bool>> done_gen_;

  // execution_finished_[i][j]:
  // The ith bit denotes if the operator with ID = i has finished its execution.
  // The jth bit denotes if the partition with ID = j has finished its execution.
  std::vector<std::vector<bool>> execution_finished_;

  struct RebuildStatus {
    explicit RebuildStatus(const std::size_t output_num_partitions)
        : has_initiated(output_num_partitions),
          num_pending_workorders(output_num_partitions) {}

    // Whether rebuild for operator at index i has been initiated.
    std::vector<bool> has_initiated;

    // The number of pending rebuild workorders for the operator.
    // Valid if and only if 'has_initiated' is true.
    std::vector<std::size_t> num_pending_workorders;

#ifdef QUICKSTEP_DISTRIBUTED
    std::unordered_set<std::size_t> rebuilt_shiftboss_indexes;
#endif  // QUICKSTEP_DISTRIBUTED
  };

  // Key is dag_node_index for which rebuild is required.
  std::unordered_map<std::size_t, RebuildStatus> rebuild_status_;

  DISALLOW_COPY_AND_ASSIGN(QueryExecutionState);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_QUERY_EXECUTION_STATE_HPP_
