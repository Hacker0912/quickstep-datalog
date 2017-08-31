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

#ifndef QUICKSTEP_QUERY_EXECUTION_WORKORDERS_CONTAINER_HPP_
#define QUICKSTEP_QUERY_EXECUTION_WORKORDERS_CONTAINER_HPP_

#include <cstddef>
#include <list>
#include <memory>
#include <queue>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/WorkOrderSelectionPolicy.hpp"
#include "relational_operators/WorkOrder.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

 /**
  * @brief A container to hold the WorkOrders for a given query.
  * @note This container stays alive during the lifetime of the query.
  * @note The NUMA node indexing is assumed to start from 0.
  **/
class WorkOrdersContainer {
 public:
  /**
   * @brief Constructor
   *
   * @param num_operators Number of operators in the query DAG.
   * @param num_numa_nodes Number of NUMA nodes in the system.
   **/
  WorkOrdersContainer(const std::size_t num_operators,
                      const std::size_t num_numa_nodes,
                      const std::vector<std::size_t> &input_num_partitions,
                      const std::vector<std::size_t> &output_num_partitions)
      : num_operators_(num_operators),
        num_numa_nodes_(num_numa_nodes),
        normal_work_orders_policy_(input_num_partitions),
        has_ever_normal_workorders_(num_operators) {
    DEBUG_ASSERT(num_operators != 0);
    for (std::size_t op = 0; op < num_operators; ++op) {
      normal_workorders_.push_back(
          new PartitionedOperatorWorkOrdersContainer(input_num_partitions[op]));
      rebuild_workorders_.push_back(
          new PartitionedOperatorWorkOrdersContainer(output_num_partitions[op]));

      has_ever_normal_workorders_[op].resize(input_num_partitions[op]);
    }
  }

  /**
   * @brief Destructor.
   *
   * @note If the query is executed normally, we should never encounter a
   *       situation where at the time of deletion the WorkOrdersContainer has
   *       pending WorkOrders.
   **/
  ~WorkOrdersContainer();

  /**
   * @brief Check if there are some pending WorkOrders for the given operator.
   *
   * @param operator_index Index of the operator.
   *
   * @return If there are pending WorkOrders.
   **/
  inline bool hasNormalWorkOrder(const std::size_t operator_index,
                                 const partition_id part_id) const {
    DCHECK_LT(operator_index, num_operators_);
    return normal_workorders_[operator_index].hasWorkOrder(part_id);
  }

  /**
   * @brief Check if there are some pending normal (i.e. non-rebuild) WorkOrders
   *        for the given operator which prefer the specified NUMA node.
   *
   * @param operator_index Index of the operator.
   * @param numa_node_id The NUMA node.
   *
   * @return If there are pending WorkOrders for the given operator which prefer
   *         numa_node_id.
   **/
  inline bool hasNormalWorkOrderForNUMANode(
      const std::size_t operator_index, const int numa_node_id) const {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_GE(numa_node_id, 0);
    DCHECK_LT(static_cast<std::size_t>(numa_node_id), num_numa_nodes_);
    return false;
    /*
    return normal_workorders_[operator_index].hasWorkOrderForNUMANode(
        numa_node_id);
        */
  }

  /**
   * @brief Check if there are some pending rebuild WorkOrders for the given
   *        operator.
   *
   * @param operator_index Index of the operator.
   *
   * @return If there are pending rebuild WorkOrders.
   **/
  inline bool hasRebuildWorkOrder(const std::size_t operator_index) const {
    DCHECK_LT(operator_index, num_operators_);
    return rebuild_workorders_[operator_index].hasWorkOrder();
  }

  inline bool hasRebuildWorkOrder(const std::size_t operator_index,
                                  const partition_id part_id) const {
    DCHECK_LT(operator_index, num_operators_);
    return rebuild_workorders_[operator_index].hasWorkOrder(part_id);
  }

  /**
   * @brief Check if there are some pending rebuild WorkOrders for the given
   *        operator which prefer the specified NUMA node.
   *
   * @param operator_index Index of the operator.
   * @param numa_node_id The NUMA node.
   *
   * @return If there are pending rebuild WorkOrders for the given operator which
   *         prefer numa_node_id.
   **/
  inline bool hasRebuildWorkOrderForNUMANode(
      const std::size_t operator_index, const int numa_node_id) const {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_GE(numa_node_id, 0);
    DCHECK_LT(static_cast<std::size_t>(numa_node_id), num_numa_nodes_);
    return false;
    /*
    return rebuild_workorders_[operator_index].hasWorkOrderForNUMANode(
        numa_node_id);
        */
  }

  /**
   * @brief Get a normal (non-rebuild) WorkOrder for a given operator which
   *        prefer the given NUMA node.
   *
   * @param operator_index The index of the operator.
   * @param numa_node_id The NUMA node.
   *
   * @return A WorkOrder which prefers numa_node_id. If no such WorkOrder is
   *         available, return nullptr. The caller is responsible for taking the
   *         ownership.
   **/
  WorkOrder* getNormalWorkOrderForNUMANode(const std::size_t operator_index,
                                           const int numa_node_id) {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_GE(numa_node_id, 0);
    DCHECK_LT(static_cast<std::size_t>(numa_node_id), num_numa_nodes_);
    return nullptr;
    /*
    return normal_workorders_[operator_index].getWorkOrderForNUMANode(
        numa_node_id);
        */
  }

  /**
   * @brief Get a normal (non-rebuild) WorkOrder for a given operator.
   *
   * @param operator_index The index of the operator.
   * @param prefer_single_NUMA_node If true, first try to get workorders which
   *        prefer exactly one NUMA node over workorders which list more than
   *        one NUMA node as their preference.
   *
   * @return A WorkOrder. If no WorkOrder is available, returns nullptr. The
   *         caller is responsible for taking the ownership.
   **/
  WorkOrder* getNormalWorkOrder(const std::size_t operator_index,
                                const partition_id part_id,
                                const bool prefer_single_NUMA_node = true) {
    DCHECK_LT(operator_index, num_operators_);
    return normal_workorders_[operator_index].getWorkOrder(part_id);
  }

  WorkOrder* getNextWorkOrder(std::size_t *operator_index, partition_id *part_id,
                              bool *is_rebuild) {
    if (rebuild_work_orders_policy_.hasWorkOrder()) {
      rebuild_work_orders_policy_.getOperatorIndexForNextWorkOrder(operator_index, part_id);
      *is_rebuild = true;
      return rebuild_workorders_[*operator_index].getWorkOrder(*part_id);
    }

    if (normal_work_orders_policy_.hasWorkOrder()) {
      normal_work_orders_policy_.getOperatorIndexForNextWorkOrder(operator_index, part_id);
      *is_rebuild = false;
      return normal_workorders_[*operator_index].getWorkOrder(*part_id);
    }

    return nullptr;
  }

  /**
   * @brief Get a rebuild WorkOrder for a given operator whch prefer the
   *        specified NUMA node.
   *
   * @param operator_index The index of the operator.
   * @param numa_node_id The NUMA node.
   *
   * @return A WorkOrder that prefers numa_node_id. If no such WorkOrder is
   *         available, return nullptr. The caller is responsible for taking the
   *         ownership.
   **/
  WorkOrder* getRebuildWorkOrderForNUMANode(const std::size_t operator_index,
                                            const int numa_node_id) {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_GE(numa_node_id, 0);
    DCHECK_LT(static_cast<std::size_t>(numa_node_id), num_numa_nodes_);
    return nullptr;
    /*
    return rebuild_workorders_[operator_index].getWorkOrderForNUMANode(
        numa_node_id);
        */
  }

  /**
   * @brief Get a rebuild WorkOrder for the given operator
   *
   * @param operator_index The index of the operator.
   * @param prefer_single_NUMA_node If true, first try to get workorders which
   *        prefer exactly one NUMA node over workorders which list more than
   *        one NUMA node as their preference.
   *
   * @return A WorkOrder. If no WorkOrder is available, returns nullptr. The
   *         caller is responsible for taking the ownership.
   **/
  WorkOrder* getRebuildWorkOrder(const std::size_t operator_index,
                                 const partition_id part_id,
                                 const bool prefer_single_NUMA_node = true) {
    DCHECK_LT(operator_index, num_operators_);
    return rebuild_workorders_[operator_index].getWorkOrder(part_id);
  }

  /**
   * @brief Add a normal (non-rebuild) WorkOrder generated from a given
   *        operator.
   *
   * @note Take the ownership of \p workorder.
   * @note The workorder to be added contains information about its preferred
   *       NUMA nodes. This information is used to insert the WorkOrder
   *       appropriately.
   *
   * @param workorder A pointer to the WorkOrder to be added.
   * @param operator_index The index of the operator in the query DAG.
   **/
  void addNormalWorkOrder(WorkOrder *workorder, const std::size_t operator_index,
                          const partition_id part_id) {
    LOG(INFO) << "addNormalWorkOrder from Operator " << operator_index << ", Partition " << part_id;
    DCHECK(workorder != nullptr);
    DCHECK_LT(operator_index, num_operators_);
    normal_workorders_[operator_index].addWorkOrder(workorder, part_id);
    normal_work_orders_policy_.addWorkOrder(operator_index, part_id);

    has_ever_normal_workorders_[operator_index][part_id] = true;
  }

  /**
   * @brief Add a rebuild WorkOrder generated from a given operator.
   *
   * @note Take the ownership of \p workorder.
   * @note The workorder to be added contains information about its preferred
   *       NUMA nodes. This information is used to insert the WorkOrder
   *       appropriately.
   *
   * @param workorder A pointer to the WorkOrder to be added.
   * @param operator_index The index of the operator in the query DAG.
   **/
  void addRebuildWorkOrder(WorkOrder *workorder,
                           const std::size_t operator_index,
                           const partition_id part_id) {
    LOG(INFO) << "addRebuildWorkOrder from Operator " << operator_index << ", Partition " << part_id;
    DCHECK(workorder != nullptr);
    DCHECK_LT(operator_index, num_operators_);
    rebuild_workorders_[operator_index].addWorkOrder(workorder, part_id);
    rebuild_work_orders_policy_.addWorkOrder(operator_index, part_id);
  }

  /**
   * @brief Get the number of pending normal WorkOrders for a given operator
   *        which prefer the specified NUMA node.
   *
   * @param operator_index The index of the operator.
   * @param numa_node_id The NUMA node.
   *
   * @return The number of pending WorkOrders which prefer numa_node_id.
   **/
  inline std::size_t getNumNormalWorkOrdersForNUMANode(
      const std::size_t operator_index, const int numa_node_id) const {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_GE(numa_node_id, 0);
    DCHECK_LT(static_cast<std::size_t>(numa_node_id), num_numa_nodes_);
    return 0;
    /*
    return normal_workorders_[operator_index].getNumWorkOrdersForNUMANode(
        numa_node_id);
        */
  }

  /**
   * @brief Get the number of all pending normal (i.e. non-rebuild) WorkOrders
   *        for a given operator.
   *
   * @param operator_index The index of the operator.
   *
   * @return The number of pending normal WorkOrders.
   **/
  inline std::size_t getNumNormalWorkOrders(
      const std::size_t operator_index) const {
    DCHECK_LT(operator_index, num_operators_);
    return normal_workorders_[operator_index].getNumWorkOrders();
  }

  inline std::size_t getNumNormalWorkOrders(
      const std::size_t operator_index,
      const partition_id part_id) const {
    DCHECK_LT(operator_index, num_operators_);
    return normal_workorders_[operator_index].getNumWorkOrders(part_id);
  }

  /**
   * @brief Get the number of pending rebuild WorkOrders for a given operator
   *        which prefer the specified NUMA node.
   *
   * @param operator_index The index of the operator.
   * @param numa_node_id The NUMA node.
   *
   * @return The number of pending WorkOrders which prefer numa_node_id.
   **/
  inline std::size_t getNumRebuildWorkOrdersForNUMANode(
      const std::size_t operator_index, const int numa_node_id) const {
    DCHECK_LT(operator_index, num_operators_);
    DCHECK_GE(numa_node_id, 0);
    DCHECK_LT(static_cast<std::size_t>(numa_node_id), num_numa_nodes_);
    return 0;
    /*
    return rebuild_workorders_[operator_index].getNumWorkOrdersForNUMANode(
        numa_node_id);
        */
  }

  /**
   * @brief Get the number of all pending rebuild WorkOrders for a given
   *        operator.
   *
   * @param operator_index The index of the operator.
   *
   * @return The number of pending rebuild WorkOrders.
   **/
  inline std::size_t getNumRebuildWorkOrders(
      const std::size_t operator_index,
      const partition_id part_id) const {
    DCHECK_LT(operator_index, num_operators_);
    return rebuild_workorders_[operator_index].getNumWorkOrders(part_id);
  }

  /**
   * @brief Get the total number of work orders for the given operator.
   *
   * @param operator_index The index of the operator.
   *
   * @return The total number of WorkOrders (normal + rebuild).
   **/
  inline std::size_t getNumTotalWorkOrders(
      const std::size_t operator_index) const {
    return getNumNormalWorkOrders(operator_index) +
           rebuild_workorders_[operator_index].getNumWorkOrders();
  }

  bool hasEverNormalWorkOrders(
      const std::size_t operator_index,
      const partition_id part_id) const {
    return has_ever_normal_workorders_[operator_index][part_id];
  }

 private:
  /**
   * @brief An internal queue-based container structure to hold the WorkOrders.
   **/
  class InternalQueueContainer {
   public:
    InternalQueueContainer() {
    }

    inline void addWorkOrder(WorkOrder *workorder) {
      workorders_.emplace(std::unique_ptr<WorkOrder>(workorder));
    }

    inline WorkOrder* getWorkOrder() {
      if (workorders_.empty()) {
        return nullptr;
      }

      WorkOrder *work_order = workorders_.front().release();
      workorders_.pop();
      return work_order;
    }

    inline bool hasWorkOrder() const {
      return !workorders_.empty();
    }

    inline std::size_t getNumWorkOrders() const {
      return workorders_.size();
    }

   private:
    std::queue<std::unique_ptr<WorkOrder>> workorders_;

    DISALLOW_COPY_AND_ASSIGN(InternalQueueContainer);
  };

  /**
   * @brief An internal list-based container structure to hold the WorkOrders.
   *
   * @note We use this class for WorkOrders whose inputs come from multiple NUMA
   *       nodes e.g. a HashJoinWorkOrder whose probe block and the hash table
   *       may reside on two different NUMA nodes.
   **/
  class InternalListContainer {
   public:
    InternalListContainer() {
    }

    inline void addWorkOrder(WorkOrder *workorder) {
      workorders_.emplace_back(std::unique_ptr<WorkOrder>(workorder));
    }

    inline WorkOrder* getWorkOrder() {
      if (workorders_.empty()) {
        return nullptr;
      }

      WorkOrder *work_order = workorders_.front().release();
      workorders_.pop_front();
      return work_order;
    }

    /**
     * @note This method has O(N) complexity.
     **/
    WorkOrder* getWorkOrderForNUMANode(const int numa_node);

    inline bool hasWorkOrder() const {
      return !workorders_.empty();
    }

    /**
     * @brief Check if numa_node is in the set of preferred NUMA nodes for at
     *        least one WorkOrder in this container.
     *
     * @note This method has O(N) complexity.
     **/
    bool hasWorkOrderForNUMANode(const int numa_node) const;

    inline std::size_t getNumWorkOrders() const {
      return workorders_.size();
    }

    /**
     * @brief Return the number of WorkOrders that list the numa_node as one of
     *        the preferred nodes of execution.
     **/
    std::size_t getNumWorkOrdersForNUMANode(const int numa_node) const;

   private:
    std::list<std::unique_ptr<WorkOrder>> workorders_;

    DISALLOW_COPY_AND_ASSIGN(InternalListContainer);
  };

  /**
   * @brief A container to hold all the WorkOrders generated by one operator.
   **/
  class OperatorWorkOrdersContainer {
   public:
    explicit OperatorWorkOrdersContainer(const std::size_t num_numa_nodes)
        : num_numa_nodes_(num_numa_nodes) {
      for (std::size_t numa_node = 0; numa_node < num_numa_nodes; ++numa_node) {
        single_numa_node_workorders_.push_back(new InternalQueueContainer());
      }
    }

    void addWorkOrder(WorkOrder *workorder);

    bool hasWorkOrderForNUMANode(const int numa_node_id) const {
      DCHECK_GE(numa_node_id, 0);
      DCHECK_LT(static_cast<std::size_t>(numa_node_id), num_numa_nodes_);
      return single_numa_node_workorders_[numa_node_id].hasWorkOrder() ||
             multiple_numa_nodes_workorders_.hasWorkOrderForNUMANode(
                 numa_node_id);
    }

    bool hasWorkOrder() const {
      if (!(numa_agnostic_workorders_.hasWorkOrder() ||
            multiple_numa_nodes_workorders_.hasWorkOrder())) {
        for (std::size_t i = 0; i < num_numa_nodes_; ++i) {
          if (hasWorkOrderForNUMANode(i)) {
            return true;
          }
        }
        return false;
      }
      return true;
    }

    std::size_t getNumWorkOrdersForNUMANode(
        const int numa_node_id) const {
      DCHECK_GE(numa_node_id, 0);
      DCHECK_LT(static_cast<std::size_t>(numa_node_id), num_numa_nodes_);
      return single_numa_node_workorders_[numa_node_id].getNumWorkOrders() +
             multiple_numa_nodes_workorders_.getNumWorkOrdersForNUMANode(
                 numa_node_id);
    }

    inline std::size_t getNumWorkOrders() const {
      std::size_t num_workorders = numa_agnostic_workorders_.getNumWorkOrders();
      // for each NUMA node involved ..
      for (PtrVector<InternalQueueContainer>::const_iterator it =
               single_numa_node_workorders_.begin();
           it != single_numa_node_workorders_.end();
           ++it) {
        // Add the number of workorders for the NUMA node.
        num_workorders += it->getNumWorkOrders();
      }
      // Add the number of workorders who have multiple NUMA nodes as input.
      num_workorders += multiple_numa_nodes_workorders_.getNumWorkOrders();
      return num_workorders;
    }

    WorkOrder* getWorkOrderForNUMANode(const int numa_node_id) {
      DCHECK_GE(numa_node_id, 0);
      DCHECK_LT(static_cast<std::size_t>(numa_node_id), num_numa_nodes_);
      WorkOrder *work_order = single_numa_node_workorders_[numa_node_id].getWorkOrder();
      if (work_order == nullptr) {
        work_order = multiple_numa_nodes_workorders_.getWorkOrderForNUMANode(
            numa_node_id);
      }
      return work_order;
    }

    WorkOrder* getWorkOrder(const bool prefer_single_NUMA_node = true);

   private:
    WorkOrder* getSingleNUMANodeWorkOrderHelper();

    const std::size_t num_numa_nodes_;

    // A container to store NUMA agnostic workorders.
    InternalQueueContainer numa_agnostic_workorders_;

    // A vector of containers to store workorders which prefer exactly one NUMA
    // node for execution.
    PtrVector<InternalQueueContainer> single_numa_node_workorders_;

    // A container to store workorders which prefer more than one NUMA node for
    // execution.
    InternalListContainer multiple_numa_nodes_workorders_;
    // Note that no workorder should be shared among the *_workorders_ structures.

    DISALLOW_COPY_AND_ASSIGN(OperatorWorkOrdersContainer);
  };

  /**
   * @brief A container to hold all the WorkOrders generated by one operator with partitions.
   **/
  class PartitionedOperatorWorkOrdersContainer {
   public:
    explicit PartitionedOperatorWorkOrdersContainer(const std::size_t num_partitions)
        : workorders_(num_partitions) {}

    void addWorkOrder(WorkOrder *workorder, const partition_id part_id) {
      DCHECK_LT(part_id, workorders_.size());
      workorders_[part_id].emplace(std::unique_ptr<WorkOrder>(workorder));
      ++workorders_count_;
    }

    bool hasWorkOrder() const {
      return workorders_count_ != 0u;
    }

    bool hasWorkOrder(const partition_id part_id) const {
      DCHECK_LT(part_id, workorders_.size());
      return !workorders_[part_id].empty();
    }

    std::size_t getNumWorkOrders() const {
      return workorders_count_;
    }

    std::size_t getNumWorkOrders(const partition_id part_id) const {
      DCHECK_LT(part_id, workorders_.size());
      return workorders_[part_id].size();
    }

    WorkOrder* getWorkOrder(const partition_id part_id) {
      DCHECK_LT(part_id, workorders_.size());
      if (hasWorkOrder(part_id)) {
        WorkOrder *workorder = workorders_[part_id].front().release();
        workorders_[part_id].pop();

        --workorders_count_;

        return workorder;
      }
      return nullptr;
    }

    std::size_t size() const {
      return workorders_.size();
    }

   private:
    // A vector of containers to store workorders per partition for execution.
    std::vector<std::queue<std::unique_ptr<WorkOrder>>> workorders_;
    std::size_t workorders_count_ = 0u;

    DISALLOW_COPY_AND_ASSIGN(PartitionedOperatorWorkOrdersContainer);
  };

  const std::size_t num_operators_;
  const std::size_t num_numa_nodes_;

  PtrVector<PartitionedOperatorWorkOrdersContainer> normal_workorders_;
  PtrVector<PartitionedOperatorWorkOrdersContainer> rebuild_workorders_;

  LifoWorkOrderSelectionPolicy normal_work_orders_policy_;
  FifoWorkOrderSelectionPolicy rebuild_work_orders_policy_;

  std::vector<std::vector<bool>> has_ever_normal_workorders_;

  DISALLOW_COPY_AND_ASSIGN(WorkOrdersContainer);
};

/** @} */

}  // namespace quickstep


#endif  // QUICKSTEP_QUERY_EXECUTION_WORKORDERS_CONTAINER_HPP_
