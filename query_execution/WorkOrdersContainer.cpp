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

#include "query_execution/WorkOrdersContainer.hpp"

#include <algorithm>
#include <cstddef>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "relational_operators/WorkOrder.hpp"

#include "glog/logging.h"

using std::string;
using std::to_string;
using std::unique_ptr;
using std::vector;

namespace quickstep {

WorkOrdersContainer::~WorkOrdersContainer() {
  // For each operator ..
  for (std::size_t op = 0; op < num_operators_; ++op) {
    bool has_work_order = false;
    for (partition_id part_id = 0; part_id < normal_workorders_[op].size(); ++part_id) {
      if (hasNormalWorkOrder(op, part_id)) {
        has_work_order = true;
        break;
      }
    }

    if (!has_work_order) {
      for (partition_id part_id = 0; part_id < rebuild_workorders_[op].size(); ++part_id) {
        if (hasRebuildWorkOrder(op, part_id)) {
          has_work_order = true;
          break;
        }
      }
    }

    if (has_work_order) {
      LOG(WARNING) << "Destroying a WorkOrdersContainer that still has pending WorkOrders.";
      break;
    }
  }
}

string WorkOrdersContainer::debugString() const {
  string result(1, '\n');
  for (std::size_t op = 0; op < num_operators_; ++op) {
    const std::size_t input_num_partitions = normal_workorders_[op].size();
    vector<std::size_t> num_normal_work_orders_per_partition(input_num_partitions);
    bool has_work_order = false;
    for (partition_id part_id = 0; part_id < input_num_partitions; ++part_id) {
      const std::size_t num_normal_work_orders = getNumNormalWorkOrders(op, part_id);
      num_normal_work_orders_per_partition[part_id] = num_normal_work_orders;

      if (!has_work_order && num_normal_work_orders != 0) {
        has_work_order = true;
      }
    }

    const std::size_t output_num_partitions = rebuild_workorders_[op].size();
    vector<std::size_t> num_rebuild_work_orders_per_partition(output_num_partitions);
    for (partition_id part_id = 0; part_id < output_num_partitions; ++part_id) {
      const std::size_t num_rebuild_work_orders = getNumRebuildWorkOrders(op, part_id);
      num_rebuild_work_orders_per_partition[part_id] = num_rebuild_work_orders;

      if (!has_work_order && num_rebuild_work_orders != 0) {
        has_work_order = true;
      }
    }

    if (has_work_order) {
      result += "\tOperator " + to_string(op) + ":\n";
      for (partition_id part_id = 0; part_id < input_num_partitions; ++part_id) {
        if (num_normal_work_orders_per_partition[part_id] != 0) {
          result += "\t\tInput Partition " + to_string(part_id) + ": "
            + to_string(num_normal_work_orders_per_partition[part_id]) + " normal work orders\n";
        }
      }

      for (partition_id part_id = 0; part_id < output_num_partitions; ++part_id) {
        if (num_rebuild_work_orders_per_partition[part_id] != 0) {
          result += "\t\tOutput Partition " + to_string(part_id) + ": "
            + to_string(num_rebuild_work_orders_per_partition[part_id]) + " rebuild work orders\n";
        }
      }
    }
  }

  return result;
}

WorkOrder* WorkOrdersContainer::InternalListContainer::getWorkOrderForNUMANode(
    const int numa_node) {
  for (std::list<unique_ptr<WorkOrder>>::iterator it = workorders_.begin();
       it != workorders_.end();
       ++it) {
    const std::vector<int> &numa_nodes = (*it)->getPreferredNUMANodes();
    if (!numa_nodes.empty()) {
      if (std::find(numa_nodes.begin(), numa_nodes.end(), numa_node) !=
          numa_nodes.end()) {
        WorkOrder *work_order = it->release();
        workorders_.erase(it);
        return work_order;
      }
    }
  }
  return nullptr;
}

void WorkOrdersContainer::OperatorWorkOrdersContainer::addWorkOrder(
    WorkOrder *workorder) {
  const std::vector<int> &numa_nodes = workorder->getPreferredNUMANodes();
  if (!numa_nodes.empty()) {
    if (numa_nodes.size() == 1) {
      // This WorkOrder prefers exactly one NUMA node.
      single_numa_node_workorders_[numa_nodes.front()].addWorkOrder(
          workorder);
    } else {
      // This WorkOrder prefers more than one NUMA node.
      multiple_numa_nodes_workorders_.addWorkOrder(workorder);
    }
  } else {
    numa_agnostic_workorders_.addWorkOrder(workorder);
  }
}

std::size_t
    WorkOrdersContainer::InternalListContainer::getNumWorkOrdersForNUMANode(
    const int numa_node) const {
  std::size_t num_workorders = 0;
  for (const unique_ptr<WorkOrder> &work_order : workorders_) {
    const std::vector<int> &numa_nodes = work_order->getPreferredNUMANodes();
    if (!numa_nodes.empty()) {
      std::vector<int>::const_iterator
          it = std::find(numa_nodes.begin(), numa_nodes.end(), numa_node);
      if (it != numa_nodes.end()) {
        // Found a match.
       ++num_workorders;
      }
    }
  }
  return num_workorders;
}

bool WorkOrdersContainer::InternalListContainer::hasWorkOrderForNUMANode(
    const int numa_node) const {
  for (const unique_ptr<WorkOrder> &work_order : workorders_) {
    const std::vector<int> &numa_nodes = work_order->getPreferredNUMANodes();
    if (!numa_nodes.empty()) {
      std::vector<int>::const_iterator
          it = std::find(numa_nodes.begin(), numa_nodes.end(), numa_node);
      if (it != numa_nodes.end()) {
        // Found a match.
        return true;
      }
    }
  }
  return false;
}

WorkOrder* WorkOrdersContainer::OperatorWorkOrdersContainer::getWorkOrder(
    const bool prefer_single_NUMA_node) {
  // This function tries to get any available WorkOrder.
  WorkOrder *workorder = numa_agnostic_workorders_.getWorkOrder();
  if (workorder == nullptr) {
    if (prefer_single_NUMA_node) {
      workorder = getSingleNUMANodeWorkOrderHelper();
      if (workorder == nullptr) {
        workorder = multiple_numa_nodes_workorders_.getWorkOrder();
      }
    } else {
      workorder = multiple_numa_nodes_workorders_.getWorkOrder();
      if (workorder == nullptr) {
        workorder = getSingleNUMANodeWorkOrderHelper();
      }
    }
  }
  return workorder;
}

WorkOrder* WorkOrdersContainer::OperatorWorkOrdersContainer::
    getSingleNUMANodeWorkOrderHelper() {
  WorkOrder *workorder = nullptr;
  for (PtrVector<InternalQueueContainer>::iterator it =
           single_numa_node_workorders_.begin();
       it != single_numa_node_workorders_.end(); ++it) {
    workorder = it->getWorkOrder();
    if (workorder != nullptr) {
      return workorder;
    }
  }
  return nullptr;
}

}  // namespace quickstep
