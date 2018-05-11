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

#include "relational_operators/BitMatrixJoinOperator.hpp"

#include <algorithm>
#include <cstddef>
#include <stack>
#include <utility>

#include "cli/Flags.hpp"
#include "query_execution/QueryContext.hpp"
#include "query_execution/WorkOrderProtosContainer.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "relational_operators/WorkOrder.pb.h"
#include "utility/ArrayIndex.hpp"
#include "utility/BitMatrix.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace quickstep {

bool BitMatrixJoinOperator::getAllWorkOrders(
    WorkOrdersContainer *container,
    QueryContext *query_context,
    StorageManager *storage_manager,
    const tmb::client_id scheduler_client_id,
    tmb::MessageBus *bus) {
  if (started_) {
    return true;
  }
  started_ = true;

  BitMatrix *matrix = query_context->getBitMatrixMutable(bit_matrix_index_);
  const std::size_t range = matrix->size();
  if (range == 0) {
    return true;
  }

  const ArrayIndex &array_index = query_context->getArrayIndex(array_index_);

  const RangeSplitter splitter =
      RangeSplitter::CreateWithNumPartitions(array_index.base(), range, FLAGS_num_workers * 2);

  for (std::size_t i = 0; i < splitter.getNumPartitions(); ++i) {
    container->addNormalWorkOrder(
        new BitMatrixJoinWorkOrder(query_id_, splitter.getPartition(i), array_index, matrix),
        op_index_);
  }

  return true;
}

bool BitMatrixJoinOperator::getAllWorkOrderProtos(
    WorkOrderProtosContainer *container) {
  LOG(FATAL) << "Not supported";
}

void BitMatrixJoinWorkOrder::execute() {
  for (std::size_t i = range_.begin(); i < range_.end(); ++i) {
    const auto &entries = array_index_.get(i);
    for (const auto x : entries) {
      for (const auto y : entries) {
        if (x != y) {
          evaluate(x, y);
        }
      }
    }
  }
}

void BitMatrixJoinWorkOrder::evaluate(std::uint32_t x, std::uint32_t y) {
  if (matrix_->get(x, y)) {
    return;
  }

  matrix_->set(x, y);

  std::stack<std::pair<std::uint32_t, std::uint32_t>> st;
  st.emplace(x, y);

  while (!st.empty()) {
    const auto &top = st.top();
    x = top.first;
    y = top.second;
    st.pop();

    const auto &x_children = array_index_.get(x);
    const auto &y_children = array_index_.get(y);

    if (x_children.empty() || y_children.empty()) {
      continue;
    }

    for (const auto new_x : x_children) {
      for (const auto new_y : y_children) {
        if (!matrix_->get(new_x, new_y)) {
          matrix_->set(new_x, new_y);
          st.emplace(new_x, new_y);
        }
      }
    }
  }
}

}  // namespace quickstep
