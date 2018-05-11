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

#ifndef QUICKSTEP_RELATIONAL_OPERATORS_BIT_MATRIX_JOIN_OPERATOR_HPP_
#define QUICKSTEP_RELATIONAL_OPERATORS_BIT_MATRIX_JOIN_OPERATOR_HPP_

#include <cstddef>
#include <string>

#include "query_execution/QueryContext.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "relational_operators/WorkOrder.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace tmb { class MessageBus; }

namespace quickstep {

class ArrayIndex;
class BitMatrix;
class StorageManager;
class WorkOrderProtosContainer;
class WorkOrdersContainer;

/** \addtogroup RelationalOperators
 *  @{
 */

class BitMatrixJoinOperator : public RelationalOperator {
 public:
  BitMatrixJoinOperator(const std::size_t query_id,
                        const QueryContext::array_index_id array_index,
                        const QueryContext::bit_matrix_id bit_matrix_index)
      : RelationalOperator(query_id, 1u),
        array_index_(array_index),
        bit_matrix_index_(bit_matrix_index),
        started_(false) {
  }

  ~BitMatrixJoinOperator() override {}

  OperatorType getOperatorType() const override {
    return kBitMatrixJoin;
  }

  std::string getName() const override {
    return "BitMatrixJoinOperator";
  }

  bool getAllWorkOrders(WorkOrdersContainer *container,
                        QueryContext *query_context,
                        StorageManager *storage_manager,
                        const tmb::client_id scheduler_client_id,
                        tmb::MessageBus *bus) override;

  bool getAllWorkOrderProtos(WorkOrderProtosContainer *container) override;

 private:
  const QueryContext::array_index_id array_index_;
  const QueryContext::bit_matrix_id bit_matrix_index_;
  bool started_;

  DISALLOW_COPY_AND_ASSIGN(BitMatrixJoinOperator);
};

class BitMatrixJoinWorkOrder : public WorkOrder {
 public:
  BitMatrixJoinWorkOrder(const std::size_t query_id,
                         const Range &range,
                         const ArrayIndex &array_index,
                         BitMatrix *matrix)
      : WorkOrder(query_id),
        range_(range),
        array_index_(array_index),
        matrix_(matrix) {}

  ~BitMatrixJoinWorkOrder() override {}

  void execute() override;

 private:
  void evaluate(std::uint32_t x, std::uint32_t y);

  const Range range_;
  const ArrayIndex &array_index_;
  BitMatrix *matrix_;

  DISALLOW_COPY_AND_ASSIGN(BitMatrixJoinWorkOrder);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_RELATIONAL_OPERATORS_BIT_MATRIX_JOIN_OPERATOR_HPP_
