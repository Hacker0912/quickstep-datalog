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
#include <cstdint>
#include <memory>
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
  using Pairs = std::vector<std::pair<std::uint32_t, std::uint32_t>>;
  using UniquePairs = std::unique_ptr<const Pairs>;
  /**
   * @brief Feedback message to Foreman when a BitMatrixJoinInitWorkOrder or BitMatrixJoinWorkOrder has
   *        completed one batch.
   */
  enum FeedbackMessageType : WorkOrder::FeedbackMessageType {
      kNextBatchMessage = 0,
  };

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

  void receiveFeedbackMessage(const WorkOrder::FeedbackMessage &msg) override;

 private:
  const QueryContext::array_index_id array_index_;
  const QueryContext::bit_matrix_id bit_matrix_index_;
  bool started_;

  std::vector<const Pairs*> next_batches_;
  std::uint64_t count_ = 0;

  DISALLOW_COPY_AND_ASSIGN(BitMatrixJoinOperator);
};

class BitMatrixJoinInitWorkOrder : public WorkOrder {
 public:
  BitMatrixJoinInitWorkOrder(const std::size_t query_id,
                             const Range &range,
                             const ArrayIndex &array_index,
                             BitMatrix *matrix,
                             const std::size_t operator_index,
                             const tmb::client_id scheduler_client_id,
                             MessageBus *bus)
      : WorkOrder(query_id),
        range_(range),
        array_index_(array_index),
        matrix_(matrix),
        operator_index_(operator_index),
        scheduler_client_id_(scheduler_client_id),
        bus_(DCHECK_NOTNULL(bus)) {}

  ~BitMatrixJoinInitWorkOrder() override {}

  void execute() override;

 private:
  const Range range_;
  const ArrayIndex &array_index_;
  BitMatrix *matrix_;

  const std::size_t operator_index_;
  const tmb::client_id scheduler_client_id_;
  MessageBus *bus_;

  DISALLOW_COPY_AND_ASSIGN(BitMatrixJoinInitWorkOrder);
};

class BitMatrixJoinWorkOrder : public WorkOrder {
 public:
  BitMatrixJoinWorkOrder(const std::size_t query_id,
                         const ArrayIndex &array_index,
                         const BitMatrixJoinOperator::Pairs *pairs,
                         BitMatrix *matrix,
                         const std::size_t operator_index,
                         const tmb::client_id scheduler_client_id,
                         MessageBus *bus)
      : WorkOrder(query_id),
        array_index_(array_index),
        pairs_(pairs),
        matrix_(matrix),
        operator_index_(operator_index),
        scheduler_client_id_(scheduler_client_id),
        bus_(DCHECK_NOTNULL(bus)) {}

  ~BitMatrixJoinWorkOrder() override {}

  void execute() override;

 private:
  const ArrayIndex &array_index_;
  const BitMatrixJoinOperator::UniquePairs pairs_;
  BitMatrix *matrix_;

  const std::size_t operator_index_;
  const tmb::client_id scheduler_client_id_;
  MessageBus *bus_;

  DISALLOW_COPY_AND_ASSIGN(BitMatrixJoinWorkOrder);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_RELATIONAL_OPERATORS_BIT_MATRIX_JOIN_OPERATOR_HPP_
