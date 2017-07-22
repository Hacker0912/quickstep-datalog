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

#ifndef QUICKSTEP_RELATIONAL_OPERATORS_INITIALIZE_VECTOR_OPERATOR_HPP_
#define QUICKSTEP_RELATIONAL_OPERATORS_INITIALIZE_VECTOR_OPERATOR_HPP_

#include <string>

#include "query_execution/QueryContext.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "relational_operators/WorkOrder.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace tmb { class MessageBus; }

namespace quickstep {

class CollisionFreeVector;
class StorageManager;
class WorkOrderProtosContainer;
class WorkOrdersContainer;

namespace serialization { class WorkOrder; }

/** \addtogroup RelationalOperators
 *  @{
 */

/**
 * @brief An operator which initializes a CollisionFreeVector.
 **/
class InitializeVectorOperator : public RelationalOperator {
 public:
  /**
   * @brief Constructor.
   *
   * @param query_id The ID of this query.
   * @param vector_index The index of the CollisionFreeVector in QueryContext.
   * @param num_partitions The number of partitions in 'input_relation'. If no
   *        partitions, it is one.
   * @param vector_num_init_partitions The number of partitions to be used
   *        for initialize the collision free vector table.
   **/
  InitializeVectorOperator(const std::size_t query_id,
                           const QueryContext::join_hash_table_id vector_index,
                           const std::size_t num_partitions,
                           const std::size_t vector_num_init_partitions)
      : RelationalOperator(query_id),
        vector_index_(vector_index),
        num_partitions_(num_partitions),
        vector_num_init_partitions_(vector_num_init_partitions),
        started_(false) {}

  ~InitializeVectorOperator() override {}

  OperatorType getOperatorType() const override {
    return kInitializeVector;
  }

  std::string getName() const override {
    return "InitializeVectorOperator";
  }

  bool getAllWorkOrders(WorkOrdersContainer *container,
                        QueryContext *query_context,
                        StorageManager *storage_manager,
                        const tmb::client_id scheduler_client_id,
                        tmb::MessageBus *bus) override;

  bool getAllWorkOrderProtos(WorkOrderProtosContainer *container) override;

 private:
  const QueryContext::join_hash_table_id vector_index_;
  const std::size_t num_partitions_, vector_num_init_partitions_;
  bool started_;

  DISALLOW_COPY_AND_ASSIGN(InitializeVectorOperator);
};

/**
 * @brief A WorkOrder produced by InitializeVectorOperator.
 **/
class InitializeVectorWorkOrder : public WorkOrder {
 public:
  /**
   * @brief Constructor.
   *
   * @param query_id The ID of the query to which this operator belongs.
   * @param vector_partition_id The partition ID for which the work order is issued.
   * @param vector The CollisionFreeVector to be initialized.
   */
  InitializeVectorWorkOrder(const std::size_t query_id,
                            const std::size_t vector_partition_id,
                            CollisionFreeVector *vector)
      : WorkOrder(query_id),
        vector_partition_id_(vector_partition_id),
        vector_(DCHECK_NOTNULL(vector)) {}

  ~InitializeVectorWorkOrder() override {}

  void execute() override;

 private:
  const std::size_t vector_partition_id_;

  CollisionFreeVector *vector_;

  DISALLOW_COPY_AND_ASSIGN(InitializeVectorWorkOrder);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_RELATIONAL_OPERATORS_INITIALIZE_VECTOR_OPERATOR_HPP_
