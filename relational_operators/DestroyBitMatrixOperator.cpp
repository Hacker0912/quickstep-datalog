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

#include "relational_operators/DestroyBitMatrixOperator.hpp"

#include "query_execution/QueryContext.hpp"
#include "query_execution/WorkOrderProtosContainer.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "relational_operators/WorkOrder.pb.h"

#include "tmb/id_typedefs.h"

namespace quickstep {

bool DestroyBitMatrixOperator::getAllWorkOrders(
    WorkOrdersContainer *container,
    QueryContext *query_context,
    StorageManager *storage_manager,
    const tmb::client_id scheduler_client_id,
    tmb::MessageBus *bus) {
  if (work_generated_) {
    return true;
  }

  container->addNormalWorkOrder(
      new DestroyBitMatrixWorkOrder(query_id_, bit_matrix_index_, query_context),
      op_index_);
  work_generated_ = true;
  return true;
}

bool DestroyBitMatrixOperator::getAllWorkOrderProtos(WorkOrderProtosContainer *container) {
  LOG(FATAL) << "TODO";
}


void DestroyBitMatrixWorkOrder::execute() {
  query_context_->destroyBitMatrix(bit_matrix_index_);
}

}  // namespace quickstep
