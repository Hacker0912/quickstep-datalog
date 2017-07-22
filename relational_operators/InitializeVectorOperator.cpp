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

#include "relational_operators/InitializeVectorOperator.hpp"

#include <cstddef>

#include "query_execution/QueryContext.hpp"
#include "query_execution/WorkOrderProtosContainer.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "relational_operators/WorkOrder.pb.h"
#include "storage/CollisionFreeVector.hpp"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace quickstep {

bool InitializeVectorOperator::getAllWorkOrders(
    WorkOrdersContainer *container,
    QueryContext *query_context,
    StorageManager *storage_manager,
    const tmb::client_id scheduler_client_id,
    tmb::MessageBus *bus) {
  if (!started_) {
    for (partition_id part_id = 0; part_id < num_partitions_; ++part_id) {
      CollisionFreeVector *vector =
          query_context->getCollisionFreeVector(vector_index_, part_id);
      DCHECK(vector != nullptr);

      for (std::size_t vector_part_id = 0;
           vector_part_id < vector_num_init_partitions_;
           ++vector_part_id) {
        container->addNormalWorkOrder(
            new InitializeVectorWorkOrder(query_id_, vector_part_id, vector),
            op_index_);
      }
    }
    started_ = true;
  }
  return true;
}

bool InitializeVectorOperator::getAllWorkOrderProtos(WorkOrderProtosContainer *container) {
  if (started_) {
    return true;
  }

  for (partition_id part_id = 0; part_id < num_partitions_; ++part_id) {
    for (std::size_t vector_part_id = 0;
         vector_part_id < vector_num_init_partitions_;
         ++vector_part_id) {
      serialization::WorkOrder *proto = new serialization::WorkOrder;
      proto->set_work_order_type(serialization::INITIALIZE_VECTOR);
      proto->set_query_id(query_id_);

      proto->SetExtension(serialization::InitializeVectorWorkOrder::vector_index, vector_index_);
      proto->SetExtension(serialization::InitializeVectorWorkOrder::partition_id, part_id);
      proto->SetExtension(serialization::InitializeVectorWorkOrder::vector_partition_id, vector_part_id);

      container->addWorkOrderProto(proto, op_index_);
    }
  }
  started_ = true;
  return true;
}

void InitializeVectorWorkOrder::execute() {
  vector_->initialize(vector_partition_id_);
}

}  // namespace quickstep
