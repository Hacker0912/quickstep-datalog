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

#include "relational_operators/BuildArrayIndexOperator.hpp"

#include <memory>

#include "catalog/CatalogRelation.hpp"
#include "query_execution/QueryContext.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageManager.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "storage/ValueAccessor.hpp"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace quickstep {

bool BuildArrayIndexOperator::getAllWorkOrders(
    WorkOrdersContainer *container,
    QueryContext *query_context,
    StorageManager *storage_manager,
    const tmb::client_id scheduler_client_id,
    tmb::MessageBus *bus) {
  DCHECK(query_context != nullptr);

  if (started_) {
    return true;
  }

  ArrayIndex *index = query_context->getArrayIndexMutable(array_index_);
  container->addNormalWorkOrder(
      new BuildArrayIndexWorkOrder(query_id_, relation_, relation_.getBlocksSnapshot(),
                                   index, storage_manager),
      op_index_);
  started_ = true;
  return true;
}

void BuildArrayIndexWorkOrder::execute() {
  for (const block_id id : blocks_) {
    BlockReference block(
        storage_manager_->getBlock(id, relation_));

    // Create the ValueAccessor to be initialized later.
    std::unique_ptr<ValueAccessor> accessor
        (block->getTupleStorageSubBlock().createValueAccessor());

    index_->putValueAccessor(0 /* key_attr_id */, 1 /* value_attr_id */, accessor.get());
  }
}

}  // namespace quickstep
