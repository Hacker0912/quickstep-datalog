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

#include "relational_operators/BitMatrixExportOperator.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>

#include "cli/Flags.hpp"
#include "query_execution/QueryContext.hpp"
#include "query_execution/WorkOrderProtosContainer.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "relational_operators/WorkOrder.pb.h"
#include "types/IntType.hpp"
#include "types/containers/ColumnVector.hpp"
#include "types/containers/ColumnVectorsValueAccessor.hpp"
#include "utility/BarrieredReadWriteConcurrentBitVector.hpp"
#include "utility/BitMatrix.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace quickstep {

namespace {

constexpr std::size_t kMinBatchSize = 1024ul * 1024ul * 4ul;

}  // namespace

bool BitMatrixExportOperator::getAllWorkOrders(
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

  const std::size_t num_batches =
      std::max(1ul, std::min(range * range / kMinBatchSize,
                             static_cast<std::size_t>(FLAGS_num_workers)));

  const RangeSplitter splitter =
      RangeSplitter::CreateWithNumPartitions(0, range, num_batches);

  InsertDestination *output_destination =
    query_context->getInsertDestination(output_destination_index_);

  for (std::size_t i = 0; i < splitter.getNumPartitions(); ++i) {
    container->addNormalWorkOrder(
        new BitMatrixExportWorkOrder(query_id_, splitter.getPartition(i), matrix, output_destination),
        op_index_);
  }

  return true;
}

bool BitMatrixExportOperator::getAllWorkOrderProtos(
    WorkOrderProtosContainer *container)  {
  LOG(FATAL) << "Not supported";
}

void BitMatrixExportWorkOrder::execute() {
  const int length = matrix_->size();

  const std::size_t kBulkInsertBatchSize = std::max(0x10000, length * 2);

  auto cv0 =
      std::make_shared<NativeColumnVector>(IntType::InstanceNonNullable(), kBulkInsertBatchSize);
  auto cv1 =
      std::make_shared<NativeColumnVector>(IntType::InstanceNonNullable(), kBulkInsertBatchSize);

  std::size_t total = 0;
  for (std::size_t x = range_.begin(); x < range_.end(); ++x) {
    const BarrieredReadWriteConcurrentBitVector &bit_vector = matrix_->get(x);
    DCHECK_EQ(length, bit_vector.size());

    std::size_t y = bit_vector.firstOne();
    while (y != length) {
      ++total;
      *static_cast<int*>(cv0->getPtrForDirectWrite()) = x;
      *static_cast<int*>(cv1->getPtrForDirectWrite()) = y;

      y = bit_vector.nextOne(y);
    }

    if (total > length) {
      bulkInsert(cv0, cv1);
      cv0->clear();
      cv1->clear();
      total = 0;
    }
  }

  if (total > 0) {
    bulkInsert(cv0, cv1);
  }
}

void BitMatrixExportWorkOrder::bulkInsert(const ColumnVectorPtr &cv0,
                                          const ColumnVectorPtr &cv1) {
  ColumnVectorsValueAccessor columns;
  columns.addColumn(cv0);
  columns.addColumn(cv1);
  output_destination_->bulkInsertTuples(&columns);
}

}  // namespace quickstep
