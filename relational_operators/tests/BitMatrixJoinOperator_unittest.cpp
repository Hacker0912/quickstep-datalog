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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogDatabase.hpp"
#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/QueryContext.hpp"
#include "query_execution/QueryContext.pb.h"
#include "query_execution/QueryExecutionTypedefs.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "relational_operators/BitMatrixJoinOperator.hpp"
#include "relational_operators/BuildArrayIndexOperator.hpp"
#include "relational_operators/InitializeBitMatrixOperator.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "relational_operators/WorkOrder.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageManager.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "threading/ThreadIDBasedMap.hpp"
#include "types/IntType.hpp"
#include "types/Type.hpp"
#include "types/TypedValue.hpp"
#include "types/containers/Tuple.hpp"
#include "utility/ArrayIndex.hpp"
#include "utility/ArrayIndex.pb.h"
#include "utility/BitMatrix.hpp"
#include "utility/BitMatrix.pb.h"
#include "utility/Macros.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "tmb/id_typedefs.h"

using std::make_unique;
using std::size_t;
using std::unique_ptr;

namespace quickstep {

namespace {

constexpr int kMinValue = 0;
constexpr std::size_t kRangeSize = 9;

constexpr std::size_t kQueryId = 0;
constexpr int kOpIndex = 0;

const int data[][2] = { { 0, 1 },
                        { 0, 2 },
                        { 1, 3 },
                        { 2, 4 },
                        { 2, 5 },
                        { 4, 6 },
                        { 5, 7 },
                        { 6, 8 }};

}  // namespace

class BitMatrixJoinOperatorTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    thread_id_map_ = ClientIDMap::Instance();

    bus_.Initialize();

    const tmb::client_id worker_thread_client_id = bus_.Connect();
    bus_.RegisterClientAsSender(worker_thread_client_id, kCatalogRelationNewBlockMessage);

    // Usually the worker thread makes the following call. In this test setup,
    // we don't have a worker thread hence we have to explicitly make the call.
    thread_id_map_->addValue(worker_thread_client_id);

    foreman_client_id_ = bus_.Connect();
    bus_.RegisterClientAsSender(foreman_client_id_, kCatalogRelationNewBlockMessage);
    bus_.RegisterClientAsReceiver(foreman_client_id_, kCatalogRelationNewBlockMessage);

    storage_manager_ = make_unique<StorageManager>("./BitMatrixJoin/");

    // Create a database.
    db_ = std::make_unique<CatalogDatabase>(nullptr, "database");

    // Create tables, owned by db_.
    base_table_ = new CatalogRelation(NULL, "parent", 100);
    db_->addRelation(base_table_);

    // Add attributes.
    const Type &int_type = IntType::InstanceNonNullable();
    base_table_->addAttribute(new CatalogAttribute(base_table_, "x", int_type));
    base_table_->addAttribute(new CatalogAttribute(base_table_, "y", int_type));

    // Create block.
    block_ = storage_manager_->createBlock(*base_table_, base_table_->getDefaultStorageBlockLayout());
    MutableBlockReference storage_block = storage_manager_->getBlockMutable(block_, *base_table_);
    base_table_->addBlock(block_);

    // Insert tuples to table.
    for (std::size_t i = 0; i < sizeof(data) / sizeof(data[0]); ++i) {
      std::vector<TypedValue> attr_values;
      attr_values.emplace_back(data[i][0]);
      attr_values.emplace_back(data[i][1]);

      Tuple tuple(std::move(attr_values));
      EXPECT_TRUE(storage_block->insertTupleInBatch(tuple));
    }
    storage_block->rebuild();
  }

  virtual void TearDown() {
    thread_id_map_->removeValue();

    // Drop blocks from relations.
    storage_manager_->deleteBlockOrBlobFile(block_);
  }

  void fetchAndExecuteWorkOrders(RelationalOperator *op) {
    // Note: We treat each operator as an individual query plan DAG. The
    // index for each operator should be set, so that the WorkOrdersContainer
    // class' checks about operator index are successful.
    op->setOperatorIndex(kOpIndex);
    WorkOrdersContainer container(1, 0);
    const std::size_t op_index = 0;
    op->getAllWorkOrders(&container,
                         query_context_.get(),
                         storage_manager_.get(),
                         foreman_client_id_,
                         &bus_);

    while (container.hasNormalWorkOrder(op_index)) {
      WorkOrder *work_order = container.getNormalWorkOrder(op_index);
      work_order->execute();
      delete work_order;
    }
  }

  // This map is needed for InsertDestination and some WorkOrders that send
  // messages to Foreman directly. To know the reason behind the design of this
  // map, see the note in InsertDestination.hpp.
  ClientIDMap *thread_id_map_;

  MessageBusImpl bus_;
  tmb::client_id foreman_client_id_;

  unique_ptr<QueryContext> query_context_;
  unique_ptr<StorageManager> storage_manager_;

  unique_ptr<CatalogDatabase> db_;
  // The following CatalogRelations are owned by db_.
  CatalogRelation *base_table_;
  block_id block_;
};

TEST_F(BitMatrixJoinOperatorTest, BasicTest) {
  // Setup the query context proto.
  serialization::QueryContext query_context_proto;
  query_context_proto.set_query_id(kQueryId);

  const QueryContext::bit_matrix_id bit_matrix_index =
      query_context_proto.bit_matrices_size();

  query_context_proto.add_bit_matrices()->set_size(kRangeSize);

  // Create the builder operator.
  auto init = make_unique<InitializeBitMatrixOperator>(kQueryId, bit_matrix_index);

  // Create the prober operator with two selection attributes.
  const QueryContext::array_index_id array_index = query_context_proto.array_indexes_size();
  serialization::ArrayIndex *array_index_proto = query_context_proto.add_array_indexes();
  array_index_proto->set_base(kMinValue);
  array_index_proto->set_length(kRangeSize);
  array_index_proto->set_reserve_size(2);

  auto build = make_unique<BuildArrayIndexOperator>(kQueryId, *base_table_, array_index);

  auto join = make_unique<BitMatrixJoinOperator>(kQueryId, array_index, bit_matrix_index);

  // Set up the QueryContext.
  query_context_.reset(new QueryContext(query_context_proto,
                                        *db_,
                                        storage_manager_.get(),
                                        foreman_client_id_,
                                        &bus_));

  // Execute the operators.
  fetchAndExecuteWorkOrders(init.get());
  fetchAndExecuteWorkOrders(build.get());
  fetchAndExecuteWorkOrders(join.get());

  // Check result values
  const BitMatrix &bit_matrix = query_context_->getBitMatrix(bit_matrix_index);
  EXPECT_TRUE(bit_matrix.get(1, 2));
  EXPECT_TRUE(bit_matrix.get(2, 1));
  EXPECT_TRUE(bit_matrix.get(3, 4));
  EXPECT_TRUE(bit_matrix.get(3, 5));
  EXPECT_TRUE(bit_matrix.get(4, 3));
  EXPECT_TRUE(bit_matrix.get(4, 5));
  EXPECT_TRUE(bit_matrix.get(5, 3));
  EXPECT_TRUE(bit_matrix.get(5, 4));
  EXPECT_TRUE(bit_matrix.get(6, 7));
  EXPECT_TRUE(bit_matrix.get(7, 6));
}

}  // namespace quickstep

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  // Honor FLAGS_buffer_pool_slots in StorageManager.
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
