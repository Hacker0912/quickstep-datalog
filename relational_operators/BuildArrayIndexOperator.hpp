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

#ifndef QUICKSTEP_RELATIONAL_OPERATORS_BUILD_ARRAY_INDEX_OPERATOR_HPP_
#define QUICKSTEP_RELATIONAL_OPERATORS_BUILD_ARRAY_INDEX_OPERATOR_HPP_

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "catalog/CatalogRelation.hpp"
#include "query_execution/QueryContext.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace tmb { class MessageBus; }

namespace quickstep {

class ArrayIndex;
class CatalogRelationSchema;
class StorageManager;
class WorkOrderProtosContainer;
class WorkOrdersContainer;

/** \addtogroup RelationalOperators
 *  @{
 */

/**
 * @brief An operator which creates an index.
 **/
class BuildArrayIndexOperator : public RelationalOperator {
 public:
  /**
   * @brief Constructor.
   *
   * @param query_id The ID of the query to which this operator belongs.
   * @param relation The relation to create index upon.
   * @param index_name The index to create.
   * @param index_description The index_description associated with this index.
   **/
  BuildArrayIndexOperator(const std::size_t query_id,
                          const CatalogRelation &relation,
                          const QueryContext::array_index_id array_index)
      : RelationalOperator(query_id, 0u),
        relation_(relation),
        array_index_(array_index),
        started_(false) {
    DCHECK_EQ(2u, relation.size());
  }

  ~BuildArrayIndexOperator() override {}

  OperatorType getOperatorType() const override {
    return kBuildArrayIndex;
  }

  std::string getName() const override {
    return "BuildArrayIndexOperator";
  }

  bool getAllWorkOrders(WorkOrdersContainer *container,
                        QueryContext *query_context,
                        StorageManager *storage_manager,
                        const tmb::client_id scheduler_client_id,
                        tmb::MessageBus *bus) override;

  bool getAllWorkOrderProtos(WorkOrderProtosContainer *container) override {
    LOG(FATAL) << "TODO";
  }

 private:
  const CatalogRelation &relation_;
  const QueryContext::array_index_id array_index_;

  bool started_;

  DISALLOW_COPY_AND_ASSIGN(BuildArrayIndexOperator);
};

/**
 * @brief A WorkOrder produced by BuildArrayIndexOperator
 **/
class BuildArrayIndexWorkOrder : public WorkOrder {
 public:
  BuildArrayIndexWorkOrder(const std::size_t query_id,
                           const CatalogRelationSchema &relation,
                           std::vector<block_id> &&blocks,
                           ArrayIndex *index,
                           StorageManager *storage_manager)
      : WorkOrder(query_id),
        relation_(relation),
        blocks_(std::move(blocks)),
        index_(index),
        storage_manager_(storage_manager) {
  }

  void execute() override;

 private:
  const CatalogRelationSchema &relation_;
  const std::vector<block_id> blocks_;

  ArrayIndex *index_;
  StorageManager *storage_manager_;

  DISALLOW_COPY_AND_ASSIGN(BuildArrayIndexWorkOrder);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_RELATIONAL_OPERATORS_BUILD_ARRAY_INDEX_OPERATOR_HPP_
