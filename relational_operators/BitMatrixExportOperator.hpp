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

#ifndef QUICKSTEP_RELATIONAL_OPERATORS_BIT_MATRIX_EXPORT_OPERATOR_HPP_
#define QUICKSTEP_RELATIONAL_OPERATORS_BIT_MATRIX_EXPORT_OPERATOR_HPP_

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/QueryContext.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "relational_operators/WorkOrder.hpp"
#include "types/containers/ColumnVector.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace tmb { class MessageBus; }

namespace quickstep {

class BitMatrix;
class InsertDestination;
class StorageManager;
class WorkOrderProtosContainer;
class WorkOrdersContainer;

namespace serialization { class WorkOrder; }

/** \addtogroup RelationalOperators
 *  @{
 */

/**
 * @brief An operator which exports BitMatrix.
 **/
class BitMatrixExportOperator : public RelationalOperator {
 public:
  /**
   * @brief Constructor for BitMatrixExportOperator.
   *
   * @param query_id The ID of the query to which this operator belongs.
   * @param bit_matrix_index The BitMatrix to use.
   * @param output_relation The output relation.
   * @param output_destination_index The index of the InsertDestination in the
   *        QueryContext to insert the sampling results.
   **/
  BitMatrixExportOperator(
      const std::size_t query_id,
      const QueryContext::bit_matrix_id bit_matrix_index,
      const relation_id output_relation_id,
      const QueryContext::insert_destination_id output_destination_index)
      : RelationalOperator(query_id),
        bit_matrix_index_(bit_matrix_index),
        output_relation_id_(output_relation_id),
        output_destination_index_(output_destination_index),
        started_(false) {}

  ~BitMatrixExportOperator() override {}

  OperatorType getOperatorType() const override {
    return kBitMatrixExport;
  }

  std::string getName() const override {
    return "BitMatrixExportOperator";
  }

  bool getAllWorkOrders(WorkOrdersContainer *container,
                        QueryContext *query_context,
                        StorageManager *storage_manager,
                        const tmb::client_id scheduler_client_id,
                        tmb::MessageBus *bus) override;

  bool getAllWorkOrderProtos(WorkOrderProtosContainer *container) override;

  QueryContext::insert_destination_id getInsertDestinationID() const override {
    return output_destination_index_;
  }

  const relation_id getOutputRelationID() const override {
    return output_relation_id_;
  }

 private:
  const QueryContext::bit_matrix_id bit_matrix_index_;
  const relation_id output_relation_id_;
  const QueryContext::insert_destination_id output_destination_index_;

  bool started_;

  DISALLOW_COPY_AND_ASSIGN(BitMatrixExportOperator);
};

/**
 * @brief A WorkOrder produced by BitMatrixExportOperator.
 **/
class BitMatrixExportWorkOrder : public WorkOrder {
 public:
  /**
   * @brief Constructor.
   *
   * @param query_id The ID of the query to which this WorkOrder belongs.
   * @param range The range of BitMatrix to export.
   * @param output_destination The InsertDestination to insert the sample results.
   **/
  BitMatrixExportWorkOrder(const std::size_t query_id,
                           const Range &range,
                           BitMatrix *matrix,
                           InsertDestination *output_destination)
      : WorkOrder(query_id),
        range_(range),
        matrix_(DCHECK_NOTNULL(matrix)),
        output_destination_(DCHECK_NOTNULL(output_destination)) {}

  ~BitMatrixExportWorkOrder() override {}

  /**
   * @exception TupleTooLargeForBlock A tuple produced by this selection was
   *            too large to insert into an empty block provided by
   *            _op->_outputDestination. Select may be partially complete
   *            (with some tuples inserted into the destination) when this
   *            exception is thrown, causing potential inconsistency.
   **/
  void execute() override;

 private:
  void bulkInsert(const ColumnVectorPtr &cv0, const ColumnVectorPtr &cv1);

  const Range range_;

  BitMatrix *matrix_;
  InsertDestination *output_destination_;

  DISALLOW_COPY_AND_ASSIGN(BitMatrixExportWorkOrder);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_RELATIONAL_OPERATORS_BIT_MATRIX_EXPORT_OPERATOR_HPP_
