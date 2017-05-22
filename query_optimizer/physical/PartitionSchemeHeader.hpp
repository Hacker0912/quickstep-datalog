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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_PARTITION_SCHEME_HEADER_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_PARTITION_SCHEME_HEADER_HPP_

#include <cstddef>
#include <utility>
#include <vector>

#include "query_optimizer/expressions/ExprId.hpp"
#include "utility/Macros.hpp"

namespace quickstep {
namespace optimizer {
namespace physical {

/** \addtogroup OptimizerPhysical
 *  @{
 */

/**
 * @brief Store the partitioning info for a physical plan node.
 */
struct PartitionSchemeHeader {
  typedef std::vector<expressions::ExprId> PartitionExprIds;

  enum PartitionType {
    kHash = 0,
    kRange
  };

  /**
   * @brief Constructor.
   *
   * @param type The type of partitioning.
   * @param num_partitions The number of partitions to be created.
   * @param expr_ids The attributes on which the partitioning happens.
   **/
  PartitionSchemeHeader(const PartitionType type,
                        const std::size_t num_partitions,
                        PartitionExprIds &&expr_ids)  // NOLINT(whitespace/operators)
      : partition_type_(type),
        num_partitions_(num_partitions),
        partition_expr_ids_(std::move(expr_ids)) {
  }

  /**
   * @brief Copy constructor.
   *
   * @param other The copy-from instance.
   **/
  PartitionSchemeHeader(const PartitionSchemeHeader &other)
      : partition_type_(other.partition_type_),
        num_partitions_(other.num_partitions_),
        partition_expr_ids_(other.partition_expr_ids_) {
  }

  const PartitionType partition_type_;
  const std::size_t num_partitions_;
  const PartitionExprIds partition_expr_ids_;

  // Copyable.
};

/** @} */

}  // namespace physical
}  // namespace optimizer
}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_PARTITION_SCHEME_HEADER_HPP_
