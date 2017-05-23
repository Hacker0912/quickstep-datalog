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

#include "query_optimizer/rules/Partition.hpp"

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "query_optimizer/expressions/NamedExpression.hpp"
#include "query_optimizer/physical/HashJoin.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/physical/PhysicalType.hpp"
#include "query_optimizer/physical/Selection.hpp"
#include "query_optimizer/physical/TableReference.hpp"
#include "utility/Cast.hpp"

#include "gflags/gflags.h"

using std::make_unique;
using std::move;
using std::static_pointer_cast;
using std::unordered_set;

namespace quickstep {
namespace optimizer {

namespace E = expressions;
namespace P = physical;

DEFINE_uint64(num_repartitions, 4, "Number of repartitions for a hash join.");

P::PhysicalPtr Partition::applyToNode(const P::PhysicalPtr &input) {
  switch (input->getPhysicalType()) {
    case P::PhysicalType::kHashJoin: {
      const P::HashJoinPtr hash_join = static_pointer_cast<const P::HashJoin>(input);

      P::PhysicalPtr right = hash_join->right();
      const P::PartitionSchemeHeader *right_partition_scheme_header =
          right->getOutputPartitionSchemeHeader();
      const auto &right_join_attributes = hash_join->right_join_attributes();

      P::PhysicalPtr left = hash_join->left();
      const P::PartitionSchemeHeader *left_partition_scheme_header =
          left->getOutputPartitionSchemeHeader();
      const auto &left_join_attributes = hash_join->left_join_attributes();

      /*
       * Whether left or right side needs to repartition.
       *
       * --------------------------------------------------------------------------
       * | Right \ Left     | No Partition  | Hash Partition h' | Other Partition |
       * --------------------------------------------------------------------------
       * | No Partition     | false \ false |  false \ false    |  true \ true    |
       * --------------------------------------------------------------------------
       * | Hash Partition h | false \ true  | false* \ false    | false \ true    |
       * --------------------------------------------------------------------------
       * | Other Partition  |  true \ true  |   true \ false    |  true \ true    |
       * --------------------------------------------------------------------------
       *
       * Hash Partition h / h': the paritition attributes are as the same as the join attributes.
       * *: If h and h' has different number of partitions, the left side needs to repartition.
       */
      bool right_needs_repartition = false;
      std::size_t num_partitions = 1u;
      if (right_partition_scheme_header) {
        // Need to repartition unless the partition attributes are as the same as
        // the join attributes.
        right_needs_repartition = true;
        if (right_partition_scheme_header->isHashPartition()) {
          unordered_set<E::ExprId> right_join_expr_ids;
          for (const E::AttributeReferencePtr &attr : right_join_attributes) {
            right_join_expr_ids.insert(attr->id());
          }

          if (right_partition_scheme_header->reusablePartitionScheme(right_join_expr_ids)) {
            right_needs_repartition = false;
            num_partitions = right_partition_scheme_header->num_partitions;
          }
        }
      }

      bool left_needs_repartition = false;
      if (left_partition_scheme_header) {
        if (!right_partition_scheme_header) {
          // Broadcast hash join.
          num_partitions = left_partition_scheme_header->num_partitions;
        }

        // Need to repartition unless the partition attributes are as the same as
        // the join attributes.
        left_needs_repartition = true;
        if (left_partition_scheme_header->isHashPartition()) {
          unordered_set<E::ExprId> left_join_expr_ids;
          for (const E::AttributeReferencePtr &attr : left_join_attributes) {
            left_join_expr_ids.insert(attr->id());
          }

          if (left_partition_scheme_header->reusablePartitionScheme(left_join_expr_ids) &&
              (right_needs_repartition || num_partitions == left_partition_scheme_header->num_partitions)) {
            left_needs_repartition = false;
            num_partitions = left_partition_scheme_header->num_partitions;
          }
        }
      } else if (right_partition_scheme_header) {
        left_needs_repartition = true;
      }

      // Repartition the right relation.
      if (right_needs_repartition) {
        if (left_needs_repartition) {
          num_partitions = FLAGS_num_repartitions;
        }

        P::PartitionSchemeHeader::PartitionExprIds right_repartition_expr_ids;
        for (const E::AttributeReferencePtr &attr : right_join_attributes) {
          right_repartition_expr_ids.push_back({ attr->id() });
        }
        auto right_repartition_scheme_header = make_unique<P::PartitionSchemeHeader>(
            P::PartitionSchemeHeader::PartitionType::kHash, num_partitions, move(right_repartition_expr_ids));

        switch (right->getPhysicalType()) {
          case P::PhysicalType::kSharedSubplanReference:
          case P::PhysicalType::kSort:
          case P::PhysicalType::kTableReference:
          case P::PhysicalType::kUnionAll:
            // Add a Selection node.
            right = P::Selection::Create(right,
                                         CastSharedPtrVector<E::NamedExpression>(right->getOutputAttributes()),
                                         nullptr /* filter_predicate */, right_repartition_scheme_header.release());
            break;
          default:
            // Overwrite the output partition scheme header of the node.
            right = right->copyWithNewOutputPartitionSchemeHeader(right_repartition_scheme_header.release());
        }
      }

      if (left_needs_repartition) {
        P::PartitionSchemeHeader::PartitionExprIds left_repartition_expr_ids;
        for (const E::AttributeReferencePtr &attr : left_join_attributes) {
          left_repartition_expr_ids.push_back({ attr->id() });
        }
        auto left_repartition_scheme_header = make_unique<P::PartitionSchemeHeader>(
            P::PartitionSchemeHeader::PartitionType::kHash, num_partitions, move(left_repartition_expr_ids));

        switch (left->getPhysicalType()) {
          case P::PhysicalType::kSharedSubplanReference:
          case P::PhysicalType::kSort:
          case P::PhysicalType::kTableReference:
          case P::PhysicalType::kUnionAll:
            // Add a Selection node.
            left = P::Selection::Create(left,
                                        CastSharedPtrVector<E::NamedExpression>(left->getOutputAttributes()),
                                        nullptr /* filter_predicate */, left_repartition_scheme_header.release());
            break;
          default:
            // Overwrite the output partition scheme header of the node.
            left = left->copyWithNewOutputPartitionSchemeHeader(left_repartition_scheme_header.release());
        }
      }

      unordered_set<E::ExprId> project_expr_ids;
      for (const E::AttributeReferencePtr &project_expression : hash_join->getOutputAttributes()) {
        project_expr_ids.insert(project_expression->id());
      }

      P::PartitionSchemeHeader::PartitionExprIds output_repartition_expr_ids;
      for (int i = 0; i < left_join_attributes.size(); ++i) {
        const E::ExprId left_join_id = left_join_attributes[i]->id();
        const E::ExprId right_join_id = right_join_attributes[i]->id();

        output_repartition_expr_ids.emplace_back();

        if (project_expr_ids.count(left_join_id)) {
          output_repartition_expr_ids.back().insert(left_join_id);
        }

        if (project_expr_ids.count(right_join_id)) {
          output_repartition_expr_ids.back().insert(right_join_id);
        }

        if (output_repartition_expr_ids.back().empty()) {
          // Some partition attribute will be projected out, so we use
          // the input partition id as the output partition id.
          output_repartition_expr_ids.clear();
          break;
        }
      }
      auto output_partition_scheme_header = make_unique<P::PartitionSchemeHeader>(
          P::PartitionSchemeHeader::PartitionType::kHash, num_partitions, move(output_repartition_expr_ids));
      if (right_needs_repartition || left_needs_repartition) {
        return P::HashJoin::Create(left, right, left_join_attributes, right_join_attributes,
                                   hash_join->residual_predicate(),
                                   hash_join->project_expressions(),
                                   hash_join->join_type(),
                                   output_partition_scheme_header.release());
      } else if (left_partition_scheme_header) {
        return hash_join->copyWithNewOutputPartitionSchemeHeader(output_partition_scheme_header.release());
      }
      break;
    }
    default:
      break;
  }
  return input;
}

}  // namespace optimizer
}  // namespace quickstep
