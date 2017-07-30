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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_RULES_TRANSFORM_MULTI_VALUE_FILTER_JOIN_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_RULES_TRANSFORM_MULTI_VALUE_FILTER_JOIN_HPP_

#include <string>
#include <unordered_map>
#include <vector>

#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ExprId.hpp"
#include "query_optimizer/expressions/Expression.hpp"
#include "query_optimizer/expressions/Predicate.hpp"
#include "query_optimizer/logical/HashJoin.hpp"
#include "query_optimizer/logical/Logical.hpp"
#include "query_optimizer/logical/TableReference.hpp"
#include "query_optimizer/rules/BottomUpRule.hpp"
#include "utility/Macros.hpp"

namespace quickstep {
namespace optimizer {

class OptimizerContext;

/** \addtogroup OptimizerRules
 *  @{
 */

class TransformMultiValueFilterJoin : public BottomUpRule<logical::Logical> {
 public:
  TransformMultiValueFilterJoin(OptimizerContext *optimizer_context)
      : optimizer_context_(optimizer_context) {}

  std::string getName() const override {
    return "TransformMultiValueFilterJoin";
  }

 protected:
  logical::LogicalPtr applyToNode(const logical::LogicalPtr &input) override;

 private:
  logical::LogicalPtr transformInternal(const logical::HashJoinPtr &hash_join);

  struct JoinContext {
    logical::TableReferencePtr table;
    expressions::AttributeReferencePtr build_equi_join_attr;
    expressions::AttributeReferencePtr probe_equi_join_attr;
    expressions::AttributeReferencePtr build_not_equal_attr;
    expressions::AttributeReferencePtr other_not_equal_attr;
    expressions::PredicatePtr predicate;
  };

  bool visitBuildSide(const logical::LogicalPtr &input,
                      const expressions::AttributeReferencePtr &left_ne_attr,
                      const expressions::AttributeReferencePtr &right_ne_attr,
                      JoinContext *context) const;

  bool visitProbeSide(const logical::LogicalPtr &input,
                      const JoinContext &context) const;

  bool isIsomorphic(const expressions::ExpressionPtr &build_expr,
                    const expressions::ExpressionPtr &probe_expr,
                    const std::unordered_map<expressions::ExprId,
                                             expressions::ExprId> &substitution_map) const;

  OptimizerContext *optimizer_context_;

  DISALLOW_COPY_AND_ASSIGN(TransformMultiValueFilterJoin);
};

/** @} */

}  // namespace optimizer
}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_OPTIMIZER_RULES_TRANSFORM_MULTI_VALUE_FILTER_JOIN_HPP_
