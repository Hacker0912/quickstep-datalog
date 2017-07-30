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

#include "query_optimizer/rules/TransformMultiValueFilterJoin.hpp"

#include <cstddef>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "expressions/aggregation/AggregateFunctionHasMultipleValues.hpp"
#include "query_optimizer/OptimizerContext.hpp"
#include "query_optimizer/expressions/AggregateFunction.hpp"
#include "query_optimizer/expressions/Alias.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/BinaryExpression.hpp"
#include "query_optimizer/expressions/Cast.hpp"
#include "query_optimizer/expressions/ComparisonExpression.hpp"
#include "query_optimizer/expressions/ExprId.hpp"
#include "query_optimizer/expressions/ExpressionUtil.hpp"
#include "query_optimizer/expressions/LogicalAnd.hpp"
#include "query_optimizer/expressions/LogicalOr.hpp"
#include "query_optimizer/expressions/LogicalNot.hpp"
#include "query_optimizer/expressions/PatternMatcher.hpp"
#include "query_optimizer/expressions/Predicate.hpp"
#include "query_optimizer/expressions/PredicateLiteral.hpp"
#include "query_optimizer/expressions/ScalarLiteral.hpp"
#include "query_optimizer/expressions/UnaryExpression.hpp"
#include "query_optimizer/logical/Aggregate.hpp"
#include "query_optimizer/logical/Filter.hpp"
#include "query_optimizer/logical/HashJoin.hpp"
#include "query_optimizer/logical/Logical.hpp"
#include "query_optimizer/logical/PatternMatcher.hpp"
#include "query_optimizer/logical/Project.hpp"
#include "query_optimizer/logical/SetOperation.hpp"
#include "query_optimizer/logical/TableReference.hpp"
#include "query_optimizer/rules/RuleHelper.hpp"
#include "types/Type.hpp"
#include "types/IntType.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"
#include "types/operations/comparisons/Comparison.hpp"
#include "types/operations/comparisons/EqualComparison.hpp"
#include "types/operations/unary_operations/UnaryOperation.hpp"
#include "types/operations/binary_operations/BinaryOperation.hpp"

namespace quickstep {
namespace optimizer {

namespace E = ::quickstep::optimizer::expressions;
namespace L = ::quickstep::optimizer::logical;

namespace {

template <typename TargetType, typename SourceType>
inline static std::shared_ptr<const TargetType> Cast(
    const std::shared_ptr<const SourceType> &expr) {
  return std::static_pointer_cast<const TargetType>(expr);
}

}  // namespace

L::LogicalPtr TransformMultiValueFilterJoin::applyToNode(
    const L::LogicalPtr &input) {
  std::vector<L::LogicalPtr> new_children;
  new_children.reserve(input->children().size());

  for (const L::LogicalPtr &child : input->children()) {
    L::HashJoinPtr hash_join;
    if (L::SomeHashJoin::MatchesWithConditionalCast(child, &hash_join)) {
      new_children.emplace_back(transformInternal(hash_join));
    } else {
      new_children.emplace_back(child);
    }
  }

  if (new_children != input->children()) {
    return input->copyWithNewChildren(new_children);
  } else {
    return input;
  }
}

L::LogicalPtr TransformMultiValueFilterJoin::transformInternal(
    const L::HashJoinPtr &hash_join) {
  // Semi/Anti join.
  const auto join_type = hash_join->join_type();
  if (join_type != L::HashJoin::JoinType::kLeftSemiJoin &&
      join_type != L::HashJoin::JoinType::kLeftAntiJoin) {
    return hash_join;
  }

  // Single join attribute.
  if (hash_join->left_join_attributes().size() != 1) {
    return hash_join;
  }

  // NotEqual predicate.
  const E::PredicatePtr predicate = hash_join->residual_predicate();
  if (predicate == nullptr ||
      predicate->getExpressionType() != E::ExpressionType::kComparisonExpression) {
    return hash_join;
  }

  const E::ComparisonExpressionPtr comp_expr =
      std::static_pointer_cast<const E::ComparisonExpression>(predicate);
  if (comp_expr->comparison().getComparisonID() != ComparisonID::kNotEqual) {
    return hash_join;
  }

  E::AttributeReferencePtr left_ne_attr, right_ne_attr;
  if (!E::SomeAttributeReference::MatchesWithConditionalCast(comp_expr->left(),
                                                             &left_ne_attr) ||
      !E::SomeAttributeReference::MatchesWithConditionalCast(comp_expr->right(),
                                                             &right_ne_attr)) {
    return hash_join;
  }

  JoinContext context;
  context.probe_equi_join_attr = hash_join->left_join_attributes().front();
  context.build_equi_join_attr = hash_join->right_join_attributes().front();

  if (!visitBuildSide(hash_join->right(), left_ne_attr, right_ne_attr, &context) ||
      !visitProbeSide(hash_join->left(), context)) {
    return hash_join;
  }

  // Do the transformation.
  const E::AggregateFunctionPtr multival_func =
      E::AggregateFunction::Create(
          AggregateFunctionHasMultipleValues::Instance(),
          {context.build_not_equal_attr},
          true /* is_vector_aggregate */,
          false /* is_distinct */);

  const E::ExprId multival_attr_id = optimizer_context_->nextExprId();
  const Type &multival_type = multival_func->getValueType();

  const L::AggregatePtr multival_aggr =
      L::Aggregate::Create(
          hash_join->right(),
          {context.build_equi_join_attr},
          {E::Alias::Create(multival_attr_id, multival_func, "multival", "")});

  const TypedValue literal_one =
      multival_type.coerceValue(TypedValue(static_cast<int>(1)),
                                IntType::InstanceNonNullable());

  const E::PredicatePtr multival_predicate =
      E::ComparisonExpression::Create(
          EqualComparison::Instance(),
          E::AttributeReference::Create(multival_attr_id,
                                        "multival",
                                        "",
                                        "",
                                        multival_type,
                                        E::AttributeReferenceScope::kLocal),
          E::ScalarLiteral::Create(literal_one, multival_type));

  const L::FilterPtr multi_value_filter =
      L::Filter::Create(multival_aggr, multival_predicate);

  return L::HashJoin::Create(
      hash_join->left(),
      L::Project::Create(multi_value_filter, {context.build_equi_join_attr}),
      {context.probe_equi_join_attr},
      {context.build_equi_join_attr},
      nullptr /* residual_predicate */,
      hash_join->join_type());
}

bool TransformMultiValueFilterJoin::visitBuildSide(
    const L::LogicalPtr &input,
    const E::AttributeReferencePtr &left_ne_attr,
    const E::AttributeReferencePtr &right_ne_attr,
    JoinContext *context) const {
  L::LogicalPtr node = input;

  L::ProjectPtr project;
  if (L::SomeProject::MatchesWithConditionalCast(node, &project)) {
    const auto &proj_exprs = project->project_expressions();
    if (!E::ContainsExpression(proj_exprs, left_ne_attr) &&
        !E::ContainsExpression(proj_exprs, right_ne_attr)) {
      return false;
    }
    node = project->input();
  }

  L::FilterPtr filter;
  if (L::SomeFilter::MatchesWithConditionalCast(node, &filter)) {
    context->predicate = filter->filter_predicate();
    node = filter->input();
  }

  L::TableReferencePtr table;
  if (L::SomeTableReference::MatchesWithConditionalCast(node, &table)) {
    const auto &attr_list = table->attribute_list();
    if (E::ContainsExpression(attr_list, left_ne_attr)) {
      context->build_not_equal_attr = left_ne_attr;
      context->other_not_equal_attr = right_ne_attr;
    } else if (E::ContainsExpression(attr_list, right_ne_attr)) {
      context->build_not_equal_attr = right_ne_attr;
      context->other_not_equal_attr = left_ne_attr;
    } else {
      return false;
    }
    context->table = table;
    return true;
  }

  return false;
}

bool TransformMultiValueFilterJoin::visitProbeSide(
    const L::LogicalPtr &input,
    const JoinContext &context) const {
  L::LogicalPtr node;
  L::FilterPtr filter;
  E::PredicatePtr predicate;
  if (L::SomeFilter::MatchesWithConditionalCast(input, &filter)) {
    predicate = filter->filter_predicate();
    node = filter->input();
  } else {
    node = input;
  }

  L::TableReferencePtr table;
  if (L::SomeTableReference::MatchesWithConditionalCast(node, &table)) {
    if (table->catalog_relation() != context.table->catalog_relation()) {
      return false;
    }

    std::unordered_map<E::ExprId, E::ExprId> substitution_map;
    const auto &build_table_attr_list = context.table->attribute_list();
    const auto &probe_table_attr_list = table->attribute_list();
    for (std::size_t i = 0; i < table->attribute_list().size(); ++i) {
      substitution_map.emplace(build_table_attr_list[i]->id(),
                               probe_table_attr_list[i]->id());
    }

    if (!isIsomorphic(context.build_equi_join_attr,
                      context.probe_equi_join_attr,
                      substitution_map) ||
        !isIsomorphic(context.build_not_equal_attr,
                      context.other_not_equal_attr,
                      substitution_map)) {
      return false;
    }

    if (context.predicate != nullptr) {
      if (predicate == nullptr) {
        return false;
      }

      const std::vector<E::PredicatePtr> probe_side_predicates =
          GetConjunctivePredicates(predicate);
      const std::vector<E::PredicatePtr> build_side_predicates =
          GetConjunctivePredicates(context.predicate);

      for (const E::PredicatePtr &build_predicate : build_side_predicates) {
        bool contains = false;
        for (const E::PredicatePtr &probe_predicate : probe_side_predicates) {
          if (isIsomorphic(build_predicate, probe_predicate, substitution_map)) {
            contains = true;
            break;
          }
        }
        if (!contains) {
          return false;
        }
      }
    }
    return true;
  }

  // Whitelist.
  switch (input->getLogicalType()) {
    case L::LogicalType::kAggregate:  // Fall through
    case L::LogicalType::kFilter:
    case L::LogicalType::kHashJoin:
    case L::LogicalType::kNestedLoopsJoin:
    case L::LogicalType::kProject:
    case L::LogicalType::kSort:
    case L::LogicalType::kWindowAggregate:
      break;
    case L::LogicalType::kSetOperation: {
      const L::SetOperationPtr set_operation =
          std::static_pointer_cast<const L::SetOperation>(input);
      if (set_operation->getSetOperationType() == L::SetOperation::kIntersect) {
        break;
      }
      return false;
    }
    default:
      return false;
  }

  for (const auto &child : input->children()) {
    const auto attrs = child->getOutputAttributes();
    if (E::ContainsExpression(attrs, context.probe_equi_join_attr) &&
        E::ContainsExpression(attrs, context.other_not_equal_attr)) {
      return visitProbeSide(child, context);
    }
  }
  return false;
}

bool TransformMultiValueFilterJoin::isIsomorphic(
    const E::ExpressionPtr &build_expr,
    const E::ExpressionPtr &probe_expr,
    const std::unordered_map<E::ExprId, E::ExprId> &substitution_map) const {
  if (build_expr->getExpressionType() != probe_expr->getExpressionType()) {
    return false;
  }

  switch (build_expr->getExpressionType()) {
    case E::ExpressionType::kAttributeReference: {
      const E::ExprId lhs = Cast<E::AttributeReference>(build_expr)->id();
      const E::ExprId rhs = Cast<E::AttributeReference>(probe_expr)->id();
      if (lhs == rhs) {
        return true;
      }
      auto it = substitution_map.find(lhs);
      return (it != substitution_map.end() && it->second == rhs);
    }
    case E::ExpressionType::kBinaryExpression: {
      const E::BinaryExpressionPtr lhs = Cast<E::BinaryExpression>(build_expr);
      const E::BinaryExpressionPtr rhs = Cast<E::BinaryExpression>(probe_expr);
      return lhs->operation().getBinaryOperationID()
                 == rhs->operation().getBinaryOperationID()
          && isIsomorphic(lhs->left(), rhs->left(), substitution_map)
          && isIsomorphic(lhs->right(), rhs->right(), substitution_map);
    }
    case E::ExpressionType::kCast: {
      const E::CastPtr lhs = Cast<E::Cast>(build_expr);
      const E::CastPtr rhs = Cast<E::Cast>(probe_expr);
      return lhs->getValueType().equals(rhs->getValueType())
          && isIsomorphic(lhs->operand(), rhs->operand(), substitution_map);
    }
    case E::ExpressionType::kComparisonExpression: {
      const E::ComparisonExpressionPtr lhs = Cast<E::ComparisonExpression>(build_expr);
      const E::ComparisonExpressionPtr rhs = Cast<E::ComparisonExpression>(probe_expr);
      return lhs->comparison().getComparisonID() == rhs->comparison().getComparisonID()
          && isIsomorphic(lhs->left(), rhs->left(), substitution_map)
          && isIsomorphic(lhs->right(), rhs->right(), substitution_map);
    }
    case E::ExpressionType::kLogicalAnd: {
      const E::LogicalAndPtr lhs = Cast<E::LogicalAnd>(build_expr);
      const E::LogicalAndPtr rhs = Cast<E::LogicalAnd>(probe_expr);
      if (lhs->operands().size() != rhs->operands().size()) {
        return false;
      }
      for (std::size_t i = 0; i < lhs->operands().size(); ++i) {
        if (!isIsomorphic(lhs->operands()[i], rhs->operands()[i], substitution_map)) {
          return false;
        }
      }
      return true;
    }
    case E::ExpressionType::kLogicalOr: {
      const E::LogicalOrPtr lhs = Cast<E::LogicalOr>(build_expr);
      const E::LogicalOrPtr rhs = Cast<E::LogicalOr>(probe_expr);
      if (lhs->operands().size() != rhs->operands().size()) {
        return false;
      }
      for (std::size_t i = 0; i < lhs->operands().size(); ++i) {
        if (!isIsomorphic(lhs->operands()[i], rhs->operands()[i], substitution_map)) {
          return false;
        }
      }
      return true;
    }
    case E::ExpressionType::kLogicalNot: {
      const E::LogicalNotPtr lhs = Cast<E::LogicalNot>(build_expr);
      const E::LogicalNotPtr rhs = Cast<E::LogicalNot>(probe_expr);
      return isIsomorphic(lhs->operand(), rhs->operand(), substitution_map);
    }
    case E::ExpressionType::kPredicateLiteral: {
      const E::PredicateLiteralPtr lhs = Cast<E::PredicateLiteral>(build_expr);
      const E::PredicateLiteralPtr rhs = Cast<E::PredicateLiteral>(probe_expr);
      return lhs->is_true() == rhs->is_true();
    }
    case E::ExpressionType::kScalarLiteral: {
      const E::ScalarLiteralPtr lhs = Cast<E::ScalarLiteral>(build_expr);
      const E::ScalarLiteralPtr rhs = Cast<E::ScalarLiteral>(probe_expr);
      return lhs->equals(rhs);
    }
    case E::ExpressionType::kUnaryExpression: {
      const E::UnaryExpressionPtr lhs = Cast<E::UnaryExpression>(build_expr);
      const E::UnaryExpressionPtr rhs = Cast<E::UnaryExpression>(probe_expr);
      return lhs->operation().getUnaryOperationID()
                 == rhs->operation().getUnaryOperationID()
          && isIsomorphic(lhs->operand(), rhs->operand(), substitution_map);
    }
    // TODO(jianqiao): kInTableQuery, kSearchedCase, kSimpleCase
    default:
      break;
  }
  return false;
}

}  // namespace optimizer
}  // namespace quickstep
