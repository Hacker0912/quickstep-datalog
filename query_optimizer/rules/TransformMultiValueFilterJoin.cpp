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

#include <vector>

#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/ExpressionUtil.hpp"
#include "query_optimizer/logical/HashJoin.hpp"
#include "query_optimizer/logical/Logical.hpp"
#include "query_optimizer/logical/PatternMatcher.hpp"

namespace quickstep {
namespace optimizer {

namespace E = ::quickstep::optimizer::expressions;
namespace L = ::quickstep::optimizer::logical;

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
    const L::HashJoinPtr &input) {
  return input;
}

}  // namespace optimizer
}  // namespace quickstep
