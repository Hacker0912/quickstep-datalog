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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_LOGICAL_SAME_GENERATION_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_LOGICAL_SAME_GENERATION_HPP_

#include <memory>
#include <string>
#include <vector>

#include "query_optimizer/OptimizerTree.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/logical/Logical.hpp"
#include "query_optimizer/logical/LogicalType.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {
namespace optimizer {
namespace logical {

/** \addtogroup OptimizerLogical
 *  @{
 */

class SameGeneration;
typedef std::shared_ptr<const SameGeneration> SameGenerationPtr;

/**
 * @brief SameGeneration operator that recursively generates tuples that belongs
 *        the same generation and outputs them as a new relation.
 */
class SameGeneration : public Logical {
 public:
  LogicalType getLogicalType() const override { return LogicalType::kSameGeneration; }

  std::string getName() const override { return "SameGeneration"; }

  LogicalPtr copyWithNewChildren(
      const std::vector<LogicalPtr> &new_children) const override {
    DCHECK_EQ(children().size(), new_children.size());
    return Create(new_children[0]);
  }

  std::vector<expressions::AttributeReferencePtr>
      getOutputAttributes() const override {
    return input_->getOutputAttributes();
  }

  std::vector<expressions::AttributeReferencePtr>
      getReferencedAttributes() const override {
    return input_->getReferencedAttributes();
  }

  /**
   * @return The input operator.
   */
  const LogicalPtr& input() const { return input_; }

  /**
   * @brief Creates a SameGeneration operator that operates \p input.
   *
   * @param input The input operator to this SameGeneration.
   *
   * @return An immutable SameGeneration.
   */
  static SameGenerationPtr Create(const LogicalPtr &input) {
    return SameGenerationPtr(new SameGeneration(input));
  }

 protected:
  void getFieldStringItems(
      std::vector<std::string> *inline_field_names,
      std::vector<std::string> *inline_field_values,
      std::vector<std::string> *non_container_child_field_names,
      std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
      std::vector<std::string> *container_child_field_names,
      std::vector<std::vector<OptimizerTreeBaseNodePtr>>
          *container_child_fields) const override {
    non_container_child_field_names->push_back("input");
    non_container_child_fields->push_back(input_);
  }

 private:
  explicit SameGeneration(const LogicalPtr &input)
      : input_(input) {
     addChild(input);
  }

  const LogicalPtr input_;

  DISALLOW_COPY_AND_ASSIGN(SameGeneration);
};

/** @} */

}  // namespace logical
}  // namespace optimizer
}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_OPTIMIZER_LOGICAL_SAME_GENERATION_HPP_
