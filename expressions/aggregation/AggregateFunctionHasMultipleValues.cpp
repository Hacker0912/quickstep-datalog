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

#include "expressions/aggregation/AggregateFunctionHasMultipleValues.hpp"

#include <vector>

#include "expressions/aggregation/AggregationHandleHasMultipleValues.hpp"
#include "types/IntType.hpp"
#include "types/Type.hpp"
#include "types/TypeID.hpp"
#include "utility/EqualsAnyConstant.hpp"

#include "glog/logging.h"

namespace quickstep {

bool AggregateFunctionHasMultipleValues::canApplyToTypes(
    const std::vector<const Type*> &argument_types) const {
  if (argument_types.size() != 1) {
    return false;
  }

  // TODO(jianqiao): Systematically handle the types.
  return QUICKSTEP_EQUALS_ANY_CONSTANT(
      argument_types.front()->getTypeID(),
      kInt, kLong, kFloat, kDouble, kDecimal2, kDecimal4, kDecimal6);
}

const Type* AggregateFunctionHasMultipleValues::resultTypeForArgumentTypes(
    const std::vector<const Type*> &argument_types) const {
  if (!canApplyToTypes(argument_types)) {
    return nullptr;
  }

  return &IntType::InstanceNonNullable();
}

AggregationHandle* AggregateFunctionHasMultipleValues::createHandle(
    const std::vector<const Type*> &argument_types) const {
  DCHECK(canApplyToTypes(argument_types))
      << "Attempted to create an AggregateFunctionHasMultipleValues for "
      << "argument Type(s) that SUM can not be applied to.";

  return new AggregationHandleHasMultipleValues(*argument_types.front());
}

}  // namespace quickstep
