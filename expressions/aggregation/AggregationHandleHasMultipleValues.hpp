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

#ifndef QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_HAS_MULTIPLE_VALUES_HPP_
#define QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_HAS_MULTIPLE_VALUES_HPP_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "expressions/aggregation/AggregationConcreteHandle.hpp"
#include "expressions/aggregation/AggregationHandle.hpp"
#include "storage/ValueAccessorMultiplexer.hpp"
#include "types/IntType.hpp"
#include "types/Type.hpp"
#include "types/TypedValue.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

class AggregationStateHashTableBase;
class ColumnVector;

/** \addtogroup Expressions
 *  @{
 */

class AggregationHandleHasMultipleValues : public AggregationConcreteHandle {
 public:
  ~AggregationHandleHasMultipleValues() override {}

  std::vector<const Type *> getArgumentTypes() const override {
    return {&argument_type_};
  }

  const Type* getResultType() const override {
    return &IntType::InstanceNonNullable();
  }

  AggregationState* createInitialState() const override {
    LOG(FATAL) << "Not implemented";
  }

  AggregationState* accumulateValueAccessor(
      const std::vector<MultiSourceAttributeId> &argument_ids,
      const ValueAccessorMultiplexer &accessor_mux) const override {
    LOG(FATAL) << "Not implemented";
  }

  void mergeStates(const AggregationState &source,
                   AggregationState *destination) const override {
    LOG(FATAL) << "Not implemented";
  }

  TypedValue finalize(const AggregationState &state) const override {
    LOG(FATAL) << "Not implemented";
  }

  std::size_t getPayloadSize() const override {
    LOG(FATAL) << "Not implemented";
  }

  void initPayload(std::uint8_t *byte_ptr) const override {
    LOG(FATAL) << "Not implemented";
  }

  void destroyPayload(std::uint8_t *byte_ptr) const override {
    LOG(FATAL) << "Not implemented";
  }

  inline void updateStateUnary(const TypedValue &argument,
                               std::uint8_t *byte_ptr) const override {
    LOG(FATAL) << "Not implemented";
  }

  void mergeStates(const std::uint8_t *source,
                   std::uint8_t *destination) const override {
    LOG(FATAL) << "Not implemented";
  }

  ColumnVector* finalizeHashTable(
      const AggregationStateHashTableBase &hash_table,
      const std::size_t index,
      std::vector<std::vector<TypedValue>> *group_by_keys) const override {
    LOG(FATAL) << "Not implemented";
  }

  AggregationState* aggregateOnDistinctifyHashTableForSingle(
      const AggregationStateHashTableBase &distinctify_hash_table) const override {
    LOG(FATAL) << "Not implemented";
  }

  void aggregateOnDistinctifyHashTableForGroupBy(
      const AggregationStateHashTableBase &distinctify_hash_table,
      const std::size_t index,
      AggregationStateHashTableBase *aggregation_hash_table) const override {
    LOG(FATAL) << "Not implemented";
  }

 private:
  friend class AggregateFunctionHasMultipleValues;

  explicit AggregationHandleHasMultipleValues(const Type &type);

  const Type &argument_type_;

  DISALLOW_COPY_AND_ASSIGN(AggregationHandleHasMultipleValues);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_HAS_MULTIPLE_VALUES_HPP_
