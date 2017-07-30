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

#include "storage/CollisionFreeVectorTable.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <vector>

#include "expressions/aggregation/AggregationHandle.hpp"
#include "expressions/aggregation/AggregationID.hpp"
#include "expressions/aggregation/AggregateFunctionTraits.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageManager.hpp"
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorMultiplexer.hpp"
#include "storage/ValueAccessorUtil.hpp"
#include "types/TypeID.hpp"
#include "types/TypeTraits.hpp"
#include "types/containers/ColumnVectorsValueAccessor.hpp"
#include "utility/BarrieredReadWriteConcurrentBitVector.hpp"
#include "utility/meta/Dispatchers.hpp"

#include "glog/logging.h"

namespace quickstep {

namespace {

using KeyTypeIDDispatcher = meta::SequenceDispatcher<
    meta::Sequence<TypeID, kInt, kLong>>;

using ArgumentTypeIDDispatcher = meta::SequenceDispatcher<
    meta::Sequence<TypeID, kInt, kLong, kFloat, kDouble,
                   kDecimal2, kDecimal4, kDecimal6>>;

using AggregationIDDispatcher = meta::SequenceDispatcher<
    meta::Sequence<AggregationID, kCount, kSum, kAvg, kMin, kMax, kHasMultipleValues>>;

using AggregateFunctionDispatcher =
    AggregationIDDispatcher
        ::set_next<ArgumentTypeIDDispatcher>
        ::set_transformer<AggregateFunctionTransformer<true>>;

using BoolDispatcher = meta::SequenceDispatcher<
    meta::Sequence<bool, true, false>>;

}  // namespaces

CollisionFreeVectorTable::CollisionFreeVectorTable(
    const Type *key_type,
    const std::size_t num_entries,
    const std::size_t memory_size,
    const std::size_t num_init_partitions,
    const std::size_t num_finalize_partitions,
    const std::vector<std::size_t> &state_offsets,
    const std::vector<AggregationHandle *> &handles,
    StorageManager *storage_manager)
    : key_type_(key_type),
      num_entries_(num_entries),
      num_handles_(handles.size()),
      handles_(handles),
      memory_size_(memory_size),
      num_init_partitions_(num_init_partitions),
      num_finalize_partitions_(num_finalize_partitions),
      storage_manager_(storage_manager) {
  DCHECK_GT(num_entries, 0u);
  DCHECK_GT(num_finalize_partitions_, 0u);
  DCHECK_EQ(num_handles_, state_offsets.size());
  // std::cerr << "Use collision free!\n";

  const std::size_t num_storage_slots =
      storage_manager_->SlotsNeededForBytes(memory_size_);

  const block_id blob_id = storage_manager_->createBlob(num_storage_slots);
  blob_ = storage_manager_->getBlobMutable(blob_id);

  void *memory_start = blob_->getMemoryMutable();
  existence_map_.reset(new BarrieredReadWriteConcurrentBitVector(
      reinterpret_cast<char *>(memory_start),
      num_entries,
      false /* initialize */));

  for (std::size_t i = 0; i < num_handles_; ++i) {
    // Columnwise layout.
    vec_tables_.emplace_back(
        reinterpret_cast<char *>(memory_start) + state_offsets.at(i));
  }
}

CollisionFreeVectorTable::~CollisionFreeVectorTable() {
  const block_id blob_id = blob_->getID();
  blob_.release();
  storage_manager_->deleteBlockOrBlobFile(blob_id);
}

void CollisionFreeVectorTable::destroyPayload() {
}

bool CollisionFreeVectorTable::upsertValueAccessorCompositeKey(
    const std::vector<std::vector<MultiSourceAttributeId>> &argument_ids,
    const std::vector<MultiSourceAttributeId> &key_ids,
    const ValueAccessorMultiplexer &accessor_mux) {
  DCHECK_EQ(1u, key_ids.size());
  const ValueAccessorSource key_source = key_ids.front().source;
  const attribute_id key_id = key_ids.front().attr_id;

  if (handles_.empty()) {
    InvokeOnValueAccessorMaybeTupleIdSequenceAdapter(
        accessor_mux.getValueAccessorBySource(key_source),
        [&](auto *accessor) -> void {  // NOLINT(build/c++11)
      KeyTypeIDDispatcher::set_next<BoolDispatcher>
                         ::InvokeOn(
          key_type_->getTypeID(),
          key_type_->isNullable(),
          [&](auto typelist) -> void {
        using TL = decltype(typelist);
        using KeyT = typename TypeIDTrait<TL::template at<0>::value>::cpptype;
        constexpr bool key_nullable = TL::template at<1>::value;

        this->upsertValueAccessorKeyOnly<key_nullable, KeyT>(
            key_ids.front().attr_id, accessor);
      });
    });
    return true;
  }

  DCHECK(accessor_mux.getDerivedAccessor() == nullptr ||
         accessor_mux.getDerivedAccessor()->getImplementationType()
             == ValueAccessor::Implementation::kColumnVectors);

  ValueAccessor *base_accessor = accessor_mux.getBaseAccessor();
  ColumnVectorsValueAccessor *derived_accesor =
      static_cast<ColumnVectorsValueAccessor*>(accessor_mux.getDerivedAccessor());

  for (std::size_t i = 0; i < num_handles_; ++i) {
    const AggregationHandle *handle = handles_[i];
    const auto &argument_types = handle->getArgumentTypes();
    DCHECK_EQ(argument_types.size(), argument_ids[i].size());

    if (argument_types.empty()) {
      DCHECK(argument_ids[i].empty());
      DCHECK(handle->getAggregationID() == kCount);

      InvokeOnValueAccessorMaybeTupleIdSequenceAdapter(
          accessor_mux.getValueAccessorBySource(key_source),
          [&](auto *accessor) -> void {  // NOLINT(build/c++11)
        KeyTypeIDDispatcher::set_next<BoolDispatcher>
                           ::InvokeOn(
            key_type_->getTypeID(),
            key_type_->isNullable(),
            [&](auto typelist) -> void {
          using TL = decltype(typelist);
          using KeyT = typename TypeIDTrait<TL::template at<0>::value>::cpptype;
          constexpr bool key_nullable = TL::template at<1>::value;

          this->upsertValueAccessorCountNullary<key_nullable, KeyT>(
              key_ids.front().attr_id, vec_tables_[i], accessor);
        });
      });
      continue;
    }

    const auto &argument_ids_i = argument_ids[i];
    DCHECK_EQ(argument_types.size(), argument_ids_i.size());

    const ValueAccessorSource argument_source = argument_ids_i.front().source;
    const attribute_id argument_id = argument_ids_i.front().attr_id;
    const Type &argument_type = *argument_types.front();

    // Dispatch to specialized implementations to achieve maximum performance.
    InvokeOnValueAccessorMaybeTupleIdSequenceAdapter(
        base_accessor,
        [&](auto *accessor) -> void {  // NOLINT(build/c++11)
      AggregateFunctionDispatcher::InvokeOn(
          handle->getAggregationID(),
          argument_type.getTypeID(),
          [&](auto typelist) -> void {
        using AggFunc = typename decltype(typelist)::head;

        KeyTypeIDDispatcher::set_next<BoolDispatcher>
                           ::set_next<BoolDispatcher>
                           ::InvokeOn(
            key_type_->getTypeID(),
            key_type_->isNullable(),
            argument_type.isNullable(),
            [&](auto typelist) -> void {
          using TL = decltype(typelist);
          using KeyT = typename TypeIDTrait<TL::template at<0>::value>::cpptype;
          constexpr bool key_nullable = TL::template at<1>::value;
          constexpr bool argument_nullable = TL::template at<2>::value;

          if (key_source == ValueAccessorSource::kBase) {
            if (argument_source == ValueAccessorSource::kBase) {
              this->upsertValueAccessorInternal<
                  AggFunc, KeyT, key_nullable, argument_nullable, false>(
                      key_id, argument_id, vec_tables_[i],
                      accessor, accessor);
            } else {
              this->upsertValueAccessorInternal<
                  AggFunc, KeyT, key_nullable, argument_nullable, true>(
                      key_id, argument_id, vec_tables_[i],
                      accessor, derived_accesor);
            }
          } else {
            if (argument_source == ValueAccessorSource::kBase) {
              this->upsertValueAccessorInternal<
                  AggFunc, KeyT, key_nullable, argument_nullable, true>(
                      key_id, argument_id, vec_tables_[i],
                      derived_accesor, accessor);
            } else {
              this->upsertValueAccessorInternal<
                  AggFunc, KeyT, key_nullable, argument_nullable, false>(
                      key_id, argument_id, vec_tables_[i],
                      derived_accesor, derived_accesor);
            }
          }
        });
      });
    });
  }
  return true;
}

void CollisionFreeVectorTable::finalizeKey(const std::size_t partition_id,
                                           NativeColumnVector *output_cv) const {
  const std::size_t start_position =
      calculatePartitionStartPosition(partition_id);
  const std::size_t end_position =
      calculatePartitionEndPosition(partition_id);

  switch (key_type_->getTypeID()) {
    case TypeID::kInt:
      finalizeKeyInternal<int>(start_position, end_position, output_cv);
      return;
    case TypeID::kLong:
      finalizeKeyInternal<std::int64_t>(start_position, end_position, output_cv);
      return;
    default:
      LOG(FATAL) << "Not supported";
  }
}

void CollisionFreeVectorTable::finalizeState(const std::size_t partition_id,
                                             const std::size_t handle_id,
                                             NativeColumnVector *output_cv) const {
  const std::size_t start_position =
      calculatePartitionStartPosition(partition_id);
  const std::size_t end_position =
      calculatePartitionEndPosition(partition_id);

  const AggregationHandle *handle = handles_[handle_id];
  const auto &argument_types = handle->getArgumentTypes();

  if (argument_types.empty()) {
    DCHECK(handle->getAggregationID() == kCount);
    finalizeStateInternal<AggregateFunctionTrait<kCount, kInt, true>>(
        vec_tables_[handle_id], start_position, end_position, output_cv);
    return;
  }

  DCHECK_EQ(1u, argument_types.size());

  AggregateFunctionDispatcher::InvokeOn(
      handle->getAggregationID(),
      argument_types.front()->getTypeID(),
      [&](auto typelist) -> void {
    this->finalizeStateInternal<typename decltype(typelist)::head>(
        vec_tables_[handle_id], start_position, end_position, output_cv);
  });
}

}  // namespace quickstep
