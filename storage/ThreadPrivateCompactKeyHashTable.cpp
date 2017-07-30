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

#include "storage/ThreadPrivateCompactKeyHashTable.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <vector>

#include "expressions/aggregation/AggregateFunctionTraits.hpp"
#include "expressions/aggregation/AggregationHandle.hpp"
#include "expressions/aggregation/AggregationID.hpp"
#include "storage/StorageBlob.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageManager.hpp"
#include "storage/ValueAccessorMultiplexer.hpp"
#include "types/Type.hpp"
#include "types/TypeID.hpp"
#include "types/containers/ColumnVectorsValueAccessor.hpp"
#include "utility/ScopedBuffer.hpp"
#include "utility/meta/Dispatchers.hpp"

#include "glog/logging.h"

namespace quickstep {

namespace {

using KeySizeDispatcher = meta::SequenceDispatcher<
    meta::Sequence<std::size_t, 1u, 2u, 3u, 4u, 5u, 6u, 7u, 8u>>;

using ArgumentTypeIDDispatcher = meta::SequenceDispatcher<
    meta::Sequence<TypeID, kInt, kLong, kFloat, kDouble,
                   kDecimal2, kDecimal4, kDecimal6>>;

using AggregationIDDispatcher = meta::SequenceDispatcher<
    meta::Sequence<AggregationID, kSum>>;

using AggregateFunctionDispatcher =
    AggregationIDDispatcher
        ::set_next<ArgumentTypeIDDispatcher>
        ::set_transformer<AggregateFunctionTransformer<false>>;

}  // namespace

constexpr std::size_t ThreadPrivateCompactKeyHashTable::kKeyCodeSize;

ThreadPrivateCompactKeyHashTable::ThreadPrivateCompactKeyHashTable(
    const std::vector<const Type*> &key_types,
    const std::size_t num_entries,
    const std::vector<AggregationHandle*> &handles,
    StorageManager *storage_manager)
    : key_types_(key_types),
      handles_(handles),
      total_state_size_(0),
      num_buckets_(0),
      buckets_allocated_(0),
      storage_manager_(storage_manager) {
  // Cache key sizes.
  for (const Type *key_type : key_types) {
    DCHECK(!key_type->isVariableLength());
    DCHECK(!key_type->isNullable());
    key_sizes_.emplace_back(key_type->maximumByteLength());
  }

  for (const AggregationHandle *handle : handles) {
    const std::vector<const Type*> arg_types = handle->getArgumentTypes();
    DCHECK_LE(arg_types.size(), 1u);
    DCHECK(arg_types.empty() || !arg_types.front()->isNullable());

    // Figure out state size.
    std::size_t state_size = 0;
    if (handle->getAggregationID() == kCount) {
      state_size = sizeof(std::atomic<std::int64_t>);
    } else {
      DCHECK(!arg_types.empty());
      AggregateFunctionDispatcher::InvokeOn(
          handle->getAggregationID(),
          arg_types.front()->getTypeID(),
          [&](auto typelist) -> void {
        state_size = sizeof(typename decltype(typelist)::head::StateType);
      });
    }
    DCHECK_NE(state_size, 0);
    state_sizes_.emplace_back(state_size);
    total_state_size_ += state_size;
  }

  // Calculate required memory size for keys and states.
  const std::size_t required_memory =
      num_entries * (kKeyCodeSize + total_state_size_);
  const std::size_t num_storage_slots =
      storage_manager_->SlotsNeededForBytes(required_memory);

  // Use storage manager to allocate memory.
  const block_id blob_id = storage_manager->createBlob(num_storage_slots);
  blob_ = storage_manager->getBlobMutable(blob_id);

  num_buckets_ = blob_->size() / (kKeyCodeSize + total_state_size_);
  void *memory = blob_->getMemoryMutable();

  // Calculate the memory locations of state vectors.
  keys_ = static_cast<KeyCode*>(memory);
  char *state_memory = static_cast<char*>(memory) + num_buckets_ * kKeyCodeSize;
  std::memset(state_memory, 0, num_buckets_ * total_state_size_);

  for (std::size_t i = 0; i < state_sizes_.size(); ++i) {
    state_vecs_.emplace_back(state_memory);
    state_memory += num_buckets_ * state_sizes_[i];
  }
}

ThreadPrivateCompactKeyHashTable::~ThreadPrivateCompactKeyHashTable() {
  // Release the blob.
  if (blob_.valid()) {
    const block_id blob_id = blob_->getID();
    blob_.release();
    storage_manager_->deleteBlockOrBlobFile(blob_id);
  }
}

void ThreadPrivateCompactKeyHashTable::resize() {
  DCHECK_EQ(buckets_allocated_, num_buckets_);

  const std::size_t resized_memory_size =
      num_buckets_ * 2 * (kKeyCodeSize + total_state_size_);
  const std::size_t resized_num_slots =
      storage_manager_->SlotsNeededForBytes(resized_memory_size);

  const block_id resized_blob_id =
      storage_manager_->createBlob(resized_num_slots);
  MutableBlobReference resized_blob =
      storage_manager_->getBlobMutable(resized_blob_id);

  const std::size_t resized_num_buckets =
      resized_blob->size() / (kKeyCodeSize + total_state_size_);
  void *resized_memory = resized_blob->getMemoryMutable();

  KeyCode *resized_keys = static_cast<KeyCode*>(resized_memory);
  std::memcpy(resized_keys, keys_, buckets_allocated_ * kKeyCodeSize);
  keys_ = resized_keys;

  char *resized_state_memory =
      static_cast<char*>(resized_memory) + resized_num_buckets * kKeyCodeSize;
  for (std::size_t i = 0; i < state_sizes_.size(); ++i) {
    const std::size_t vec_size = buckets_allocated_ * state_sizes_[i];
    const std::size_t resized_vec_size = resized_num_buckets * state_sizes_[i];

    std::memcpy(resized_state_memory, state_vecs_[i], vec_size);
    std::memset(resized_state_memory + vec_size,
                0,
                resized_vec_size - vec_size);

    state_vecs_[i] = resized_state_memory;
    resized_state_memory += resized_vec_size;
  }

  std::swap(blob_, resized_blob);
  num_buckets_ = resized_num_buckets;

  const block_id blob_id_to_delete = resized_blob->getID();
  resized_blob.release();
  storage_manager_->deleteBlockOrBlobFile(blob_id_to_delete);
}

bool ThreadPrivateCompactKeyHashTable::upsertValueAccessorCompositeKey(
    const std::vector<std::vector<MultiSourceAttributeId>> &argument_ids,
    const std::vector<MultiSourceAttributeId> &key_attr_ids,
    const ValueAccessorMultiplexer &accessor_mux) {
  ValueAccessor *base_accessor = accessor_mux.getBaseAccessor();
  ValueAccessor *derived_accessor = accessor_mux.getDerivedAccessor();

  DCHECK(base_accessor != nullptr);
  const std::size_t num_tuples = base_accessor->getNumTuplesVirtual();

  ScopedBuffer buffer(num_tuples * kKeyCodeSize);
  KeyCode *key_codes = static_cast<KeyCode*>(buffer.get());
  std::size_t key_code_offset = 0;
  for (std::size_t i = 0; i < key_attr_ids.size(); ++i) {
    const auto &key_attr_id = key_attr_ids[i];
    ValueAccessor *accessor =
        key_attr_id.source == ValueAccessorSource::kBase
            ? base_accessor
            : derived_accessor;
    DCHECK(accessor != nullptr);

    // Pack the key component into the 64-bit code (with proper offset).
    KeySizeDispatcher::InvokeOn(
        key_sizes_[i],
        [&](auto typelist) -> void {  // NOLINT(build/c++11)
      ConstructKeyCode<decltype(typelist)::head::value>(
          key_code_offset, key_attr_id.attr_id, accessor, key_codes);
    });
    key_code_offset += key_sizes_[i];
  }

  std::vector<BucketIndex> bucket_indices(num_tuples);
  for (std::size_t i = 0; i < num_tuples; ++i) {
    const std::size_t code = key_codes[i];
    const auto index_it = index_.find(code);
    if (index_it == index_.end()) {
      if (buckets_allocated_ >= num_buckets_) {
        resize();
      }
      index_.emplace(code, buckets_allocated_);
      bucket_indices[i] = buckets_allocated_;
      keys_[buckets_allocated_] = code;
      ++buckets_allocated_;
    } else {
      bucket_indices[i] = index_it->second;
    }
  }

  // Dispatch on AggregationID and argument type.
  for (std::size_t i = 0; i < handles_.size(); ++i) {
    const AggregationHandle *handle = handles_[i];
    if (handle->getAggregationID() == kCount) {
      upsertValueAccessorCount(bucket_indices, state_vecs_[i]);
    } else {
      DCHECK_EQ(1u, argument_ids[i].size());
      const auto &argument_id = argument_ids[i].front();
      ValueAccessor *accessor =
          argument_id.source == ValueAccessorSource::kBase
              ? base_accessor
              : derived_accessor;
      DCHECK(accessor != nullptr);
      DCHECK_EQ(1u, handle->getArgumentTypes().size());
      AggregateFunctionDispatcher::InvokeOn(
          handle->getAggregationID(),
          handle->getArgumentTypes().front()->getTypeID(),
          [&](auto typelist) -> void {
        this->upsertValueAccessorGeneric<typename decltype(typelist)::head>(
            bucket_indices, argument_id.attr_id, accessor, state_vecs_[i]);
      });
    }
  }

  return true;
}

void ThreadPrivateCompactKeyHashTable::mergeFrom(
    const ThreadPrivateCompactKeyHashTable &source) {
  // First merge keys and generate location mappings. That is, source hash
  // table's bucket *i* should be merged into destination hash table's bucket
  // *dst_bucket_indices[i]*.
  std::vector<BucketIndex> dst_bucket_indices(source.buckets_allocated_);
  const KeyCode *src_keys = source.keys_;
  for (std::size_t i = 0; i < source.buckets_allocated_; ++i) {
    const KeyCode code = src_keys[i];
    const auto index_it = index_.find(code);

    if (index_it == index_.end()) {
      if (buckets_allocated_ >= num_buckets_) {
        resize();
      }
      index_.emplace(code, buckets_allocated_);
      dst_bucket_indices[i] = buckets_allocated_;
      keys_[buckets_allocated_] = code;
      ++buckets_allocated_;
    } else {
      dst_bucket_indices[i] = index_it->second;
    }
  }

  // Then merge states in a column-wise way based on dst_bucket_indices.
  for (std::size_t i = 0; i < handles_.size(); ++i) {
    const AggregationHandle *handle = handles_[i];
    if (handle->getAggregationID() == kCount) {
      mergeStateCount(dst_bucket_indices, source.state_vecs_[i], state_vecs_[i]);
    } else {
      DCHECK_EQ(1u, handle->getArgumentTypes().size());
      const Type *argument_type = handle->getArgumentTypes().front();
      AggregateFunctionDispatcher::InvokeOn(
          handle->getAggregationID(),
          argument_type->getTypeID(),
          [&](auto typelist) -> void {
        this->mergeStateGeneric<typename decltype(typelist)::head>(
            dst_bucket_indices, source.state_vecs_[i], state_vecs_[i]);
      });
    }
  }
}

void ThreadPrivateCompactKeyHashTable::finalize(
    ColumnVectorsValueAccessor *output) const {
  // First finalize keys.
  std::size_t key_offset = 0;
  for (std::size_t i = 0; i < key_types_.size(); ++i) {
    const Type &key_type = *key_types_[i];
    std::unique_ptr<NativeColumnVector> native_cv(
        std::make_unique<NativeColumnVector>(key_type, buckets_allocated_));

    KeySizeDispatcher::InvokeOn(
        key_sizes_[i],
        [&](auto typelist) -> void {  // NOLINT(build/c++11)
      this->finalizeKey<decltype(typelist)::head::value>(key_offset, native_cv.get());
    });
    output->addColumn(native_cv.release());
    key_offset += key_sizes_[i];
  }

  // Then finalize states.
  for (std::size_t i = 0; i < handles_.size(); ++i) {
    const AggregationHandle *handle = handles_[i];
    const Type &result_type = *handle->getResultType();
    std::unique_ptr<NativeColumnVector> native_cv(
        std::make_unique<NativeColumnVector>(result_type, buckets_allocated_));

    if (handle->getAggregationID() == kCount) {
      finalizeStateCount(state_vecs_[i], native_cv.get());
    } else {
      DCHECK_EQ(1u, handle->getArgumentTypes().size());
      AggregateFunctionDispatcher::InvokeOn(
          handle->getAggregationID(),
          handle->getArgumentTypes().front()->getTypeID(),
          [&](auto typelist) -> void {
        this->finalizeStateGeneric<typename decltype(typelist)::head>(
            state_vecs_[i], native_cv.get());
      });
    }

    output->addColumn(native_cv.release());
  }
}

}  // namespace quickstep
