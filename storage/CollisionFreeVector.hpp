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

#ifndef QUICKSTEP_STORAGE_COLLISION_FREE_JOIN_HASH_TABLE_HPP_
#define QUICKSTEP_STORAGE_COLLISION_FREE_JOIN_HASH_TABLE_HPP_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <vector>

#include "storage/HashTable.pb.h"
#include "storage/StorageBlob.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageConstants.hpp"
#include "storage/StorageManager.hpp"
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorUtil.hpp"
#include "types/Type.hpp"
#include "types/TypeFactory.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief A hash table implementation which uses a fixed array for buckets.
 **/
class CollisionFreeVector {
 public:
  static CollisionFreeVector* ReconstructFromProto(const serialization::HashTable &proto,
                                                   StorageManager *storage_manager) {
    DCHECK(ProtoIsValid(proto))
        << "Attempted to create CollisionFreeVector from invalid proto description:\n"
        << proto.DebugString();

    const serialization::CollisionFreeVectorInfo &proto_collision_free_vector_info =
        proto.collision_free_vector_info();

    return new CollisionFreeVector(TypeFactory::ReconstructFromProto(proto.key_types(0)),
                                   proto.estimated_num_entries(),
                                   proto_collision_free_vector_info.memory_size(),
                                   proto_collision_free_vector_info.num_init_partitions(),
                                   storage_manager);
  }

  static bool ProtoIsValid(const serialization::HashTable &proto) {
    return proto.IsInitialized() &&
           proto.hash_table_impl_type() ==
               serialization::HashTableImplType::COLLISION_FREE_VECTOR &&
           proto.key_types_size() == 1u &&
           TypeFactory::ProtoIsValid(proto.key_types(0)) &&
           proto.has_collision_free_vector_info() &&
           proto.collision_free_vector_info().IsInitialized();
  }

  /**
   * @brief The payload size.
   *
   * @return the payload size.
   */
  static std::size_t payloadSize() {
    return sizeof(TupleReference);
  }

  ~CollisionFreeVector() {
    const block_id blob_id = blob_->getID();
    blob_.release();
    storage_manager_->deleteBlockOrBlobFile(blob_id);
  }

  template <typename FunctorT>
  void getAllFromValueAccessor(ValueAccessor *accessor,
                               const attribute_id key_attr_id,
                               const bool check_for_null_key,
                               FunctorT *functor) const {
    // Pass through to method with additional template parameters for less
    // branching in inner loop.
    if (check_for_null_key) {
      getAllFromValueAccessorImpl<true, FunctorT>(
          accessor, key_attr_id, functor);
    } else {
      getAllFromValueAccessorImpl<false, FunctorT>(
          accessor, key_attr_id, functor);
    }
  }

  template <typename FunctorT>
  void getAllFromValueAccessorWithExtraWorkForFirstMatch(
          ValueAccessor *accessor,
          const attribute_id key_attr_id,
          const bool check_for_null_key,
          FunctorT *functor) const {
    InvokeOnAnyValueAccessor(
        accessor,
        [&](auto *accessor) -> void {  // NOLINT(build/c++11)
      while (accessor->next()) {
        TypedValue key = accessor->getTypedValue(key_attr_id);
        if (check_for_null_key && key.isNull()) {
          continue;
        }
        const std::size_t hash_code = key.getHash();
        const TupleReference *value;
        if (getNextEntryForKey(hash_code, &value)) {
          functor->recordMatch(*accessor);
          (*functor)(*accessor, *value);
          continue;
        }
      }
    });  // NOLINT(whitespace/parens)
  }

  // If run_if_match_found is true, apply the functor to each key if a match is
  // found; otherwise, apply the functor if no match is found.
  template <bool run_if_match_found, typename FunctorT>
  void runOverKeysFromValueAccessor(ValueAccessor *accessor,
                                    const attribute_id key_attr_id,
                                    const bool check_for_null_key,
                                    FunctorT *functor) const {
    InvokeOnAnyValueAccessor(
        accessor,
        [&](auto *accessor) -> void {  // NOLINT(build/c++11)
      while (accessor->next()) {
        TypedValue key = accessor->getTypedValue(key_attr_id);
        if (check_for_null_key && key.isNull()) {
          if (!run_if_match_found) {
            (*functor)(*accessor);
            continue;
          }
        }
        if (run_if_match_found) {
          if (hasKey(key)) {
            (*functor)(*accessor);
          }
        } else {
          if (!hasKey(key)) {
            (*functor)(*accessor);
          }
        }
      }
    });  // NOLINT(whitespace/parens)
  }

  template <typename FunctorT>
  HashTablePutResult putValueAccessor(ValueAccessor *accessor,
                                      const attribute_id key_attr_id,
                                      const bool check_for_null_key,
                                      FunctorT *functor) {
    InvokeOnAnyValueAccessor(
        accessor,
        [&](auto *accessor) -> void {  // NOLINT(build/c++11)
      while (accessor->next()) {
        const TypedValue key = accessor->getTypedValue(key_attr_id);
        if (check_for_null_key && key.isNull()) {
          continue;
        }

        const std::size_t hash_code = key.getHashScalarLiteral();
        DCHECK_LT(hash_code, max_num_entries_);

        values_[hash_code] = (*functor)(*accessor);
      }
    });

    return HashTablePutResult::kOK;
  }

  /**
   * @brief Get the number of partitions to be used for initializing the table.
   *
   * @return The number of partitions to be used for initializing the table.
   */
  inline std::size_t getNumInitializationPartitions() const {
    return num_init_partitions_;
  }

  /**
   * @brief Initialize the specified partition of this join hash table.
   *
   * @param partition_id ID of the partition to be initialized.
   */
  inline void initialize(const std::size_t partition_id) {
    const std::size_t memory_start = kCollisonFreeVectorInitBlobSize * partition_id;
    std::memset(reinterpret_cast<TupleReference*>(blob_->getMemoryMutable()) + memory_start,
                0,
                std::min(kCollisonFreeVectorInitBlobSize,
                         memory_size_ - memory_start));
  }

 protected:
  /**
   * @brief Constructor.
   *
   * @param key_type The join key type.
   * @param num_entries The estimated number of entries this table will hold.
   * @param memory_size The memory size for this table.
   * @param num_init_partitions The number of partitions to be used for
   *        initializing the hash table.
   * @param storage_manager The StorageManager to use (a StorageBlob will be
   *        allocated to hold this table's contents).
   **/
  CollisionFreeVector(const Type &key_type,
                      const std::size_t num_entries,
                      const std::size_t memory_size,
                      const std::size_t num_init_partitions,
                      StorageManager *storage_manager)
      : max_num_entries_(num_entries),
        memory_size_(memory_size),
        num_init_partitions_(num_init_partitions),
        storage_manager_(storage_manager) {
    DCHECK(TypedValue::HashIsReversible(key_type.getTypeID()));
    DCHECK_GT(num_entries, 0u);

    const std::size_t num_storage_slots =
        storage_manager->SlotsNeededForBytes(memory_size);

    const block_id blob_id = storage_manager->createBlob(num_storage_slots);
    blob_ = storage_manager->getBlobMutable(blob_id);

    values_ = reinterpret_cast<TupleReference*>(blob_->getMemoryMutable());
  }

  bool getNextEntryForKey(const std::size_t hash_code,
                          const TupleReference **value) const {
    DCHECK_LT(hash_code, max_num_entries_);
    if (!values_[hash_code].isValid()) {
      return false;
    }

    *value = values_ + hash_code;
    return true;
  }

  bool hasKey(const TypedValue &key) const {
    const std::size_t hash_code = key.getHashScalarLiteral();
    DCHECK_LT(hash_code, max_num_entries_);
    return values_[hash_code].isValid();
  }

 private:
  template <bool check_for_null_key, typename FunctorT>
  void getAllFromValueAccessorImpl(ValueAccessor *accessor,
                                   const attribute_id key_attr_id,
                                   FunctorT *functor) const {
    InvokeOnAnyValueAccessor(
        accessor,
        [&](auto *accessor) -> void {  // NOLINT(build/c++11)
      while (accessor->next()) {
        const TypedValue key = accessor->getTypedValue(key_attr_id);
        if (check_for_null_key && key.isNull()) {
          continue;
        }
        const TupleReference *value;
        if (getNextEntryForKey(key.getHashScalarLiteral(), &value)) {
          (*functor)(*accessor, *value);
        }
      }
    });
  }

  const std::size_t max_num_entries_;
  const std::size_t memory_size_;
  const std::size_t num_init_partitions_;

  StorageManager *storage_manager_;
  MutableBlobReference blob_;

  TupleReference *values_;

  DISALLOW_COPY_AND_ASSIGN(CollisionFreeVector);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_COLLISION_FREE_JOIN_HASH_TABLE_HPP_
