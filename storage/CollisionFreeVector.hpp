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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <vector>

#include "storage/HashTable.hpp"
#include "storage/HashTable.pb.h"
#include "storage/StorageBlob.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageConstants.hpp"
#include "storage/StorageManager.hpp"
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
class CollisionFreeVector final : public HashTable<TupleReference, false, false, false, false> {
 public:
  static CollisionFreeVector* ReconstructFromProto(const serialization::HashTable &proto,
                                                   StorageManager *storage_manager) {
    DCHECK(ProtoIsValid(proto))
        << "Attempted to create CollisionFreeVector from invalid proto description:\n"
        << proto.DebugString();

    std::vector<const Type*> key_types;
    for (int i = 0; i < proto.key_types_size(); ++i) {
      key_types.emplace_back(&TypeFactory::ReconstructFromProto(proto.key_types(i)));
    }

    const serialization::CollisionFreeVectorInfo &proto_collision_free_vector_info =
        proto.collision_free_vector_info();

    return new CollisionFreeVector(key_types, proto.estimated_num_entries(),
                                   proto_collision_free_vector_info.memory_size(),
                                   proto_collision_free_vector_info.num_init_partitions(),
                                   storage_manager);
  }

  static bool ProtoIsValid(const serialization::HashTable &proto) {
    if (!proto.IsInitialized() ||
        proto.hash_table_impl_type() != serialization::HashTableImplType::COLLISION_FREE_VECTOR) {
      return false;
    }

    for (int i = 0; i < proto.key_types_size(); ++i) {
      if (!TypeFactory::ProtoIsValid(proto.key_types(i))) {
        return false;
      }
    }

    return proto.has_collision_free_vector_info() &&
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

  ~CollisionFreeVector() override {
    const block_id blob_id = this->blob_->getID();
    this->blob_.release();
    this->storage_manager_->deleteBlockOrBlobFile(blob_id);
  }

  void clear() override {
  }

  std::size_t numEntries() const override {
    return num_entries_;
  }

  const TupleReference* getSingle(const TypedValue &key) const override {
    DCHECK(key.isPlausibleInstanceOf(this->key_types_.front()->getSignature()));

    const std::size_t hash_code = key.getHashScalarLiteral();
    DCHECK_LT(hash_code, max_num_entries_);
    const TupleReference &value = *(values_ + hash_code);
    if (value.isValid()) {
      return &value;
    }

    return nullptr;
  }

  const TupleReference* getSingleCompositeKey(const std::vector<TypedValue> &key) const override {
    DCHECK_EQ(1u, key.size());
    return getSingle(key.front());
  }

  void getAll(const TypedValue &key,
              std::vector<const TupleReference*> *values) const override {
    DCHECK(key.isPlausibleInstanceOf(this->key_types_.front()->getSignature()));

    const std::size_t hash_code = key.getHashScalarLiteral();
    DCHECK_LT(hash_code, max_num_entries_);
    const TupleReference &value = *(values_ + hash_code);
    if (value.isValid()) {
      values->push_back(&value);
    }
  }
  void getAllCompositeKey(const std::vector<TypedValue> &key,
                          std::vector<const TupleReference*> *values) const override {
    DCHECK_EQ(1u, key.size());
    return getAll(key.front(), values);
  }

  std::size_t getHashTableMemorySizeBytes() const override {
    return this->hash_table_memory_size_ + sizeof(TupleReference) * max_num_entries_;
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
    std::memset(reinterpret_cast<TupleReference*>(this->blob_->getMemoryMutable()) + memory_start,
                0,
                std::min(kCollisonFreeVectorInitBlobSize,
                         this->hash_table_memory_size_ - memory_start));
  }

 protected:
  /**
   * @brief Constructor.
   *
   * @param key_types The join key type.
   * @param num_entries The estimated number of entries this table will hold.
   * @param memory_size The memory size for this table.
   * @param num_init_partitions The number of partitions to be used for
   *        initializing the hash table.
   * @param storage_manager The StorageManager to use (a StorageBlob will be
   *        allocated to hold this table's contents).
   **/
  CollisionFreeVector(const std::vector<const Type *> &key_types,
                      const std::size_t num_entries,
                      const std::size_t memory_size,
                      const std::size_t num_init_partitions,
                      StorageManager *storage_manager)
      : HashTable<TupleReference, false, false, false, false>(
            key_types, nullptr, memory_size, true, true, false, true, true),
        key_type_id_(key_types.front()->getTypeID()),
        max_num_entries_(num_entries),
        num_init_partitions_(num_init_partitions),
        num_entries_(0) {
    DCHECK_EQ(1u, key_types.size());
    DCHECK(TypedValue::HashIsReversible(key_type_id_));
    DCHECK_GT(num_entries, 0u);

    this->storage_manager_ = storage_manager;

    const std::size_t num_storage_slots =
        storage_manager->SlotsNeededForBytes(memory_size);

    const block_id blob_id = storage_manager->createBlob(num_storage_slots);
    this->blob_ = storage_manager->getBlobMutable(blob_id);

    values_ = reinterpret_cast<TupleReference*>(this->blob_->getMemoryMutable());
  }

  HashTablePutResult putInternal(const TypedValue &key,
                                 const std::size_t variable_key_size,
                                 const TupleReference &value,
                                 HashTablePreallocationState *prealloc_state) override {
    DCHECK(key.isPlausibleInstanceOf(this->key_types_.front()->getSignature()));

    const std::size_t hash_code = key.getHashScalarLiteral();
    DCHECK_LT(hash_code, max_num_entries_);
    if ((values_ + hash_code)->isValid()) {
      return HashTablePutResult::kDuplicateKey;
    }

    *(values_ + hash_code) = value;

    ++num_entries_;
    return HashTablePutResult::kOK;
  }

  HashTablePutResult putCompositeKeyInternal(
      const std::vector<TypedValue> &key,
      const std::size_t variable_key_size,
      const TupleReference &value,
      HashTablePreallocationState *prealloc_state) override {
    DCHECK_EQ(1u, key.size());
    return putInternal(key.front(), variable_key_size, value, prealloc_state);
  }

  TupleReference* upsertInternal(const TypedValue &key,
                         const std::size_t variable_key_size,
                         const TupleReference &initial_value) override {
    DCHECK(key.isPlausibleInstanceOf(this->key_types_.front()->getSignature()));
    DCHECK_EQ(0, variable_key_size);

    const std::size_t hash_code = key.getHashScalarLiteral();
    DCHECK_LT(hash_code, max_num_entries_);
    if (!(values_ + hash_code)->isValid()) {
      *(values_ + hash_code) = initial_value;
      ++num_entries_;
    }

    return values_ + hash_code;
  }

  TupleReference* upsertCompositeKeyInternal(const std::vector<TypedValue> &key,
                                     const std::size_t variable_key_size,
                                     const TupleReference &initial_value) override {
    DCHECK_EQ(1u, key.size());
    return upsertInternal(key.front(), variable_key_size, initial_value);
  }

  bool getNextEntry(TypedValue *key, const TupleReference **value, std::size_t *entry_num) const override {
    if (*entry_num >= max_num_entries_) {
      return false;
    }

    // Reconstruct key by reversing hash.
    *key = TypedValue(key_type_id_, *entry_num);
    *value = values_ + (*entry_num);
    ++(*entry_num);
    return true;
  }

  bool getNextEntryCompositeKey(std::vector<TypedValue> *key,
                                const TupleReference **value,
                                std::size_t *entry_num) const override {
    key->resize(1);
    return getNextEntry(&(key->front()), value, entry_num);
  }

  bool getNextEntryForKey(const TypedValue &key,
                          const std::size_t hash_code,
                          const TupleReference **value,
                          std::size_t *entry_num) const override {
    DCHECK(key.isPlausibleInstanceOf(this->key_types_.front()->getSignature()));

    if (*entry_num >= max_num_entries_ ||
        hash_code >= max_num_entries_ ||
        !(values_ + hash_code)->isValid()) {
      return false;
    }

    *value = values_ + hash_code;
    *entry_num = max_num_entries_;
    return true;
  }

  bool getNextEntryForCompositeKey(const std::vector<TypedValue> &key,
                                   const std::size_t hash_code,
                                   const TupleReference **value,
                                   std::size_t *entry_num) const override {
    DCHECK_EQ(1u, key.size());
    return getNextEntryForKey(key.front(), hash_code, value, entry_num);
  }

  bool hasKey(const TypedValue &key) const override {
    const std::size_t hash_code = key.getHashScalarLiteral();
    DCHECK_LT(hash_code, max_num_entries_);
    return (values_ + hash_code)->isValid();
  }

  bool hasCompositeKey(const std::vector<TypedValue> &key) const override {
    return false;
  }

  void resize(const std::size_t extra_buckets,
              const std::size_t extra_variable_storage,
              const std::size_t retry_num = 0) override {}

 private:
  // Cache the TypeID of the key.
  const TypeID key_type_id_;
  const std::size_t max_num_entries_;

  const std::size_t num_init_partitions_;

  std::atomic_size_t num_entries_;
  TupleReference *values_;

  DISALLOW_COPY_AND_ASSIGN(CollisionFreeVector);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_COLLISION_FREE_JOIN_HASH_TABLE_HPP_
