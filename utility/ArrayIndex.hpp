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

#ifndef QUICKSTEP_UTILITY_ARRAY_INDEX_HPP_
#define QUICKSTEP_UTILITY_ARRAY_INDEX_HPP_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorUtil.hpp"
#include "types/TypedValue.hpp"
#include "utility/ArrayIndex.pb.h"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

/**
 * @brief Global index that from key to its entries.
 **/
class ArrayIndex {
 public:
  static ArrayIndex* ReconstructFromProto(const serialization::ArrayIndex &proto) {
    DCHECK(proto.IsInitialized());
    return new ArrayIndex(proto.base(), proto.length(), proto.reserve_size());
  }

  /**
   * @brief Virtual destructor
   **/
  ~ArrayIndex() {
  }

  std::uint32_t base() const {
    return base_;
  }

  /**
   * @brief Add entries to this index
   *
   * @param key The index.
   * @param value The entries to add.
   **/
  void putValueAccessor(const attribute_id key_attr_id, const attribute_id value_attr_id,
                        ValueAccessor *accessor) {
    InvokeOnAnyValueAccessor(
        accessor,
        [&](auto *accessor) {
      accessor->beginIteration();
      while (accessor->next()) {
        const std::size_t key_index =
            accessor->getTypedValue(key_attr_id).getHashScalarLiteral() - base_;
        DCHECK_LT(key_index, indexes_.size());
        indexes_[key_index].push_back(accessor->getTypedValue(value_attr_id).getHashScalarLiteral());
      }
    });
  }

  const std::vector<std::uint32_t>& get(const std::uint32_t key) const {
    const std::size_t index = key - base_;
    DCHECK_LT(index, indexes_.size());
    return indexes_[index];
  }

 private:
  /**
   * @brief Constructor.
   *
   **/
  ArrayIndex(const std::uint32_t base,
             const std::size_t length,
             const std::size_t reserve_size)
      : base_(base),
        indexes_(length) {
    for (std::size_t i = 0; i < length; ++i) {
      indexes_[i].reserve(reserve_size);
    }
  }

  const std::uint32_t base_;
  std::vector<std::vector<std::uint32_t>> indexes_;

  DISALLOW_COPY_AND_ASSIGN(ArrayIndex);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_ARRAY_INDEX_HPP_
