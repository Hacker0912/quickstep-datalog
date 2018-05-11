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

#ifndef QUICKSTEP_UTILITY_BIT_MATRIX_HPP_
#define QUICKSTEP_UTILITY_BIT_MATRIX_HPP_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "utility/BarrieredReadWriteConcurrentBitVector.hpp"
#include "utility/BitMatrix.pb.h"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

class BitMatrix {
 public:
  static BitMatrix* ReconstructFromProto(const serialization::BitMatrix &proto) {
    DCHECK(proto.IsInitialized());
    return new BitMatrix(proto.size());
  }

  std::size_t size() const {
    return size_;
  }

  void initialize(const std::uint32_t x) {
    matrix_[x] = std::make_unique<BarrieredReadWriteConcurrentBitVector>(size_);
    DCHECK(matrix_[x]);
  }

  inline void set(const std::uint32_t x, const std::uint32_t y) {
    DCHECK_LT(x, size_);
    DCHECK_LT(y, size_);
    DCHECK(matrix_[x]);
    matrix_[x]->setBit(y);
  }

  inline bool get(const std::uint32_t x, const std::uint32_t y) const {
    DCHECK_LT(x, size_);
    DCHECK_LT(y, size_);
    DCHECK(matrix_[x]);
    return matrix_[x]->getBit(y);
  }

  const BarrieredReadWriteConcurrentBitVector& get(const std::uint32_t x) const {
    DCHECK_LT(x, size_);
    DCHECK(matrix_[x]);
    return *matrix_[x];
  }

 private:
  explicit BitMatrix(const std::size_t size)
      : size_(size),
        matrix_(size_) {
  }

  // TODO(zuyu): add base.
  const std::size_t size_;
  std::vector<std::unique_ptr<BarrieredReadWriteConcurrentBitVector>> matrix_;

  DISALLOW_COPY_AND_ASSIGN(BitMatrix);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_BIT_MATRIX_HPP_
