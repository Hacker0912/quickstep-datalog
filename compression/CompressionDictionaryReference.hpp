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

#ifndef QUICKSTEP_COMPRESSION_COMPRESSION_DICTIONARY_REFERENCE_HPP_
#define QUICKSTEP_COMPRESSION_COMPRESSION_DICTIONARY_REFERENCE_HPP_

#include <cstddef>
#include <cstdint>

#include "utility/Macros.hpp"

namespace quickstep {

class Type;

/** \addtogroup Compression
 *  @{
 */

class CompressionDictionaryReference {
 public:
  CompressionDictionaryReference(const Type &value_type,
                                 const std::size_t code_size,
                                 const std::uint32_t null_code,
                                 const void *codes,
                                 const void *values)
      : value_type_(value_type),
        code_size_(code_size),
        null_code_(null_code),
        codes_(codes),
        values_(values) {}

  inline const Type& getValueType() const {
    return value_type_;
  }

  inline std::size_t getCodeSize() const {
    return code_size_;
  }

  inline std::uint32_t getNullCode() const {
    return null_code_;
  }

  inline const void* getCodes() const {
    return codes_;
  }

  inline const void* getValues() const {
    return values_;
  }

 private:
  const Type &value_type_;
  const std::size_t code_size_;
  const std::uint32_t null_code_;
  const void *codes_;
  const void *values_;

  DISALLOW_COPY_AND_ASSIGN(CompressionDictionaryReference);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_COMPRESSION_COMPRESSION_DICTIONARY_REFERENCE_HPP_
