/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 *   limitations under the License.
 **/

#ifndef QUICKSTEP_TYPES_DECIMAL_LIT_HPP_
#define QUICKSTEP_TYPES_DECIMAL_LIT_HPP_

#include <cstdint>
#include <cstdlib>
#include <type_traits>

#include "utility/meta/Common.hpp"
#include "utility/meta/Math.hpp"

namespace quickstep {

/** \addtogroup Type
 *  @{
 */

/**
 * @brief A literal representing fixed-precision decimal.
 **/
template <std::int64_t scale>
struct DecimalLit {
  typedef std::int64_t cpptype;

  cpptype data;

  DecimalLit() = default;

  explicit DecimalLit(const int value)
    : data(static_cast<cpptype>(value * kScaleBase)) {
  }

  explicit DecimalLit(const std::int64_t value)
    : data(static_cast<cpptype>(value * kScaleBase)) {
  }

  explicit DecimalLit(const float value)
    : data(static_cast<cpptype>(value * kScaleBase)) {
  }

  explicit DecimalLit(const double value)
    : data(static_cast<cpptype>(value * kScaleBase)) {
  }

  inline explicit operator int() const {
    return static_cast<int>(getIntegerPart());
  }

  inline explicit operator std::int64_t() const {
    return static_cast<std::int64_t>(getIntegerPart());
  }

  inline explicit operator float() const {
    return static_cast<float>(data) / kScaleBase;
  }

  inline explicit operator double() const {
    return static_cast<double>(data) / kScaleBase;
  }

  template <std::int64_t cast_scale>
  inline explicit operator DecimalLit<cast_scale>() const {
    DecimalLit<cast_scale> result;
    if (scale > cast_scale) {
      result.data = data / meta::Pow(10ll, scale - cast_scale);
    } else if (scale < cast_scale) {
      result.data = data * meta::Pow(10ll, cast_scale - scale);
    } else {
      result.data = data;
    }
    return result;
  }

  static inline DecimalLit FromData(const int64_t data) {
    DecimalLit result;
    result.data = data;
    return result;
  }

  /**
   * @brief Number of decimals after point.
   **/
  static constexpr std::int64_t kScaleWidth = scale;

  /**
   * @brief Normalization factor between Decimal type and floating point types.
   *        It is always equal to pow(10, kScaleWidth).
   **/
  static constexpr std::int64_t kScaleBase = meta::Pow(10ll, kScaleWidth);

  /**
   * @brief Get the fractional part of the Decimal literal.
   *
   * @return Fractional part of the Decimal number as unsigned.
   **/
  inline std::uint64_t getFractionalPart() const {
    return std::llabs(data) % kScaleBase;
  }

  /**
   * @brief Get the integer part of the Decimal literal.
   *
   * @return Integer part of the Decimal number as signed.
   **/
  inline std::int64_t getIntegerPart() const {
    return data / kScaleBase;
  }

  /**
   * @brief Operator overloading for "less than".
   *
   * @param other Other DecimalLit to be compared.
   * @return True if this is less than other,
   *         false otherwise.
   **/
  inline bool operator<(const DecimalLit &other) const {
    return data < other.data;
  }

  /**
   * @brief Operator overloading for "greater than".
   *
   * @param other Other DecimalLit to be compared.
   * @return True if this is greater than other,
   *         false otherwise.
   **/
  inline bool operator>(const DecimalLit &other) const {
    return data > other.data;
  }

  /**
   * @brief Operator overloading for "less than or equal to".
   *
   * @param other Other DecimalLit to be compared.
   * @return True if this is less than or equal to other,
   *         false otherwise.
   **/
  inline bool operator<=(const DecimalLit &other) const {
    return data <= other.data;
  }

  /**
   * @brief Operator overloading for "greater than or equal to".
   *
   * @param other Other DecimalLit to be compared.
   * @return True if this is greater than or equal to other,
   *         false otherwise.
   **/
  inline bool operator>=(const DecimalLit &other) const {
    return data >= other.data;
  }

  /**
   * @brief Operator overloading for "equal to".
   *
   * @param other Other DecimalLit to be compared.
   * @return True if this is equal to other,
   *         false otherwise.
   **/
  inline bool operator==(const DecimalLit &other) const {
    return data == other.data;
  }

  /**
   * @brief Operator overloading for "not equal to".
   *
   * @param other Other DecimalLit to be compared.
   * @return True if this is not equal to other,
   *         false otherwise.
   **/
  inline bool operator!=(const DecimalLit &other) const {
    return data != other.data;
  }

  /**
   * @brief Operator overloading for "negate".
   *
   * @return Negative of this.
   **/
  inline DecimalLit operator-() const {
    DecimalLit result;
    result.data = -result.data;
    return result;
  }

  /**
   * @brief Operator overloading for "plus".
   *
   * @param other Other DecimalLit to be added.
   * @return Sum of this and other.
   **/
  inline DecimalLit operator+(const DecimalLit &other) const {
    DecimalLit result;
    result.data = data + other.data;
    return result;
  }

  /**
   * @brief Operator overloading for "subtract".
   *
   * @param other Other DecimalLit to be subtract.
   * @return Subtraction of other from this.
   **/
  inline DecimalLit operator-(const DecimalLit &other) const {
    DecimalLit result;
    result.data = data - other.data;
    return result;
  }

  template <typename T>
  inline typename std::enable_if<
      meta::EqualsAny<T, int, int64_t>::value, DecimalLit>::type
          operator*(const T &other) const {
    DecimalLit result;
    result.data = data * other;
    return result;
  }

  /**
   * @brief Operator overloading for "multiply".
   *
   * @param other Other DecimalLit to be multiplied.
   * @return Multiplication of this and other.
   **/
  template <std::int64_t other_scale>
  inline DecimalLit<scale + other_scale> operator*(
      const DecimalLit<other_scale> &other) const {
    DecimalLit<scale + other_scale> result;
    result.data = data * other.data;
    return result;
  }

  /**
   * @brief Operator overloading for "inplace add".
   *
   * @param other DecimalLit to be added.
   * @return Reference to this.
   **/
  inline DecimalLit& operator+=(const DecimalLit &other) {
    data += other.data;
    return *this;
  }

  /**
   * @brief Operator overloading for "inplace subtract".
   *
   * @param other DecimalLit to be subtracted.
   * @return Reference to this.
   **/
  inline DecimalLit& operator-=(const DecimalLit &other) {
    data -= other.data;
    return *this;
  }
};

template <std::int64_t scale>
inline DecimalLit<scale> operator*(const int &lhs,
                                   const DecimalLit<scale> &rhs) {
  return rhs * lhs;
}
template <std::int64_t scale>
inline DecimalLit<scale> operator*(const std::int64_t &lhs,
                                   const DecimalLit<scale> &rhs) {
  return rhs * lhs;
}


//** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_DECIMAL_LIT_HPP_
