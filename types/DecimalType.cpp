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

#include "types/DecimalType.hpp"

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <iomanip>
#include <string>
#include <sstream>

#include "types/DecimalLit.hpp"
#include "types/NullCoercibilityCheckMacro.hpp"
#include "types/Type.hpp"
#include "types/TypeID.hpp"
#include "types/TypeTraits.hpp"
#include "types/TypedValue.hpp"
#include "utility/EqualsAnyConstant.hpp"

#include "glog/logging.h"

namespace quickstep {

template <std::int64_t scale>
bool DecimalType<scale>::isCoercibleFrom(const Type &original_type) const {
  QUICKSTEP_NULL_COERCIBILITY_CHECK();
  return QUICKSTEP_EQUALS_ANY_CONSTANT(
      original_type.getTypeID(),
      kInt, kLong, kFloat, kDouble, kDecimal2, kDecimal4, kDecimal6);
}

template <>
bool DecimalType<2>::isSafelyCoercibleFrom(const Type &original_type) const {
  QUICKSTEP_NULL_COERCIBILITY_CHECK();
  return QUICKSTEP_EQUALS_ANY_CONSTANT(
      original_type.getTypeID(), kInt, kLong, kFloat, kDouble, kDecimal2);
}

template <>
bool DecimalType<4>::isSafelyCoercibleFrom(const Type &original_type) const {
  QUICKSTEP_NULL_COERCIBILITY_CHECK();
  return QUICKSTEP_EQUALS_ANY_CONSTANT(
      original_type.getTypeID(),
      kInt, kLong, kFloat, kDouble, kDecimal2, kDecimal4);
}

template <>
bool DecimalType<6>::isSafelyCoercibleFrom(const Type &original_type) const {
  QUICKSTEP_NULL_COERCIBILITY_CHECK();
  return QUICKSTEP_EQUALS_ANY_CONSTANT(
      original_type.getTypeID(),
      kInt, kLong, kFloat, kDouble, kDecimal2, kDecimal4, kDecimal6);
}

template <std::int64_t scale>
TypedValue DecimalType<scale>::coerceValue(const TypedValue &original_value,
                                           const Type &original_type) const {
  DCHECK(isCoercibleFrom(original_type))
      << "Can't coerce value of Type " << original_type.getName()
      << " to Type " << getName();

  if (original_value.isNull()) {
    return makeNullValue();
  }

  switch (original_type.getTypeID()) {
    case kInt:
      return TypedValue(DecimalLit<scale>(original_value.getLiteral<int>()));
    case kLong:
      return TypedValue(DecimalLit<scale>(original_value.getLiteral<std::int64_t>()));
    case kFloat:
      return TypedValue(DecimalLit<scale>(original_value.getLiteral<float>()));
    case kDouble:
      return TypedValue(DecimalLit<scale>(original_value.getLiteral<double>()));
    case kDecimal2:
      return TypedValue(static_cast<DecimalLit<scale>>(
          original_value.getLiteral<DecimalLit<2>>()));
    case kDecimal4:
      return TypedValue(static_cast<DecimalLit<scale>>(
          original_value.getLiteral<DecimalLit<4>>()));
    case kDecimal6:
      return TypedValue(static_cast<DecimalLit<scale>>(
          original_value.getLiteral<DecimalLit<6>>()));
    default:
      LOG(FATAL) << "Attempted to coerce Type " << original_type.getName()
                 << " (not recognized as a numeric Type) to " << getName();
  }
}

template <std::int64_t scale>
std::string DecimalType<scale>::printValueToString(const TypedValue &value) const {
  DCHECK(!value.isNull());

  DecimalLit<scale> decimal = value.getLiteral<DecimalLit<scale>>();
  std::stringstream ss;
  ss << decimal.getIntegerPart() << "."
     << std::setfill('0') << std::setw(DecimalLit<scale>::kScaleWidth)
     << decimal.getFractionalPart();
  return ss.str();
}

template <std::int64_t scale>
void DecimalType<scale>::printValueToFile(const TypedValue &value,
                                          FILE *file,
                                          const int padding) const {
  DCHECK(!value.isNull());

  DecimalLit<scale> decimal = value.getLiteral<DecimalLit<scale>>();
  const std::int64_t integer_part = decimal.getIntegerPart();
  const bool is_negative_zero = (integer_part == 0 && decimal.isNegative());

  std::fprintf(file, "%*s.%0*" PRIu64,
               static_cast<int>(padding - DecimalLit<scale>::kScaleWidth - 1),
               ((is_negative_zero ? "-" : "") + std::to_string(integer_part)).c_str(),
               static_cast<int>(DecimalLit<scale>::kScaleWidth),
               decimal.getFractionalPart());
}

template <std::int64_t scale>
bool DecimalType<scale>::parseValueFromString(const std::string &value_string,
                                              TypedValue *value) const {
  double parsed_double;
  int read_chars;

  int matched = std::sscanf(value_string.c_str(),
                            "%lf%n",
                            &parsed_double,
                            &read_chars);

  if (matched != 1 || read_chars != static_cast<int>(value_string.length()))  {
    return false;
  }

  *value = TypedValue(DecimalLit<scale>(parsed_double));
  return true;
}

}  // namespace quickstep
