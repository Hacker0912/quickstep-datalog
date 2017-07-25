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

#ifndef QUICKSTEP_UTILITY_META_MATH_HPP_
#define QUICKSTEP_UTILITY_META_MATH_HPP_

namespace quickstep {
namespace meta {

template <typename T>
inline constexpr T Sqr(T arg) {
  return arg * arg;
}

template <typename T>
inline constexpr T Pow(T base, T exponent) {
  return  exponent == 0 ?
      1 : Sqr(Pow(base, exponent >> 1)) * ((exponent & 1) ? base : 1);
}

}  // namespace meta
}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_META_MATH_HPP_
