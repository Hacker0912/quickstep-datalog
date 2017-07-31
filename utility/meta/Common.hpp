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

#ifndef QUICKSTEP_UTILITY_META_COMMON_HPP_
#define QUICKSTEP_UTILITY_META_COMMON_HPP_

#include <cstddef>
#include <string>

namespace quickstep {
namespace meta {

template <typename T1, typename T2>
struct Pair {
  using first = T1;
  using second = T2;
};

template <typename T, T ...s>
struct Sequence {
  using value_type = T;

  template <template <typename ...> class Host>
  using bind_to = Host<std::integral_constant<T, s>...>;

  template <template <T ...> class Host>
  using bind_values_to = Host<s...>;

  template <typename U>
  using cast_to = Sequence<U, static_cast<U>(s)...>;

  template <typename CollectionT>
  inline static CollectionT Instantiate() {
    return { s... };
  }
};

template <std::size_t ...s>
using IntegerSequence = Sequence<std::size_t, s...>;

template <std::size_t l, std::size_t r, std::size_t ...s>
struct MakeSequence_0 : MakeSequence_0<l, r-1, r-1, s...> {};
template <std::size_t l, std::size_t ...s>
struct MakeSequence_0<l, l, s...> {
  using type = IntegerSequence<s...>;
};
template <std::size_t n>
struct MakeSequence : MakeSequence_0<0, n> {};
template <std::size_t l, std::size_t r>
struct MakeRange : MakeSequence_0<l, r> {};

template <typename ...> struct Conjunction : std::true_type {};
template <typename B> struct Conjunction<B> : B {};
template <typename B, typename ...Bs>
struct Conjunction<B, Bs...>
    : std::conditional_t<B::value, Conjunction<Bs...>, B> {};

template <typename ...> struct Disjunction : std::false_type {};
template <typename B> struct Disjunction<B> : B {};
template <typename B, typename ...Bs>
struct Disjunction<B, Bs...>
    : std::conditional_t<B::value, B, Disjunction<Bs...>> {};


template <typename check, typename ...cases>
struct EqualsAny {
  static constexpr bool value =
     Disjunction<std::is_same<check, cases>...>::value;
};

template <typename T, T check, T ...cases>
struct EqualsAnyValue {
  static constexpr bool value =
     Disjunction<std::is_same<std::integral_constant<T, check>,
                              std::integral_constant<T, cases>>...>::value;
};


template <typename T, typename Enable = void>
struct IsTrait {
  static constexpr bool value = false;
};

template <typename T>
struct IsTrait<T, std::enable_if_t<
    std::is_same<typename T::type, typename T::type>::value>> {
  static constexpr bool value = true;
};

template <typename T, template <typename> class Op, typename Enable = void>
struct IsWellFormed {
  static constexpr bool value = false;
};

template <typename T, template <typename> class Op>
struct IsWellFormed<T, Op, std::enable_if_t<std::is_same<Op<T>, Op<T>>::value>> {
  static constexpr bool value = true;
};


template <typename LeftT, typename RightT>
struct PairSelectorLeft {
  typedef LeftT type;
};

template <typename LeftT, typename RightT>
struct PairSelectorRight {
  typedef RightT type;
};


template <char ...c>
struct StringLiteral {
  inline static std::string ToString() {
    return std::string({c...});
  }
};

template <template <typename ...> class Op>
class TraitWrapper {
 private:
  template <typename ...ArgTypes>
  struct Implemenation {
    using type = Op<ArgTypes...>;
  };

 public:
  template <typename ...ArgTypes>
  using type = Implemenation<ArgTypes...>;
};

template <template <typename ...> class Op>
struct TraitUnwrapper {
  template <typename ...ArgTypes>
  using type = typename Op<ArgTypes...>::type;
};

template <typename Lhs, typename Rhs>
struct Add {
  using type = std::integral_constant<
      typename Lhs::value_type, Lhs::value + Rhs::value>;
};

}  // namespace meta
}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_META_COMMON_HPP_
