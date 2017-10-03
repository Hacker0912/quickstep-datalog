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

#ifndef QUICKSTEP_UTILITY_LOCK_FREE_LIST_HPP_
#define QUICKSTEP_UTILITY_LOCK_FREE_LIST_HPP_

#include <atomic>
#include <memory>
#include <utility>

#include "utility/Macros.hpp"

namespace quickstep {

template <typename T>
class LockFreeStack {
 public:
  LockFreeStack() = default;
  ~LockFreeStack() = default;

  void push(const T &t) {
    auto p = std::make_shared<Node>();
    p->t = t;
    p->next = std::atomic_load(&head_);
    while (!std::atomic_compare_exchange_weak(&head_, &(p->next), p)) {
    }
  }

  void push(T &&t) {
    auto p = std::make_shared<Node>();
    p->t = std::move(t);
    p->next = std::atomic_load(&head_);
    while (!std::atomic_compare_exchange_weak(&head_, &(p->next), p)) {
    }
  }

  bool popIfAvailable(T *t) {
    auto p = std::atomic_load(&head_);
    if (!p) {
      return false;
    }

    while (!std::atomic_compare_exchange_weak(&head_, &p, p->next)) {
    }

    *t = std::move(p->t);
    return true;
  }

 private:
  struct Node {
    T t;
    std::shared_ptr<Node> next;
  };

  // atomic<>
  std::shared_ptr<Node> head_;

  DISALLOW_COPY_AND_ASSIGN(LockFreeStack);
};

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_LOCK_FREE_LIST_HPP_
