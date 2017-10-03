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

#include "utility/LockFreeStack.hpp"

#include <thread>  // NOLINT(build/c++11)

#include "gtest/gtest.h"

namespace quickstep {

namespace {

constexpr int kValue = 42;

}  // namespace

TEST(LockFreeStackTest, IntPushPopTest) {
  LockFreeStack<int> int_stack;

  int value = 0;
  EXPECT_FALSE(int_stack.popIfAvailable(&value));
  EXPECT_EQ(0, value);

  int_stack.push(2017);
  int_stack.push(kValue);
  EXPECT_TRUE(int_stack.popIfAvailable(&value));
  EXPECT_EQ(kValue, value);
}

TEST(LockFreeStackTest, ConsumerProducerTest) {
  LockFreeStack<int> int_stack;

  std::thread consumer([&int_stack] {
    int value;
    while (!int_stack.popIfAvailable(&value)) {
    }

    EXPECT_EQ(kValue, value);
  });

  std::thread producer([&int_stack] {
    int_stack.push(kValue);
  });

  consumer.join();
  producer.join();
}

}  // namespace quickstep
