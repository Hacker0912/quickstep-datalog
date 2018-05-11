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

#include <memory>

#include "utility/BitMatrix.hpp"
#include "utility/BitMatrix.pb.h"

#include "benchmark/benchmark.h"

using quickstep::BitMatrix;

static void BM_BitMatrixInit(benchmark::State &state) {  // NOLINT(runtime/references)
  quickstep::serialization::BitMatrix proto;
  proto.set_size(20000);
  std::unique_ptr<BitMatrix> bit_matrix(BitMatrix::ReconstructFromProto(proto));
  for (auto _ : state) {
    for (int i = 0; i < state.range(0); ++i) {
      bit_matrix->initialize(i);
    }
  }
}
BENCHMARK(BM_BitMatrixInit)
    ->Arg(1)->Arg(10)->Arg(100)->Arg(500)
    ->Arg(1000)->Arg(2000)->Arg(4000)->Arg(8000)
    ->Arg(10000)->Arg(20000);

int main(int argc, char **argv) {
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
