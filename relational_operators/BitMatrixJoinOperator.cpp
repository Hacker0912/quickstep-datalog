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

#include "relational_operators/BitMatrixJoinOperator.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <stack>
#include <utility>

#include "cli/Flags.hpp"
#include "query_execution/QueryContext.hpp"
#include "query_execution/WorkOrderProtosContainer.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "relational_operators/WorkOrder.pb.h"
#include "utility/ArrayIndex.hpp"
#include "utility/BitMatrix.hpp"
#include "utility/Range.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace quickstep {

DEFINE_int32(bit_matrix_join_batch_threshold, 1000,
             "The threshold for batch size in BitMatrixJoin.");

namespace {

const bool kIsLastBatch = true;

struct BitMatrixJoinBatch {
  BitMatrixJoinBatch(const BitMatrixJoinOperator::Pairs *p,
                     const bool last)
      : pairs(p), last_batch(last) {}

  const BitMatrixJoinOperator::Pairs *pairs;
  const bool last_batch;
};

void send(const std::size_t operator_index,
          const BitMatrixJoinBatch &batch,
          const tmb::client_id cid,
          const tmb::client_id scheduler_client_id,
          WorkOrder *wo,
          MessageBus *bus) {
  void *pointer = std::malloc(sizeof(batch));
  std::memcpy(pointer, &batch, sizeof(batch));
  WorkOrder::FeedbackMessage msg(
      BitMatrixJoinOperator::kNextBatchMessage,
      wo->getQueryID(),
      operator_index,
      pointer,
      sizeof(batch));
  wo->SendFeedbackMessage(
      bus, cid, scheduler_client_id, msg);
}

}  // namespace

bool BitMatrixJoinOperator::getAllWorkOrders(
    WorkOrdersContainer *container,
    QueryContext *query_context,
    StorageManager *storage_manager,
    const tmb::client_id scheduler_client_id,
    tmb::MessageBus *bus) {
  BitMatrix *matrix = query_context->getBitMatrixMutable(bit_matrix_index_);
  const std::size_t range = matrix->size();
  if (range == 0) {
    return true;
  }

  const ArrayIndex &array_index = query_context->getArrayIndex(array_index_);

  if (started_) {
    count_ += next_batches_.size();
    for (std::size_t i = 0; i < next_batches_.size(); ++i) {
      container->addNormalWorkOrder(
          new BitMatrixJoinWorkOrder(query_id_, array_index, next_batches_[i], matrix, op_index_, scheduler_client_id, bus),
          op_index_);
    }
    next_batches_.clear();

    return (count_ == 0);
  }

  started_ = true;

  const RangeSplitter splitter =
      RangeSplitter::CreateWithNumPartitions(array_index.base(), range, FLAGS_num_workers);

  count_ += splitter.getNumPartitions();
  for (std::size_t i = 0; i < splitter.getNumPartitions(); ++i) {
    container->addNormalWorkOrder(
        new BitMatrixJoinInitWorkOrder(query_id_, splitter.getPartition(i), array_index, matrix, op_index_, scheduler_client_id, bus),
        op_index_);
  }

  return false;
}

bool BitMatrixJoinOperator::getAllWorkOrderProtos(
    WorkOrderProtosContainer *container) {
  LOG(FATAL) << "Not supported";
}

void BitMatrixJoinOperator::receiveFeedbackMessage(const WorkOrder::FeedbackMessage &msg) {
  DCHECK(BitMatrixJoinOperator::kNextBatchMessage == msg.type());

  const auto batch = *static_cast<const BitMatrixJoinBatch*>(msg.payload());
  if (batch.last_batch) {
    --count_;
  }

  const auto *pairs = batch.pairs;
  if (pairs->empty()) {
    delete pairs;
    return;
  }

  next_batches_.push_back(pairs);
}

void BitMatrixJoinInitWorkOrder::execute() {
  auto *next_round = new BitMatrixJoinOperator::Pairs();
  next_round->reserve(FLAGS_bit_matrix_join_batch_threshold);
  const auto cid = ClientIDMap::Instance()->getValue();

  for (std::size_t i = range_.begin(); i < range_.end(); ++i) {
    const auto &entries = array_index_.get(i);
    for (const auto x : entries) {
      for (const auto y : entries) {
        if (x != y && !matrix_->get(x, y)) {
          matrix_->set(x, y);
          next_round->emplace_back(x, y);

          if (next_round->size() == FLAGS_bit_matrix_join_batch_threshold) {
            send(operator_index_, BitMatrixJoinBatch(next_round, !kIsLastBatch), cid,
                 scheduler_client_id_, this, bus_);
            next_round = new BitMatrixJoinOperator::Pairs();
            next_round->reserve(FLAGS_bit_matrix_join_batch_threshold);
          }
        }
      }
    }
  }

  while (!next_round->empty()) {
    const auto &last = next_round->back();
    const int x = last.first;
    const int y = last.second;

    next_round->pop_back();

    const auto &x_children = array_index_.get(x);
    const auto &y_children = array_index_.get(y);

    if (x_children.empty() || y_children.empty()) {
      continue;
    }

    for (const auto new_x : x_children) {
      for (const auto new_y : y_children) {
        if (!matrix_->get(new_x, new_y)) {
          matrix_->set(new_x, new_y);
          next_round->emplace_back(new_x, new_y);

          if (next_round->size() == FLAGS_bit_matrix_join_batch_threshold) {
            send(operator_index_, BitMatrixJoinBatch(next_round, !kIsLastBatch), cid,
                 scheduler_client_id_, this, bus_);
            next_round = new BitMatrixJoinOperator::Pairs();
            next_round->reserve(FLAGS_bit_matrix_join_batch_threshold);
          }
        }
      }
    }
  }

  // Send completion message to operator.
  send(operator_index_, BitMatrixJoinBatch(next_round, kIsLastBatch), cid,
       scheduler_client_id_, this, bus_);
}

void BitMatrixJoinWorkOrder::execute() {
  auto *next_round = new BitMatrixJoinOperator::Pairs();
  next_round->reserve(FLAGS_bit_matrix_join_batch_threshold);
  const auto cid = ClientIDMap::Instance()->getValue();

  for (const auto &pair : *pairs_) {
    const int x = pair.first;
    const int y = pair.second;

    const auto &x_children = array_index_.get(x);
    const auto &y_children = array_index_.get(y);

    if (x_children.empty() || y_children.empty()) {
      continue;
    }

    for (const auto new_x : x_children) {
      for (const auto new_y : y_children) {
        if (!matrix_->get(new_x, new_y)) {
          matrix_->set(new_x, new_y);
          next_round->emplace_back(new_x, new_y);

          if (next_round->size() == FLAGS_bit_matrix_join_batch_threshold) {
            send(operator_index_, BitMatrixJoinBatch(next_round, !kIsLastBatch), cid,
                 scheduler_client_id_, this, bus_);
            next_round = new BitMatrixJoinOperator::Pairs();
            next_round->reserve(FLAGS_bit_matrix_join_batch_threshold);
          }
        }
      }
    }
  }

  while (!next_round->empty()) {
    const auto &last = next_round->back();
    const int x = last.first;
    const int y = last.second;

    next_round->pop_back();

    const auto &x_children = array_index_.get(x);
    const auto &y_children = array_index_.get(y);

    if (x_children.empty() || y_children.empty()) {
      continue;
    }

    for (const auto new_x : x_children) {
      for (const auto new_y : y_children) {
        if (!matrix_->get(new_x, new_y)) {
          matrix_->set(new_x, new_y);
          next_round->emplace_back(new_x, new_y);

          if (next_round->size() == FLAGS_bit_matrix_join_batch_threshold) {
            send(operator_index_, BitMatrixJoinBatch(next_round, !kIsLastBatch), cid,
                 scheduler_client_id_, this, bus_);
            next_round = new BitMatrixJoinOperator::Pairs();
            next_round->reserve(FLAGS_bit_matrix_join_batch_threshold);
          }
        }
      }
    }
  }

  // Send completion message to operator.
  send(operator_index_, BitMatrixJoinBatch(next_round, kIsLastBatch), cid,
       scheduler_client_id_, this, bus_);
}

}  // namespace quickstep
