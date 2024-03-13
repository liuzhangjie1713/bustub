//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/plans/hash_join_plan.h"
#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

/**
 * A simplified hash table that has all the necessary functionality for hash join.
 */
class SimpleHashJoinHashTable {
 public:
  /**
   * Construct a new SimpleHashJoinHashTable instance.
   * @param right_key_expressions
   */
  SimpleHashJoinHashTable() = default;

  void Insert(const JoinKey &join_key, const Tuple &tuple) {
    if (ht_.count(join_key) == 0) {
      JoinValue join_val;
      join_val.tuples_.push_back(tuple);
      ht_.insert({join_key, join_val});
    } else {
      ht_[join_key].tuples_.push_back(tuple);
    }
  }

  auto Count(const JoinKey &join_key) -> size_t { return ht_.count(join_key); }

  auto At(const JoinKey &join_key) -> JoinValue & { return ht_.at(join_key); }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

 private:
  /** The hash table is just a map from join keys to join values. */
  std::unordered_map<JoinKey, JoinValue> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as an RightJoinKey */
  auto MakeRightJoinKey(const Tuple *tuple) -> JoinKey {
    std::vector<Value> join_values;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      join_values.emplace_back(expr->Evaluate(tuple, right_executor_->GetOutputSchema()));
    }
    return {join_values};
  }

  /** @return The tuple as an LeftJoinKey*/
  auto MakeLeftJoinKey(const Tuple *tuple) -> JoinKey {
    std::vector<Value> join_values;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      join_values.emplace_back(expr->Evaluate(tuple, left_executor_->GetOutputSchema()));
    }
    return {join_values};
  }

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** The child executor that produces tuple for the left side of join. */
  std::unique_ptr<AbstractExecutor> left_executor_;

  /** The child executor that produces tuple for the right side of join. */
  std::unique_ptr<AbstractExecutor> right_executor_;

  /** Simple join hash table */
  SimpleHashJoinHashTable jht_;

  /** match tuples that join by the current left tuple and right tuples in hash table bucket */
  std::vector<Tuple> match_tuples_;

  /** The cursor of the match tuples. */
  size_t match_cursor_ = 0;
};
}  // namespace bustub
