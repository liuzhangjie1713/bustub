//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      jht_(SimpleHashJoinHashTable()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  jht_.Clear();
  match_tuples_.clear();
  match_cursor_ = 0;

  // Build the hash table on the right table
  // hashtable::key : The values in the tuple that match the join expression
  // hashtable::value : The bucket of tuples that match the join key
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    jht_.Insert(MakeRightJoinKey(&tuple), tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  size_t left_tuple_col_cnt = left_executor_->GetOutputSchema().GetColumnCount();
  size_t right_tuple_col_cnt = right_executor_->GetOutputSchema().GetColumnCount();
  size_t tuple_col_cnt = left_tuple_col_cnt + right_tuple_col_cnt;
  Schema join_schema =
      bustub::NestedLoopJoinPlanNode::InferJoinSchema(*(plan_->GetLeftPlan()), *(plan_->GetRightPlan()));
  // outer loop:
  while (true) {
    // If there are still matching tuples in bucket from the right table, return the next one
    if (match_cursor_ < match_tuples_.size()) {
      // Get the next matching tuple from the right table
      *tuple = match_tuples_[match_cursor_];
      match_cursor_++;
      return true;
    }

    // Get the next left tuple
    Tuple left_tuple;
    RID left_rid;

    const auto status = left_executor_->Next(&left_tuple, &left_rid);
    if (!status) {
      break;
    }

    // Get the join key from the left tuple
    auto left_join_key = MakeLeftJoinKey(&left_tuple);

    // No match
    if (jht_.Count(left_join_key) == 0) {
      // No match and left join, fill the right tuple with NULL values
      if (plan_->GetJoinType() == JoinType::LEFT) {
        // Construct the output tuple
        std::vector<Value> values;
        values.reserve(tuple_col_cnt);

        for (size_t i = 0; i < left_tuple_col_cnt; i++) {
          values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }

        for (size_t i = 0; i < right_tuple_col_cnt; i++) {
          values.emplace_back(
              ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }

        *tuple = {std::move(values), &join_schema};
        return true;
      }

      // No match and inner join, continue
      continue;
    }

    // New left tuple, get the matching tuples from the right table
    match_tuples_.clear();
    match_cursor_ = 0;
    for (auto &right_tuple : jht_.At(left_join_key).tuples_) {
      std::vector<Value> values;
      values.reserve(tuple_col_cnt);

      for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
      }

      match_tuples_.emplace_back(std::move(values), &join_schema);
    }

    *tuple = match_tuples_[match_cursor_];
    match_cursor_++;
    return true;
  }

  return false;
}

}  // namespace bustub
