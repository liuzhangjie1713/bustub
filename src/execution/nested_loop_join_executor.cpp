//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw NotImplementedException("Only LEFT and INNER join are supported.");
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  left_tuples_.clear();
  left_cursor_ = 0;
  is_matched_ = false;

  // Get all the left tuples
  while (true) {
    Tuple left_tuple;
    RID left_rid;
    const auto status = left_executor_->Next(&left_tuple, &left_rid);
    if (!status) {
      break;
    }
    left_tuples_.emplace_back(std::move(left_tuple));
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  size_t left_tuple_col_cnt = left_executor_->GetOutputSchema().GetColumnCount();
  size_t right_tuple_col_cnt = right_executor_->GetOutputSchema().GetColumnCount();
  size_t tuple_col_cnt = left_tuple_col_cnt + right_tuple_col_cnt;
  Schema join_schema =
      bustub::NestedLoopJoinPlanNode::InferJoinSchema(*(plan_->GetLeftPlan()), *(plan_->GetRightPlan()));
  auto join_expr = plan_->Predicate();

  // outer loop: left table
  while (left_cursor_ < left_tuples_.size()) {
    // Get the next left tuple
    Tuple left_tuple = left_tuples_[left_cursor_];

    // inner loop: compare current left tuple with all right tuples
    while (true) {
      // Get the next right tuple
      Tuple right_tuple;
      RID right_rid;

      const auto status = right_executor_->Next(&right_tuple, &right_rid);
      if (!status) {
        break;
      }

      // Evaluate the join predicate
      auto match = join_expr->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                           right_executor_->GetOutputSchema());

      // If the join predicate is true, then we have found a match
      // 1. if there is a match, then we need to output the joined tuple
      if (!match.IsNull() && match.GetAs<bool>()) {
        // Construct the output tuple
        std::vector<Value> values;
        values.reserve(tuple_col_cnt);

        for (size_t i = 0; i < left_tuple_col_cnt; i++) {
          values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }

        for (size_t i = 0; i < right_tuple_col_cnt; i++) {
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }

        *tuple = {std::move(values), &join_schema};
        is_matched_ = true;
        return true;
      }
    }

    // 2. if join type is LEFT and there is no match, then we need to output the current left tuple with NULL
    if (!is_matched_ && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      values.reserve(tuple_col_cnt);

      for (size_t i = 0; i < left_tuple_col_cnt; i++) {
        values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }

      // fill the right side with NULL values
      for (size_t i = 0; i < right_tuple_col_cnt; i++) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }

      *tuple = {std::move(values), &join_schema};

      // enter the next outer loop
      left_cursor_++;
      right_executor_->Init();
      is_matched_ = false;
      return true;
    }

    // 3.if the join type is INNER and there is no match, then we need to continue comparing the next left tuple
    // enter the next outer loop
    left_cursor_++;
    right_executor_->Init();
    is_matched_ = false;
  }

  return false;
}

}  // namespace bustub
