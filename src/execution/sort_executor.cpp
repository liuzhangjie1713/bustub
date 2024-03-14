#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {}

void SortExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();

  // Initialize the sort
  tuples_.clear();
  cursor_ = 0;

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }

  const auto &order_bys = plan_->GetOrderBy();
  std::sort(tuples_.begin(), tuples_.end(), [&order_bys, this](const Tuple &lhs, const Tuple &rhs) -> CmpBool {
    for (const auto &order_by : order_bys) {
      const auto &order_by_type = order_by.first;
      const auto &order_by_expr = order_by.second;
      const auto &lhs_val = order_by_expr->Evaluate(&lhs, child_executor_->GetOutputSchema());
      const auto &rhs_val = order_by_expr->Evaluate(&rhs, child_executor_->GetOutputSchema());
      if (lhs_val.CompareNotEquals(rhs_val) == CmpBool::CmpTrue) {
        if (order_by_type == OrderByType::DESC) {
          return rhs_val.CompareLessThan(lhs_val);
        }

        if (order_by_type == OrderByType::ASC || order_by_type == OrderByType::DEFAULT) {
          return lhs_val.CompareLessThan(rhs_val);
        }

        throw bustub::NotImplementedException("The order by type is invalid");
      }
    }
    return static_cast<CmpBool>(false);
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ < tuples_.size()) {
    *tuple = tuples_[cursor_];
    cursor_++;
    return true;
  }
  return false;
}

}  // namespace bustub
