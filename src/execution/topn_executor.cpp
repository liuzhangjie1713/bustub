#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  cursor_ = 0;
  top_entries_.clear();

  // Initialize the heap
  const auto &order_bys = plan_->GetOrderBy();
  auto cmp = [&order_bys, this](const Tuple &lhs, const Tuple &rhs) -> bool {
    for (const auto &order_by : order_bys) {
      const auto &order_by_type = order_by.first;
      const auto &order_by_expr = order_by.second;
      const auto &lhs_val = order_by_expr->Evaluate(&lhs, child_executor_->GetOutputSchema());
      const auto &rhs_val = order_by_expr->Evaluate(&rhs, child_executor_->GetOutputSchema());
      if (lhs_val.CompareNotEquals(rhs_val) == CmpBool::CmpTrue) {
        if (order_by_type == OrderByType::DESC) {
          // min heap
          // return static_cast<bool>(lhs_val.CompareLessThan(rhs_val));
          return static_cast<bool>(rhs_val.CompareLessThan(lhs_val));
        }

        if (order_by_type == OrderByType::ASC || order_by_type == OrderByType::DEFAULT) {
          // max heap
          // return static_cast<bool>(rhs_val.CompareLessThan(lhs_val));
          return static_cast<bool>(lhs_val.CompareLessThan(rhs_val));
        }

        throw NotImplementedException("The order by type is invalid");
      }
    }
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> heap(cmp);

  // retrieve all tuples from child executor into heap
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    heap.push(tuple);
    if (heap.size() > plan_->GetN()) {
      heap.pop();
    }
  }

  // retrieve tuples from heap into top_entries_
  top_entries_size_ = 0;
  while (!heap.empty()) {
    top_entries_.insert(top_entries_.begin(), heap.top());
    heap.pop();
    top_entries_size_++;
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (cursor_ < top_entries_.size()) {
    *tuple = top_entries_[cursor_];
    cursor_++;
    top_entries_size_--;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_size_; };

}  // namespace bustub
