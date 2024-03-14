//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  aht_.Clear();

  // Insert all the tuples from the child executor into the hash table
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  if (aht_.Begin() == aht_.End()) {
    is_empty_table_ = true;
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_empty_table_) {
    if (!plan_->group_bys_.empty()) {
      return false;
    }

    *tuple = Tuple(aht_.GenerateInitialAggregateValue().aggregates_, &plan_->OutputSchema());
    is_empty_table_ = false;
    return true;
  }

  while (aht_iterator_ != aht_.End()) {
    const auto &aggregate_key = aht_iterator_.Key();
    const auto &aggregate_value = aht_iterator_.Val();

    std::vector<Value> values;
    values.reserve(plan_->group_bys_.size() + plan_->aggregates_.size());
    if (!plan_->group_bys_.empty()) {
      for (const auto &key : aggregate_key.group_bys_) {
        values.emplace_back(key);
      }
    }
    for (const auto &val : aggregate_value.aggregates_) {
      values.emplace_back(val);
    }
    *tuple = Tuple(values, &plan_->OutputSchema());

    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
