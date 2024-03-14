//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  update_done_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // If there is no more tuples to update, return false
  if (update_done_) {
    return false;
  }

  // The number of rows updated
  int num_updated = 0;

  while (true) {
    // Get the next tuple
    Tuple child_tuple{};
    RID child_rid;
    const auto status = child_executor_->Next(&child_tuple, &child_rid);

    if (!status) {
      update_done_ = true;
      break;
    }

    // Update the tuple in the table
    // 1.delete the old tuple
    table_info_->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, child_rid);
    // 2.insert the new tuple
    std::vector<Value> values;
    values.reserve(plan_->target_expressions_.size());
    for (const auto &expression : plan_->target_expressions_) {
      values.emplace_back(expression->Evaluate(&child_tuple, table_info_->schema_));
    }
    Tuple new_tuple(values, &table_info_->schema_);
    auto result =
        table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple,
                                         exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), plan_->TableOid());
    BUSTUB_ASSERT(result.has_value(), "Update failed");
    RID new_tuple_rid = result.value();

    // Update the index
    auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto &index_info : table_indexes) {
      auto delete_key =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(delete_key, child_rid, exec_ctx_->GetTransaction());
      auto insert_key =
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(insert_key, new_tuple_rid, exec_ctx_->GetTransaction());
    }

    num_updated++;
  }

  std::vector<Value> values{{INTEGER, num_updated}};
  *tuple = Tuple(values, &(plan_->OutputSchema()));
  return true;
}

}  // namespace bustub
