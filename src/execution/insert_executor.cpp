//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      child_executor_(std::move(child_executor)),
      table_info_{exec_ctx->GetCatalog()->GetTable(plan_->TableOid())} {}

void InsertExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();

  insert_done_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // If there is no more tuples to insert, return false
  if (insert_done_) {
    return false;
  }

  // The number of rows inserted into the table
  int num_inserted = 0;

  while (true) {
    // Get the next tuple
    Tuple child_tuple{};
    RID child_rid;
    const auto status = child_executor_->Next(&child_tuple, &child_rid);

    if (!status) {
      insert_done_ = true;
      break;
    }

    // Insert the tuple into the table
    auto tuple_meta = TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    auto result = table_info_->table_->InsertTuple(tuple_meta, child_tuple, exec_ctx_->GetLockManager(),
                                                   exec_ctx_->GetTransaction(), plan_->TableOid());
    BUSTUB_ASSERT(result.has_value(), "Insert failed");
    RID tuple_rid = result.value();

    // Update the index
    auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto &index_info : table_indexes) {
      auto key =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, tuple_rid, exec_ctx_->GetTransaction());
    }

    num_inserted++;
  }

  std::vector<Value> values{{INTEGER, num_inserted}};
  *tuple = Tuple(values, &(plan_->OutputSchema()));
  return true;
}

}  // namespace bustub
