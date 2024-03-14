//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      child_executor_(std::move(child_executor)),
      table_info_{exec_ctx->GetCatalog()->GetTable(plan_->TableOid())} {}

void DeleteExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();

  delete_done_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // If there is no more tuples to delete, return false
  if (delete_done_) {
    return false;
  }

  // The number of rows deleted into the table
  int num_deleted = 0;

  while (true) {
    // Get the next tuple
    Tuple child_tuple{};
    RID child_rid;
    const auto status = child_executor_->Next(&child_tuple, &child_rid);

    if (!status) {
      delete_done_ = true;
      break;
    }

    // Delete the tuple from the table
    table_info_->table_->UpdateTupleMeta(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, true}, child_rid);

    // Update the index
    auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto &index_info : table_indexes) {
      auto delete_key =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(delete_key, child_rid, exec_ctx_->GetTransaction());
    }

    num_deleted++;
  }

  std::vector<Value> values{{INTEGER, num_deleted}};
  *tuple = Tuple(values, &(plan_->OutputSchema()));
  return true;
}

}  // namespace bustub
