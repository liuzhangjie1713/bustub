//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      table_iter_(table_info_->table_->MakeIterator()) {}

void SeqScanExecutor::Init() { cursor_ = 0; }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iter_.IsEnd()) {
    auto tuple_info = table_iter_.GetTuple();

    // Do not emit tuples that are deleted in the TableHeap
    auto tuple_meta = tuple_info.first;
    if (tuple_meta.is_deleted_) {
      ++table_iter_;
      continue;
    }

    tuple_info_list_.emplace_back(std::move(tuple_info.second), table_iter_.GetRID());
    ++table_iter_;
    break;
  }

  if (cursor_ == tuple_info_list_.size()) {
    return false;
  }

  *rid = tuple_info_list_[cursor_].second;
  *tuple = tuple_info_list_[cursor_].first;
  cursor_++;

  return true;
}

}  // namespace bustub
