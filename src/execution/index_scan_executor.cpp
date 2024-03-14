//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid())),
      table_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      tree_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())),
      index_iter_(tree_->GetBeginIterator()) {}

void IndexScanExecutor::Init() { cursor_ = 0; }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (index_iter_ != tree_->GetEndIterator()) {
    // Get the rid by index
    RID tuple_rid = (*index_iter_).second;

    // Get the tuple by rid
    auto tuple_info = table_info_->table_->GetTuple(tuple_rid);

    // Do not emit tuples that are deleted in the TableHeap
    auto tuple_meta = tuple_info.first;
    if (tuple_meta.is_deleted_) {
      ++index_iter_;
      continue;
    }

    tuple_info_list_.emplace_back(std::move(tuple_info.second), tuple_rid);
    ++index_iter_;
    break;
  }

  if (cursor_ == tuple_info_list_.size() && index_iter_ == tree_->GetEndIterator()) {
    return false;
  }

  *rid = tuple_info_list_[cursor_].second;
  *tuple = tuple_info_list_[cursor_].first;
  cursor_++;

  return true;
}

}  // namespace bustub
