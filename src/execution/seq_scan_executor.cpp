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
      table_iter_(table_info_->table_->MakeEagerIterator()),
      lock_manager_(exec_ctx->GetLockManager()) {}

void SeqScanExecutor::Init() {
  //  take a table lock
  if (!done_) {
    LockTable();
  }

  cursor_ = 0;
  done_ = true;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 1. get the current position of the table iterator
  while (!table_iter_.IsEnd()) {
    // 2. lock the tuple as needed for the isolation level
    bool is_lock = LockRow();

    // 3. fetch the tuple, check the tuple meta
    auto tuple_info = table_iter_.GetTuple();

    // Do not emit tuples that are deleted in the TableHeap
    auto tuple_meta = tuple_info.first;
    if (tuple_meta.is_deleted_) {
      if (is_lock) {
        UnlockRow(true);
      }

      ++table_iter_;
      continue;
    }

    if (is_lock) {
      UnlockRow(false);
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

auto SeqScanExecutor::LockTable() -> bool {
  auto txn = exec_ctx_->GetTransaction();

  bool can_lock = true;
  bool is_lock = false;

  // if the current operation is delete, we should assume all tuples scanned will be deleted
  // and we need to lock the table in X locks on the table and tuples as nessary
  LockManager::LockMode mode = LockManager::LockMode::INTENTION_SHARED;
  if (exec_ctx_->IsDelete()) {
    mode = LockManager::LockMode::INTENTION_EXCLUSIVE;
  }

  // already hold higher level lock
  if (CheckIfHoldHigherLockTable(mode, plan_->GetTableOid())) {
    return true;
  }

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
        is_lock = true;
        can_lock = lock_manager_->LockTable(txn, mode, plan_->GetTableOid());
      }
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      is_lock = true;
      can_lock = lock_manager_->LockTable(txn, mode, plan_->GetTableOid());
      break;
  }

  if (!can_lock) {
    throw ExecutionException("SeqScanExecutor: fail to lock table");
  }

  return is_lock;
}

auto SeqScanExecutor::LockRow() -> bool {
  auto txn = exec_ctx_->GetTransaction();

  bool is_lock = false;
  bool can_lock = true;

  LockManager::LockMode mode = LockManager::LockMode::SHARED;
  if (exec_ctx_->IsDelete()) {
    mode = LockManager::LockMode::EXCLUSIVE;
  }

  // already hold higher level lock
  if (CheckIfHoldHigherLockRow(mode, plan_->GetTableOid(), table_iter_.GetRID())) {
    return true;
  }

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (mode == LockManager::LockMode::EXCLUSIVE) {
        is_lock = true;
        can_lock = lock_manager_->LockRow(txn, mode, plan_->GetTableOid(), table_iter_.GetRID());
      }
      break;
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      is_lock = true;
      can_lock = lock_manager_->LockRow(txn, mode, plan_->GetTableOid(), table_iter_.GetRID());
      break;
  }

  if (!can_lock) {
    throw ExecutionException("SeqScanExecutor: fail to lock tuple");
  }

  return is_lock;
}

auto SeqScanExecutor::UnlockRow(bool force) -> void {
  auto txn = exec_ctx_->GetTransaction();
  if (force) {
    lock_manager_->UnlockRow(txn, plan_->GetTableOid(), table_iter_.GetRID(), force);
    return;
  }

  if (!exec_ctx_->IsDelete()) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
        break;
      case IsolationLevel::READ_COMMITTED:
        lock_manager_->UnlockRow(txn, plan_->GetTableOid(), table_iter_.GetRID());
        break;
      case IsolationLevel::REPEATABLE_READ:
        break;
    }
  }
}

auto SeqScanExecutor::CheckIfHoldHigherLockTable(LockManager::LockMode mode, table_oid_t oid) -> bool {
  Transaction *cur_transaction = exec_ctx_->GetTransaction();

  bool s_lock = cur_transaction->IsTableSharedLocked(oid);
  bool x_lock = cur_transaction->IsTableExclusiveLocked(oid);
  bool is_lock = cur_transaction->IsTableIntentionSharedLocked(oid);
  bool ix_lock = cur_transaction->IsTableIntentionExclusiveLocked(oid);
  bool six_lock = cur_transaction->IsTableSharedIntentionExclusiveLocked(oid);

  switch (cur_transaction->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
        return ix_lock || x_lock || six_lock;
      }
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      switch (mode) {
        case LockManager::LockMode::SHARED:
          return s_lock || x_lock || six_lock;
        case LockManager::LockMode::EXCLUSIVE:
          return x_lock;
        case LockManager::LockMode::INTENTION_SHARED:
          return is_lock || s_lock || x_lock || ix_lock || six_lock;
        case LockManager::LockMode::INTENTION_EXCLUSIVE:
          return ix_lock || x_lock || six_lock;
        case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
          return six_lock || x_lock;
      }
      break;
  }

  return false;
}

auto SeqScanExecutor::CheckIfHoldHigherLockRow(LockManager::LockMode mode, table_oid_t oid, RID rid) -> bool {
  Transaction *cur_transaction = exec_ctx_->GetTransaction();
  bool s_lock = cur_transaction->IsRowSharedLocked(oid, rid);
  bool x_lock = cur_transaction->IsRowExclusiveLocked(oid, rid);

  switch (cur_transaction->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (mode == LockManager::LockMode::EXCLUSIVE) {
        return x_lock;
      }
    case IsolationLevel::READ_COMMITTED:
    case IsolationLevel::REPEATABLE_READ:
      switch (mode) {
        case LockManager::LockMode::SHARED:
          return s_lock || x_lock;
        case LockManager::LockMode::EXCLUSIVE:
          return x_lock;
        case LockManager::LockMode::INTENTION_SHARED:
        case LockManager::LockMode::INTENTION_EXCLUSIVE:
        case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
          throw TransactionAbortException(cur_transaction->GetTransactionId(),
                                          AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
      }
      break;
  }

  return false;
}

}  // namespace bustub
