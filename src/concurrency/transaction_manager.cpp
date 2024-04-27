//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  // revert all the changes in write set
  auto write_set = txn->GetWriteSet();
  for (auto table_write_record = write_set->rbegin(); table_write_record != write_set->rend(); table_write_record++) {
    auto table_heap = table_write_record->table_heap_;
    auto wtype = table_write_record->wtype_;
    switch (wtype) {
      case WType::INSERT:
        table_heap->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}, table_write_record->rid_);
        break;

      case WType::DELETE:
        table_heap->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, false}, table_write_record->rid_);
        break;

      case WType::UPDATE:
        break;
    }
  }

  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
