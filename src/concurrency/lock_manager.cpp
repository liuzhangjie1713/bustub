//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <set>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 1. check if the transaction can take the lock
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  // 2. get the lock request queue of the table
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = table_lock_map_.at(oid);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  std::shared_ptr<LockRequest> new_lock_request =
      std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  bool grant = true;
  bool upgrade = false;

  // 3. check if this lock request is a lock upgrade quest
  for (auto request_iterator = lock_request_queue->request_queue_.begin();
       request_iterator != lock_request_queue->request_queue_.end(); request_iterator++) {
    auto lock_request = *request_iterator;
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      // if the transaction already has the same lock, return true
      if ((*request_iterator)->lock_mode_ == lock_mode) {
        return true;
      }

      // if the transaction has a higher level lock, try to upgrade the lock
      if (!UpgradeLockTable(txn, lock_request->lock_mode_, lock_mode, oid, lock_request_queue)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      upgrade = true;
      lock_request_queue->request_queue_.erase(request_iterator);
      break;
    }
  }

  // 4. if  not, add it to the lock request queue
  if (upgrade) {
    lock_request_queue->request_queue_.push_front(new_lock_request);
  } else {
    lock_request_queue->request_queue_.push_back(new_lock_request);
  }

  // 5. try to grant the lock
  // check the lock request compatibility
  for (const auto &lock_request : lock_request_queue->request_queue_) {
    if (lock_request->granted_ && !AreLocksCompatible(lock_request->lock_mode_, lock_mode)) {
      grant = false;
      break;
    }
  }

  if (!grant) {
    // wait until the new lock is granted
    lock_request_queue->cv_.wait(
        lock, [&]() { return new_lock_request->granted_ || txn->GetState() == TransactionState::ABORTED; });

    // if the transaction is aborted, remove the lock request and notify other transactions
    if (txn->GetState() == TransactionState::ABORTED) {
      if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      lock_request_queue->request_queue_.remove(new_lock_request);
      GrantNewLocksIfPossible(lock_request_queue);
      lock_request_queue->cv_.notify_all();

      return false;
    }
  } else {
    new_lock_request->granted_ = true;
  }

  // maintain transaction metadata
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  UpdateTransactionTableLock(txn, oid, lock_mode);

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 1.check if the table has any row locks
  size_t row_lock_count = 0;
  if (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end()) {
    row_lock_count += txn->GetExclusiveRowLockSet()->find(oid)->second.size();
  }
  if (txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end()) {
    row_lock_count += txn->GetSharedRowLockSet()->find(oid)->second.size();
  }
  if (row_lock_count > 0) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // 2. get the lock request queue of the table
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = table_lock_map_.at(oid);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  if (lock_request_queue->request_queue_.empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 3. find the lock request of the transaction
  bool found = false;
  for (const auto &lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      found = true;
      lock_request_queue->request_queue_.remove(lock_request);
      break;
    }
  }
  if (!found) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 4. notify other transactions
  UpdateTransactionTableUnlock(txn, oid, false);
  GrantNewLocksIfPossible(lock_request_queue);
  lock_request_queue->cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 1. check if the transaction can take the lock
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  if (!CanTxnTakeLock(txn, lock_mode)) {
    return false;
  }

  // 2. check if transaction has the table lock appropriated with the row lock
  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  // 3. get the lock request queue of the table
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = row_lock_map_.at(rid);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  std::shared_ptr<LockRequest> new_lock_request =
      std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  bool grant = true;
  bool upgrade = false;

  // 4. check if this lock request is a lock upgrade quest
  for (auto request_iterator = lock_request_queue->request_queue_.begin();
       request_iterator != lock_request_queue->request_queue_.end(); request_iterator++) {
    auto lock_request = *request_iterator;
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      // if the transaction already has the same lock, return true
      if (lock_request->lock_mode_ == lock_mode) {
        return true;
      }

      // if the transaction has a higher level lock, try to upgrade the lock
      UpgradeLockRow(txn, lock_request->lock_mode_, lock_mode, oid, rid, lock_request_queue);
      upgrade = true;
      lock_request_queue->request_queue_.erase(request_iterator);
      break;
    }
  }

  // 5. if  not, add it to the lock request queue
  if (upgrade) {
    lock_request_queue->request_queue_.push_front(new_lock_request);
  } else {
    lock_request_queue->request_queue_.push_back(new_lock_request);
  }

  // 6. try to grant the lock
  // check the lock request compatibility
  for (const auto &lock_request : lock_request_queue->request_queue_) {
    if (lock_request->granted_ && !AreLocksCompatible(lock_request->lock_mode_, lock_mode)) {
      grant = false;
      break;
    }
  }

  if (!grant) {
    // wait until the new lock is granted
    lock_request_queue->cv_.wait(
        lock, [&]() { return new_lock_request->granted_ || txn->GetState() == TransactionState::ABORTED; });

    // if the transaction is aborted, remove the lock request and notify other transactions
    if (txn->GetState() == TransactionState::ABORTED) {
      if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      lock_request_queue->request_queue_.remove(new_lock_request);
      GrantNewLocksIfPossible(lock_request_queue);
      lock_request_queue->cv_.notify_all();

      return false;
    }
  } else {
    new_lock_request->granted_ = true;
  }

  // maintain transaction metadata
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  UpdateTransactionRowLock(txn, oid, rid, lock_mode);

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // 1. get the lock request queue of the row
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    row_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_.at(rid);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  if (lock_request_queue->request_queue_.empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 2. find the lock request of the transaction
  bool found = false;
  for (const auto &lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      found = true;
      lock_request_queue->request_queue_.remove(lock_request);
      break;
    }
  }
  if (!found) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 3. notify other transactions
  UpdateTransactionRowUnlock(txn, oid, rid, force, false);
  GrantNewLocksIfPossible(lock_request_queue);
  lock_request_queue->cv_.notify_all();

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (t1 == INVALID_TXN_ID || t2 == INVALID_TXN_ID) {
    return;
  }

  if (t1 == t2) {
    return;
  }

  waits_for_latch_.lock();
  waits_for_[t1].push_back(t2);
  waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (t1 == INVALID_TXN_ID || t2 == INVALID_TXN_ID) {
    return;
  }

  waits_for_latch_.lock();

  for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); it++) {
    if (*it == t2) {
      waits_for_[t1].erase(it);
      break;
    }
  }

  waits_for_latch_.unlock();
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_set<txn_id_t> visited;

  std::set<txn_id_t> sorted_txn;
  for (const auto &pair : waits_for_) {
    sorted_txn.insert(pair.first);
  }

  for (const auto &source_txn : sorted_txn) {
    if (visited.find(source_txn) == visited.end()) {
      std::unordered_set<txn_id_t> on_path;
      if (FindCycle(source_txn, on_path, visited, txn_id)) {
        return true;
      }
    }
  }

  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &start : waits_for_) {
    for (const auto &end : start.second) {
      edges.emplace_back(std::make_pair(start.first, end));
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      // detect deadlock
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();

      BuildWaitForGraph();

      txn_id_t abort_txn_id = INVALID_TXN_ID;
      while (HasCycle(&abort_txn_id)) {  // DFS
        // a way to notify transations that they've been aborted;
        txn_manager_->GetTransaction(abort_txn_id)->SetState(TransactionState::ABORTED);
        waits_for_[abort_txn_id].clear();
      }

      for (const auto &table : table_lock_map_) {
        table.second->cv_.notify_all();
      }
      table_lock_map_latch_.unlock();
      for (const auto &row : row_lock_map_) {
        row.second->cv_.notify_all();
      }
      row_lock_map_latch_.unlock();

      waits_for_.clear();
    }
  }
}

auto LockManager::FindCycle(txn_id_t source_txn, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
  // if cur txn is already on the path, there is a cycle
  if (on_path.find(source_txn) != on_path.end()) {
    txn_id_t youngest_txn = INVALID_TXN_ID;
    for (const auto &txn : on_path) {
      youngest_txn = std::max(youngest_txn, txn);
    }
    *abort_txn_id = youngest_txn;
    return true;
  }

  // mark cur txn on the path
  on_path.insert(source_txn);

  // iterate through all the transactions that cur txn is waiting for
  std::set<txn_id_t> wait_for_txns(waits_for_[source_txn].begin(), waits_for_[source_txn].end());
  for (const auto &wait_for_txn : wait_for_txns) {
    if (FindCycle(wait_for_txn, on_path, visited, abort_txn_id)) {
      return true;
    }
  }

  visited.insert(source_txn);
  on_path.erase(source_txn);
  return false;
}

void LockManager::BuildWaitForGraph() {
  for (const auto &table : table_lock_map_) {
    std::vector<txn_id_t> granted;
    table.second->latch_.lock();
    for (const auto &request : table.second->request_queue_) {
      if (request->granted_) {
        granted.push_back(request->txn_id_);
      }
    }

    for (const auto &request : table.second->request_queue_) {
      if (!request->granted_) {
        for (const auto &grant : granted) {
          AddEdge(request->txn_id_, grant);
        }
      }
    }

    table.second->latch_.unlock();
  }

  for (const auto &row : row_lock_map_) {
    std::vector<txn_id_t> granted;
    row.second->latch_.lock();
    for (const auto &request : row.second->request_queue_) {
      if (request->granted_) {
        granted.push_back(request->txn_id_);
      }
    }

    for (const auto &request : row.second->request_queue_) {
      if (!request->granted_) {
        for (const auto &grant : granted) {
          AddEdge(request->txn_id_, grant);
        }
      }
    }

    row.second->latch_.unlock();
  }
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  // 1. guarantee the compatibility of lock request and transaction state
  auto transaction_state = txn->GetState();
  if (transaction_state == TransactionState::ABORTED || transaction_state == TransactionState::COMMITTED) {
    return false;
  }

  // 2. guarantee the compatibility of lock request and isolation level
  auto isolation_level = txn->GetIsolationLevel();
  switch (isolation_level) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (transaction_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (transaction_state == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (transaction_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
  }

  return true;
}

auto LockManager::CanLockUpgrade(LockManager::LockMode curr_lock_mode, LockManager::LockMode requested_lock_mode)
    -> bool {
  // IS -> [S, X, IX, SIX]
  // S -> [X, SIX]
  // IX -> [X, SIX]
  // SIX -> [X]

  switch (curr_lock_mode) {
    case LockMode::SHARED:
      return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_SHARED:
      return requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::EXCLUSIVE ||
             requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
             requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::EXCLUSIVE;
  }

  return false;
}

auto LockManager::AreLocksCompatible(LockManager::LockMode l1, LockManager::LockMode l2) -> bool {
  switch (l1) {
    case LockMode::SHARED:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;

    case LockMode::EXCLUSIVE:
      return false;

    case LockMode::INTENTION_SHARED:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE || l2 == LockMode::SHARED ||
             l2 == LockMode::SHARED_INTENTION_EXCLUSIVE;

    case LockMode::INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;

    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED;
  }

  return false;
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode curr_lock_mode, LockMode requested_lock_mode,
                                   const table_oid_t &oid, const std::shared_ptr<LockRequestQueue> &lock_request_queue)
    -> bool {
  // check if there is other transaction upgrading
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // check if the current lock mode is compatible with the requested lock mode
  if (!CanLockUpgrade(curr_lock_mode, requested_lock_mode)) {
    return false;
  }

  lock_request_queue->upgrading_ = txn->GetTransactionId();
  UpdateTransactionTableUnlock(txn, oid, true);
  return true;
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockManager::LockMode curr_lock_mode,
                                 LockManager::LockMode requested_lock_mode, const table_oid_t &oid, const RID &rid,
                                 const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  if (!CanLockUpgrade(curr_lock_mode, requested_lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
  }

  lock_request_queue->upgrading_ = txn->GetTransactionId();
  UpdateTransactionRowUnlock(txn, oid, rid, false, true);
  return true;
}

void LockManager::GrantNewLocksIfPossible(const std::shared_ptr<LockRequestQueue> &lock_request_queue) {
  //
  for (const auto &wait_request : lock_request_queue->request_queue_) {
    if (!wait_request->granted_) {
      for (const auto &grant_request : lock_request_queue->request_queue_) {
        if (grant_request->granted_ && !AreLocksCompatible(grant_request->lock_mode_, wait_request->lock_mode_)) {
          return;
        }
      }

      wait_request->granted_ = true;
    }
  }
}

void LockManager::UpdateTransactionTableLock(Transaction *txn, const table_oid_t &oid, LockMode lock_mode) {
  txn->LockTxn();
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;

    case LockManager::LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;

    case LockManager::LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;

    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;

    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }

  txn->UnlockTxn();
}

void LockManager::UpdateTransactionTableUnlock(Transaction *txn, const table_oid_t &oid, bool upgrade) {
  txn->LockTxn();

  if (txn->IsTableSharedLocked(oid)) {
    txn->GetSharedTableLockSet()->erase(oid);
    if (!upgrade && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (txn->IsTableExclusiveLocked(oid)) {
    txn->GetExclusiveTableLockSet()->erase(oid);
    if (!upgrade && (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED ||
                     txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
                     txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (txn->IsTableIntentionSharedLocked(oid)) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (txn->IsTableIntentionExclusiveLocked(oid)) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }

  txn->UnlockTxn();
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  table_lock_map_latch_.lock();
  auto lock_request_queue = table_lock_map_.at(oid);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  for (auto &lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (!lock_request->granted_) {
        break;
      }

      switch (row_lock_mode) {
        case LockMode::SHARED:
          return lock_request->lock_mode_ == LockMode::INTENTION_SHARED ||
                 lock_request->lock_mode_ == LockMode::SHARED ||
                 lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
                 lock_request->lock_mode_ == LockMode::EXCLUSIVE ||
                 lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE;

        case LockMode::EXCLUSIVE:
          return lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                 lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
                 lock_request->lock_mode_ == LockMode::EXCLUSIVE;

        case LockMode::INTENTION_SHARED:
        case LockMode::INTENTION_EXCLUSIVE:
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          break;
      }
    }
  }
  return false;
}
void LockManager::UpdateTransactionRowLock(Transaction *txn, const table_oid_t &oid, const RID &rid,
                                           LockManager::LockMode lock_mode) {
  txn->LockTxn();

  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      // txn->GetSharedRowLockSet()->at(oid).insert(rid);
      (*txn->GetSharedRowLockSet())[oid].insert(rid);
      break;
    case LockManager::LockMode::EXCLUSIVE:
      // txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
      (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }

  txn->UnlockTxn();
}

void LockManager::UpdateTransactionRowUnlock(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force,
                                             bool upgrade) {
  txn->LockTxn();

  if (txn->IsRowSharedLocked(oid, rid)) {
    // txn->GetSharedRowLockSet()->at(oid).erase(rid);
    txn->GetSharedRowLockSet()->find(oid)->second.erase(rid);
    if (!force && !upgrade && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (txn->IsRowExclusiveLocked(oid, rid)) {
    // txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
    txn->GetExclusiveRowLockSet()->find(oid)->second.erase(rid);
    if (!force && !upgrade &&
        (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED ||
         txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
         txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  txn->UnlockTxn();
}

}  // namespace bustub
