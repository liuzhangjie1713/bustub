//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  auto LockTable() -> bool;
  auto LockRow() -> bool;
  auto UnlockRow(bool force) -> void;
  auto CheckIfHoldHigherLockTable(LockManager::LockMode mode, table_oid_t oid) -> bool;
  auto CheckIfHoldHigherLockRow(LockManager::LockMode mode, table_oid_t oid, RID rid) -> bool;
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;

  /** The table to scan */
  const TableInfo *table_info_;

  /** The table iterator */
  TableIterator table_iter_;

  std::vector<std::pair<Tuple, RID>> tuple_info_list_;

  LockManager *lock_manager_;

  size_t cursor_{0};

  bool done_{false};
};
}  // namespace bustub
