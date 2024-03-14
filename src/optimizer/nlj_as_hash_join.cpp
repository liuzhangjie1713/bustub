#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::RewriteExpressionForHashJoin(const bustub::AbstractExpressionRef &expr,
                                             std::vector<AbstractExpressionRef> *left_key_expressions,
                                             std::vector<AbstractExpressionRef> *right_key_expressions) -> bool {
  // If the expression is a comparison expression, check if it is an equal condition
  if (const auto *compare_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); compare_expr != nullptr) {
    if (compare_expr->comp_type_ != ComparisonType::Equal) {
      return false;
    }
    // If the expression is an equal condition, check if it is a column = column condition
    const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(compare_expr->GetChildAt(0).get());
    const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(compare_expr->GetChildAt(1).get());
    if (left_expr == nullptr || right_expr == nullptr) {
      return false;
    }
    // construct the key expressions for the hash join
    if (left_expr->GetTupleIdx() == 0) {
      left_key_expressions->emplace_back(
          std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
      right_key_expressions->emplace_back(
          std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
      return true;
    }
    if (left_expr->GetTupleIdx() == 1) {
      left_key_expressions->emplace_back(
          std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
      right_key_expressions->emplace_back(
          std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
      return true;
    }
  }

  // If the expression is a logic expression(and / or), recursively rewrite its children
  if (const auto *logic_expr = dynamic_cast<LogicExpression *>(expr.get()); logic_expr != nullptr) {
    return std::all_of(logic_expr->GetChildren().begin(), logic_expr->GetChildren().end(), [&](const auto &child) {
      return RewriteExpressionForHashJoin(child, left_key_expressions, right_key_expressions);
    });

    //    for (auto &child : logic_expr->GetChildren()) {
    //      if (!RewriteExpressionForHashJoin(child, left_key_expressions, right_key_expressions)) {
    //        return false;
    //      }
    //    }
    //    return true;
  }

  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  // Recursively optimize abstract plan tree
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // Check if the plan is a NestedLoopJoin
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Check if the NestedLoopJoin has exactly two children
    BUSTUB_ENSURE(optimized_plan->children_.size() == 2, "NestedLoopJoin with single children?? Impossible!");
    if (!IsPredicateTrue(nlj_plan.Predicate())) {
      std::vector<AbstractExpressionRef> left_key_expressions;
      std::vector<AbstractExpressionRef> right_key_expressions;
      std::cout << "Optimizing NestedLoopJoin to HashJoin1" << std::endl;
      // Rewrite the predicate to get the key expressions for the hash join
      if (RewriteExpressionForHashJoin(nlj_plan.Predicate(), &left_key_expressions, &right_key_expressions)) {
        std::cout << "Optimizing NestedLoopJoin to HashJoin1" << std::endl;
        std::cout << left_key_expressions.size() << std::endl;
        std::cout << right_key_expressions.size() << std::endl;
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_key_expressions),
                                                  std::move(right_key_expressions), nlj_plan.GetJoinType());
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
