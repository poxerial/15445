#include <algorithm>
#include <memory>
#include <type_traits>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
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

auto static Tranverse(std::vector<AbstractExpressionRef> &left_exprs, std::vector<AbstractExpressionRef> &right_exprs,
                      const AbstractExpressionRef &expr) -> bool {
  if (auto cmp_expr = dynamic_cast<ComparisonExpression *>(expr.get())) {
    if (cmp_expr->comp_type_ != ComparisonType::Equal) {
      return false;
    }
    for (auto child : cmp_expr->GetChildren()) {
      if (auto clm_expr = dynamic_cast<ColumnValueExpression *>(child.get())) {
        if (clm_expr->GetTupleIdx() == 0) {
          left_exprs.push_back(std::move(child));
        } else {
          right_exprs.push_back(std::move(child));
        }
      } else {
        return false;
      }
    }
  } else if (auto logic_expr = dynamic_cast<LogicExpression *>(expr.get())) {
    if (logic_expr->logic_type_ != LogicType::And) {
      return false;
    }
    for (const auto &child : expr->GetChildren()) {
      if (!Tranverse(left_exprs, right_exprs, child)) {
        return false;
      }
    }
  } else {
    return false;
  }
  return true;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    auto left_exprs = std::vector<AbstractExpressionRef>{};
    auto right_exprs = std::vector<AbstractExpressionRef>{};

    if (!Tranverse(left_exprs, right_exprs, nlj_plan.predicate_)) {
      return optimized_plan;
    }

    auto hash_join_plan =
        std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                           left_exprs, right_exprs, nlj_plan.GetJoinType());
    return hash_join_plan;
  }

  return plan;
}

}  // namespace bustub
