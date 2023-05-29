#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    auto limit_plan = reinterpret_cast<const LimitPlanNode *>(optimized_plan.get());
    auto child_plan = limit_plan->GetChildPlan();
    if (child_plan->GetType() == PlanType::Sort) {
      auto sort_plan = reinterpret_cast<const SortPlanNode *>(child_plan.get());
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan->GetChildPlan(), sort_plan->GetOrderBy(),
                                            limit_plan->GetLimit());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
