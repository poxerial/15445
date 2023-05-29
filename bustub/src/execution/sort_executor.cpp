#include "execution/executors/sort_executor.h"
#include <algorithm>
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  if (!tuples_.empty()) {
    iter_ = tuples_.begin();
    return;
  }

  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }

  auto child_schema = child_executor_->GetOutputSchema();

  for (auto i = plan_->GetOrderBy().rbegin(); i != plan_->GetOrderBy().rend(); ++i) {
    const auto& order_by = *i;
    std::stable_sort(tuples_.begin(), tuples_.end(), [&](const Tuple &l, const Tuple &r) -> bool {
      auto left_result = order_by.second->Evaluate(&l, child_schema);
      auto right_result = order_by.second->Evaluate(&r, child_schema);
      CmpBool cmp_result;
      if (order_by.first != OrderByType::DESC) {
        cmp_result = left_result.CompareLessThan(right_result);
      } else {
        cmp_result = right_result.CompareLessThan(left_result);
      }
      return cmp_result == CmpBool::CmpTrue;
    });
  }

  iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.end()) {
    return false;
  }

  *tuple = *iter_;

  ++iter_;

  return true;
}

}  // namespace bustub
