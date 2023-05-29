#include "execution/executors/topn_executor.h"
#include "common/macros.h"
#include "type/type.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      tuples_([&](const Tuple &l, const Tuple &r) -> bool {
        auto &order_bys = plan_->GetOrderBy();
        auto &child_schema = child_executor_->GetOutputSchema();
        for (const auto &order_by : order_bys) {
          auto left_result = order_by.second->Evaluate(&l, child_schema);
          auto right_result = order_by.second->Evaluate(&r, child_schema);
          CmpBool cmp_result;
          if (order_by.first != OrderByType::DESC) {
            cmp_result = right_result.CompareLessThan(left_result);
          } else {
            cmp_result = left_result.CompareLessThan(right_result);
          }
          if (cmp_result == CmpBool::CmpTrue) {
            return true;
          }
          auto is_equal = left_result.CompareEquals(right_result);
          if (is_equal != CmpBool::CmpTrue) {
            return false;
          }
        }
        return false;
      }) {}

void TopNExecutor::Init() {
  if (!tuples_.empty()) {
    iter_ = tuples_.rbegin();
    return;
  }

  child_executor_->Init();

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.insert(tuple);
    if (tuples_.size() > plan_->GetN()) {
        tuples_.erase(tuples_.begin());
    }
  }

  iter_ = tuples_.rbegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.rend()) {
    return false;
  }

  *tuple = *iter_;

  ++iter_;

  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return tuples_.size(); };

}  // namespace bustub
