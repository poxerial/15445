//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();

  Tuple t;
  RID r;

  while (child_->Next(&t, &r)) {
    auto key = MakeAggregateKey(&t);
    auto value = MakeAggregateValue(&t);
    aht_.InsertCombine(key, value);
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto end = aht_.End();
  if (aht_iterator_ == end) {
    if (aht_.Begin() == end) {
      if (exec_ctx_ == nullptr || !plan_->GetGroupBys().empty()) {
        return false;
      }
      auto values = std::vector<Value>{};
      values.reserve(plan_->GetGroupBys().size() + plan_->GetAggregates().size());

      for (const auto &expr : plan_->GetGroupBys()) {
        values.emplace_back(expr->GetReturnType());
      }

      auto aggs = aht_.GenerateInitialAggregateValue();

      for (const auto &value : aggs.aggregates_) {
        values.push_back(value);
      }

      *tuple = Tuple(values, &GetOutputSchema());

      exec_ctx_ = nullptr;
      return true;
    }
    return false;
  }

  auto values = aht_iterator_.Key().group_bys_;
  values.reserve(plan_->GetGroupBys().size() + plan_->GetAggregates().size());

  for (const auto &value : aht_iterator_.Val().aggregates_) {
    values.push_back(value);
  }

  *tuple = Tuple(values, &GetOutputSchema());

  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
