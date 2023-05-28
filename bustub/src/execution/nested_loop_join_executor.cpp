//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <iterator>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  is_inner_loop_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!is_inner_loop_) {
      RID r;
      if (!left_executor_->Next(&curr_, &r)) {
        return false;
      }
      right_executor_->Init();
      is_inner_loop_ = true;
      curr_emitted_ = false;
    }

    Tuple t;
    RID r;
    if (!right_executor_->Next(&t, &r)) {
      if (plan_->join_type_ == JoinType::LEFT && !curr_emitted_) {
        auto values = std::vector<Value>{};
        values.reserve(GetOutputSchema().GetColumnCount());

        auto left_size = left_executor_->GetOutputSchema().GetColumnCount();
        for (decltype(left_size) i = 0; i < left_size; i++) {
          values.push_back(curr_.GetValue(&left_executor_->GetOutputSchema(), i));
        }

        auto right_size = right_executor_->GetOutputSchema().GetColumnCount();
        for (decltype(right_size) i = 0; i < right_size; i++) {
          values.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
          assert(values.back().IsNull()); 
        }

        *tuple = Tuple(values, &GetOutputSchema());

        is_inner_loop_ = false;
        return true;
      }
      is_inner_loop_ = false;
      continue;
    }
    auto value = plan_->Predicate()->EvaluateJoin(&curr_, left_executor_->GetOutputSchema(), &t,
                                                  right_executor_->GetOutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      auto values = std::vector<Value>{};
      values.reserve(GetOutputSchema().GetColumnCount());

      auto left_size = left_executor_->GetOutputSchema().GetColumnCount();
      for (decltype(left_size) i = 0; i < left_size; i++) {
        values.push_back(curr_.GetValue(&left_executor_->GetOutputSchema(), i));
      }

      auto right_size = right_executor_->GetOutputSchema().GetColumnCount();
      for (decltype(right_size) i = 0; i < right_size; i++) {
        values.push_back(t.GetValue(&right_executor_->GetOutputSchema(), i));
      }

      *tuple = Tuple(values, &GetOutputSchema());
      curr_emitted_ = true;
      return true;
    }
  }
}

}  // namespace bustub
