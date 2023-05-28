//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  if (!hash_map_.empty()) {
    right_child_->Init();
    right_child_end_ = false;
    curr_right_valid_ = false;
    return;
  }

  left_child_->Init();

  Tuple t;
  RID r;
  while (left_child_->Next(&t, &r)) {
    auto key = HashJoinKey(plan_->LeftJoinKeyExpressions(), t, left_child_->GetOutputSchema());
    if (hash_map_.count(key) == 0) {
      hash_map_[key] = {};
    }
    hash_map_[key].Append(t);
  }

  right_child_->Init();
  right_child_end_ = false;
  curr_right_valid_ = false;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  for (;;) {
    if (right_child_end_) {
      if (left_iter_.IsEnd()) {
        return false;
      }
      auto left = *left_iter_;  

      auto values = std::vector<Value>{};
      values.reserve(GetOutputSchema().GetColumnCount());

      auto left_size = left_child_->GetOutputSchema().GetColumnCount();
      for (decltype(left_size) i = 0; i < left_size; i++) {
        values.push_back(left.GetValue(&left_child_->GetOutputSchema(), i));
      }

      auto right_size = right_child_->GetOutputSchema().GetColumnCount();
      for (decltype(right_size) i = 0; i < right_size; i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }

      *tuple = Tuple(values, &GetOutputSchema());
      ++left_iter_;
      return true;
    }

    if (curr_right_valid_) {
      if (iter_ == end_) {
        curr_right_valid_ = false;
        continue;
      }

      for (size_t i = 0; i < plan_->left_key_expressions_.size(); i++) {
        auto left_key_value = plan_->left_key_expressions_[i]->Evaluate(&iter_->tuple_, left_child_->GetOutputSchema());
        auto right_key_value =
            plan_->right_key_expressions_[i]->Evaluate(&curr_right_, right_child_->GetOutputSchema());
        if (left_key_value.CompareEquals(right_key_value) != CmpBool::CmpTrue) {
          ++iter_;
          continue;
        }
      }

      auto values = std::vector<Value>{};
      values.reserve(GetOutputSchema().GetColumnCount());

      auto left_size = left_child_->GetOutputSchema().GetColumnCount();
      for (decltype(left_size) i = 0; i < left_size; i++) {
        values.push_back(iter_->tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }

      auto right_size = right_child_->GetOutputSchema().GetColumnCount();
      for (decltype(right_size) i = 0; i < right_size; i++) {
        values.push_back(curr_right_.GetValue(&right_child_->GetOutputSchema(), i));
      }

      *tuple = Tuple(values, &GetOutputSchema());
      iter_->is_emitted_ = true;

      ++iter_;
      return true;
    }

    RID r;

    auto status = right_child_->Next(&curr_right_, &r);
    if (!status) {
      if (plan_->join_type_ != JoinType::LEFT) {
        return false;
      }
      right_child_end_ = true;
      left_iter_ = LeftHashJoinIterator(hash_map_);
      continue;
    }

    auto key = HashJoinKey(plan_->RightJoinKeyExpressions(), curr_right_, right_child_->GetOutputSchema());
    auto i = hash_map_.find(key);
    if (i == hash_map_.end()) {
      continue;
    }

    iter_ = i->second.tuples_.begin();
    end_ = i->second.tuples_.end();
    curr_right_valid_ = true;
  }
}

}  // namespace bustub
