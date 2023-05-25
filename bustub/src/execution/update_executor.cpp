//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <optional>

#include "common/exception.h"
#include "execution/executor_context.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  auto catalog = exec_ctx_->GetCatalog();
  table_ = catalog->GetTable(plan_->TableOid());
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (table_ == nullptr) {
    return false;
  }

  Tuple t;
  RID r;

  auto num = int32_t{0};

  if (!child_executor_->Next(&t, &r)) {
    auto values = std::vector<Value>{Value(INTEGER, num)};
    *tuple = Tuple(values, &GetOutputSchema());
    table_ = nullptr;
    return true;
  }

  auto catalog = exec_ctx_->GetCatalog();
  auto indexes = catalog->GetTableIndexes(table_->name_);

  do {
    auto old_meta= table_->table_->GetTupleMeta(r);
    old_meta.is_deleted_ = true;
    table_->table_->UpdateTupleMeta(old_meta, r);

    auto values = std::vector<Value>{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());

    for (const auto& expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&t, child_executor_->GetOutputSchema()));
    }

    auto new_tuple = Tuple(values, &child_executor_->GetOutputSchema());

    auto meta = TupleMeta{};
    meta.is_deleted_ = false;
    auto new_rid = table_->table_->InsertTuple(meta, new_tuple);
    if (new_rid == std::nullopt) {
      throw ExecutionException("Can't insert tuple to table!");
    }

    for (auto index : indexes) {
      auto old_tuple_key = t.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                                         index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_tuple_key, r, nullptr);

      auto key = new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, *new_rid, nullptr);
    }

    ++num;
  } while (child_executor_->Next(&t, &r));

  auto values = std::vector<Value>{Value(INTEGER, num)};
  *tuple = Tuple(values, &GetOutputSchema());
  table_ = nullptr;
  return true;
}

}  // namespace bustub
