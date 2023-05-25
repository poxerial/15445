//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  auto catalog = exec_ctx_->GetCatalog();
  table_ = catalog->GetTable(plan_->TableOid());
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (table_ == nullptr) {
    return false;
  }

  auto num = int32_t{0};

  Tuple t;
  RID r;

  if (!child_executor_->Next(&t, &r)) {
    auto values = std::vector<Value>{Value(INTEGER, num)};
    *tuple = Tuple(values, &GetOutputSchema());
    table_ = nullptr;
    return true;
  }

  auto catalog = exec_ctx_->GetCatalog();
  auto indexes = catalog->GetTableIndexes(table_->name_);

  do {
    auto old_tuple = table_->table_->GetTuple(r);

    old_tuple.first.is_deleted_ = true;
    table_->table_->UpdateTupleMeta(old_tuple.first, r);

    for (auto index : indexes) {
      auto old_tuple_key = old_tuple.second.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_,
                                                         index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_tuple_key, r, nullptr);
    }

    num++;
  } while (child_executor_->Next(&t, &r));

  auto values = std::vector<Value>{Value(INTEGER, num)};
  *tuple = Tuple(values, &GetOutputSchema());
  table_ = nullptr;
  return true;
}

}  // namespace bustub
