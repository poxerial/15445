//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/integer_type.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_ = catalog->GetTable(plan_->TableOid());
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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
    auto meta = TupleMeta{};
    meta.is_deleted_ = false;

    auto new_rid = table_->table_->InsertTuple(meta, t);
    if (new_rid == std::nullopt) {
      throw ExecutionException("Can't insert tuple to table!");
    }
    
    for (auto index : indexes) {
      auto key = t.KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs());
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
