//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/exception.h"
#include "type/value.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() { 
    auto catalog = exec_ctx_->GetCatalog();
    auto table = catalog->GetTable(plan_->GetTableOid());
    iter_.emplace(table->table_->MakeIterator());

    if (iter_->IsEnd()) {
        return;
    }

    auto t = iter_->GetTuple();
    while (t.first.is_deleted_) {
        ++*iter_;
        if (iter_->IsEnd()) {
            return;
        }
        t = iter_->GetTuple();
    }
 }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (!iter_.has_value()) {
        throw ExecutionException("SeqScanExecutor is called without initialization!");
    }

    if (iter_->IsEnd()) {
        return false;
    }

    *tuple = iter_->GetTuple().second;
    *rid = iter_->GetRID();

    ++*iter_;

    if (iter_->IsEnd()) {
        return true;
    }

    auto t = iter_->GetTuple();
    while (t.first.is_deleted_) {
        ++*iter_;
        if (iter_->IsEnd()) {
            return true;
        }
        t = iter_->GetTuple();
    }

    return true;
}

}  // namespace bustub
