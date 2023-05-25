//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "storage/index/b_plus_tree_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan} {}

void IndexScanExecutor::Init() { 
    auto catalog = exec_ctx_->GetCatalog();
    auto index = catalog->GetIndex(plan_->GetIndexOid());
    table_info_ = catalog->GetTable(index->table_name_);
    auto tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn*>(index->index_.get());
    iter_ = tree->GetBeginIterator();
 }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (iter_.IsEnd()) {
        return false;
    }

    *rid = (*iter_).second;
    *tuple = table_info_->table_->GetTuple(*rid).second;
    
    ++iter_;

    return true;
}

}  // namespace bustub
