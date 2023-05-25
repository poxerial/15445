/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(IndexIterator&& that) noexcept {
  guard_ = std::move(that.guard_);
  bpm_ = that.bpm_;
  index_ = that.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator=(IndexIterator&& that) noexcept -> IndexIterator& {
  guard_ = std::move(that.guard_);
  bpm_ = that.bpm_;
  index_ = that.index_;
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return guard_.IsEmpty(); 
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto array = reinterpret_cast<const MappingType *>(guard_.GetData() + LEAF_PAGE_HEADER_SIZE);
  return array[index_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    index_++;
    auto page = guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    if (index_ < page->GetSize()) {
        return *this;
    }
    auto next_page_id = page->GetNextPageId();
    if (next_page_id != INVALID_PAGE_ID) {
        this->guard_ = std::move(bpm_->FetchPageRead(next_page_id));
        this->index_ = 0;
    } else {
        this->guard_  = std::move(ReadPageGuard());
        this->index_ = 0;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
