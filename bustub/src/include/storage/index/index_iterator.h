//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  explicit IndexIterator(ReadPageGuard &&guard, BufferPoolManager *bpm, int index = 0)
      : guard_(std::move(guard)), bpm_(bpm), index_(index){};
  ~IndexIterator();  // NOLINT

  IndexIterator(IndexIterator&&) noexcept ;

  auto operator=(IndexIterator&&) noexcept -> IndexIterator&; 

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return guard_ == itr.guard_ && index_ == itr.index_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

 private:
  // add your own private member variables here
  ReadPageGuard guard_{};
  BufferPoolManager *bpm_{};
  int index_{};
};

}  // namespace bustub
