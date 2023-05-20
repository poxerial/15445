#include <iterator>
#include <sstream>
#include <string>
#include <type_traits>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/generic_key.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

#undef B_PLUS_TREE_INTERNAL_PAGE_TYPE
#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Search the coresponding index to `key` in page `p`
 *
 * @tparam PageType
 * @tparam KeyType
 * @tparam KeyComparator
 * @param p the page to search in
 * @param key the key to search for
 * @param comparator the comparator like std::less
 * @return the coresponding index to `key` in page `p`
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearch(const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *p, KeyType key,
                                  const KeyComparator &comparator) -> int {
  auto r = p->GetSize() - 1;
  auto l = 1;
  while (l <= r) {
    auto mid = (r + l) / 2;
    auto comp = comparator(p->KeyAt(mid), key);
    if (comp == Less) {
      l = mid + 1;
    } else if (comp == Greater) {
      r = mid - 1;
    } else {
      r = mid;
      break;
    }
  }
  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearch(const B_PLUS_TREE_LEAF_PAGE_TYPE *p, KeyType key, const KeyComparator &comparator)
    -> int {
  auto r = p->GetSize() - 1;
  auto l = 1;
  while (l <= r) {
    auto mid = (r + l) / 2;
    auto comp = comparator(p->KeyAt(mid), key);
    if (comp == Less) {
      l = mid + 1;
    } else if (comp == Greater) {
      r = mid - 1;
    } else {
      r = mid;
      break;
    }
  }
  return r;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  if (IsEmpty()) {
    return false;
  }

  auto guard = bpm_->FetchPageRead(GetRootPageId());

  // find leaf page
  while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
    auto p = guard.template As<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();

    auto index = BinarySearch(p, key, comparator_);

    guard = bpm_->FetchPageRead(p->ValueAt(index));
  }

  auto p = guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();

  auto index = BinarySearch(p, key, comparator_);

  if (comparator_(p->KeyAt(index), key) != Equal) {
    return false;
  }

  result->push_back(reinterpret_cast<const MappingType *>(guard.GetData() + LEAF_PAGE_HEADER_SIZE)[index].second);

  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

/**
 * @brief Insert to Array by iteratively copying the value with larger index to
 * the next position and finally assign the `val` to `array[index]`, which is because
 * std::pair cannot be trivially copied.
 *
 * @tparam ArrayType
 * @param index
 * @param array_size
 * @param array
 * @param val
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertToArray(int index, int array_size, MappingType *array, MappingType &&val) {
  for (auto i = array_size - 1; i >= index; i--) {
    array[i + 1] = array[i];
  }
  array[index] = val;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertToArray(int index, int array_size, std::pair<KeyType, page_id_t> *array,
                                   std::pair<KeyType, page_id_t> &&val) {
  for (auto i = array_size - 1; i >= index; i--) {
    array[i + 1] = array[i];
  }
  array[index] = val;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CreateNewRootPage(KeyType key_to_be_insert, page_id_t page_id_to_be_insert,
                                       page_id_t curr_page_id) {
  page_id_t new_root_page_id;
  auto new_root_page_guard = bpm_->NewPageGuarded(&new_root_page_id);
  auto new_root_page = new_root_page_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  new_root_page->Init(internal_max_size_);
  new_root_page->SetSize(2);
  auto new_root_page_array =
      reinterpret_cast<std::pair<KeyType, page_id_t> *>(new_root_page_guard.GetDataMut() + INTERNAL_PAGE_HEADER_SIZE);
  new_root_page_array[1] = std::pair<KeyType, page_id_t>{key_to_be_insert, page_id_to_be_insert};
  new_root_page_array[0] = std::pair<KeyType, page_id_t>{{}, curr_page_id};

  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = new_root_page_id;
}

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  if (IsEmpty()) {
    page_id_t root_page_id;
    auto root_page_guard = bpm_->NewPageGuarded(&root_page_id);
    auto root_page = root_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    root_page->Init(leaf_max_size_);
    root_page->SetSize(1);

    reinterpret_cast<MappingType *>(root_page_guard.GetDataMut() + LEAF_PAGE_HEADER_SIZE)[0] = MappingType{key, value};

    root_page->SetNextPageId(INVALID_PAGE_ID);

    auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id;

    return true;
  }

  // Declaration of context instance.
  Context ctx;

  auto guard = bpm_->FetchPageWrite(GetRootPageId());

  // find leaf page
  while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
    auto page = guard.template As<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();

    auto index = BinarySearch(page, key, comparator_);

    if (page->GetSize() < page->GetMaxSize()) {
      for (auto it = ctx.write_set_.begin(); it != ctx.write_set_.end(); it = ctx.write_set_.erase(it)) {}
    }

    ctx.write_set_.push_back(std::move(guard));

    guard = bpm_->FetchPageWrite(page->ValueAt(index));
  }

  auto page = guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();

  auto is_full = page->GetSize() == page->GetMaxSize();
  if (!is_full) {
    (void)ctx;
  }

  auto index = BinarySearch(page, key, comparator_);

  if (comparator_(page->KeyAt(index), key) == Equal) {
    (void)ctx;
    return false;
  }

  if (comparator_(page->KeyAt(index), key) == Less) {
    index++;
  }

  if (!is_full) {
    auto array = reinterpret_cast<MappingType *>(guard.GetDataMut() + LEAF_PAGE_HEADER_SIZE);
    InsertToArray(index, page->GetSize(), array, {key, value});
    page->IncreaseSize(1);
    return true;
  }

  page_id_t new_page_id;
  auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);

  auto new_page = new_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  new_page->Init(leaf_max_size_);

  auto array = reinterpret_cast<MappingType *>(guard.GetDataMut() + LEAF_PAGE_HEADER_SIZE);
  auto new_array = reinterpret_cast<MappingType *>(new_page_guard.GetDataMut() + LEAF_PAGE_HEADER_SIZE);
  auto size = (page->GetSize() / 2) * sizeof(decltype(*array));
  auto src = array + page->GetSize() - page->GetSize() / 2;

  for (decltype(size) i = 0; i < size; i++) {
    new_array[i] = src[i];
  }

  page->SetSize(page->GetSize() - page->GetSize() / 2);
  new_page->SetNextPageId(page->GetNextPageId());
  page->SetNextPageId(new_page_id);

  int array_size;
  MappingType *array_to_insert_to;
  if (index >= page->GetSize()) {
    index -= page->GetSize();
    array_size = new_page->GetSize();
    array_to_insert_to = new_array;
    new_page->IncreaseSize(1);
  } else {
    array_size = page->GetSize();
    array_to_insert_to = array;
    page->IncreaseSize(1);
  }

  InsertToArray(index, array_size, array_to_insert_to, {key, value});

  auto key_to_be_insert = new_array[0].first;
  auto page_id_to_be_insert = new_page_id;
  auto curr_page_id = guard.PageId();
  WritePageGuard father_page_guard;
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *father_page;
  while (!ctx.write_set_.empty()) {
    father_page_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    father_page = father_page_guard.AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();

    if (father_page->GetSize() < father_page->GetMaxSize()) {
      index = BinarySearch(father_page, key_to_be_insert, comparator_);
      auto array_to_be_insert =
          reinterpret_cast<std::pair<KeyType, page_id_t> *>(father_page_guard.GetDataMut() + INTERNAL_PAGE_HEADER_SIZE);
      if (comparator_(array_to_be_insert[index].first, key_to_be_insert) == Less) {
        index++;
      }

      InsertToArray(index, father_page->GetSize(), array_to_be_insert, {key_to_be_insert, page_id_to_be_insert});

      father_page->IncreaseSize(1);

      (void)ctx;
      return true;
    }

    page_id_t new_father_page_id;
    auto new_father_page_guard = bpm_->NewPageGuarded(&new_father_page_id);
    auto new_father_page = new_father_page_guard.AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    new_father_page->Init(internal_max_size_);

    auto new_page_size = internal_max_size_ / 2;
    auto split_page_size = internal_max_size_ - new_page_size;

    auto new_page_array = reinterpret_cast<std::pair<KeyType, page_id_t> *>(new_father_page_guard.GetDataMut() +
                                                                            INTERNAL_PAGE_HEADER_SIZE);
    auto split_page_array =
        reinterpret_cast<std::pair<KeyType, page_id_t> *>(father_page_guard.GetDataMut() + INTERNAL_PAGE_HEADER_SIZE);

    for (auto i = 0; i < new_page_size; i++) {
      new_page_array[i] = split_page_array[split_page_size + i];
    }

    auto index = BinarySearch(father_page, key_to_be_insert, comparator_);
    if (comparator_(split_page_array[index].first, key_to_be_insert) == Less) {
      index++;
    }

    new_father_page->SetSize(new_page_size);
    father_page->SetSize(split_page_size);

    std::pair<KeyType, page_id_t> *array;
    int array_size;
    if (index < split_page_size) {
      father_page->IncreaseSize(1);
      array = split_page_array;
      array_size = split_page_size;
    } else {
      new_father_page->IncreaseSize(1);
      index -= split_page_size;
      array = new_page_array;
      array_size = new_page_size;
    }

    InsertToArray(index, array_size, array, {key_to_be_insert, page_id_to_be_insert});

    key_to_be_insert = new_page_array[0].first;
    page_id_to_be_insert = new_father_page_id;
    curr_page_id = father_page_guard.PageId();
  }

  // create new root internal page
  CreateNewRootPage(key_to_be_insert, page_id_to_be_insert, curr_page_id);
  (void)ctx;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

/**
 * @brief Remove from Array by iteratively copying the value with larger index to
 * the next position, which is because std::pair cannot be trivially copied.
 *
 * @tparam ArrayType
 * @param index
 * @param array_size
 * @param array
 * @param val
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromArray(int index, int array_size, MappingType *array) {
  for (auto i = index + 1; i < array_size; i++) {
    array[i - 1] = array[i];
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromArray(int index, int array_size, std::pair<KeyType, page_id_t> *array) {
  for (auto i = index + 1; i < array_size; i++) {
    array[i - 1] = array[i];
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
    if (IsEmpty()) {
      return;
    }

    // Declaration of context instance.
    Context ctx;

    auto root_page_id = GetRootPageId();
    auto guard = bpm_->FetchPageWrite(root_page_id);
    // find leaf page
    while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
      auto page = guard.template As<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
      auto index = BinarySearch(page, key, comparator_);

      if (page->GetSize() > page->GetMinSize()) {
        for (auto it = ctx.write_set_.begin(); it != ctx.write_set_.end(); it = ctx.write_set_.erase(it)) {}
      }

      ctx.write_set_.push_back(std::move(guard));

      guard = bpm_->FetchPageWrite(page->ValueAt(index));
    }

    auto page = guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();

    if (page->GetSize() > page->GetMinSize()) {
      (void)ctx;
    }

    auto index = BinarySearch(page, key, comparator_);
    if (comparator_(page->KeyAt(index), key) != Equal) {
      return;
    }
    auto array = reinterpret_cast<MappingType *>(guard.GetDataMut() + LEAF_PAGE_HEADER_SIZE);
    RemoveFromArray(index, page->GetSize(), array);
    page->SetSize(page->GetSize() - 1);

    if (page->GetSize() >= page->GetMinSize()) {
      return;
    }

    if (guard.PageId() == root_page_id) {
      if (page->GetSize() == 0) {
        auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
        auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
        auto root_page_id = header_page->root_page_id_;
        header_page->root_page_id_ = INVALID_PAGE_ID;
        guard.Drop();
        bpm_->DeletePage(root_page_id);
      }
      return;
    }

    auto father_page = ctx.write_set_.back().AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
    auto father_page_array =
        reinterpret_cast<std::pair<KeyType, page_id_t> *>(ctx.write_set_.back().GetDataMut() +
        INTERNAL_PAGE_HEADER_SIZE);
    index = BinarySearch(father_page, key, comparator_);
    auto left_page_id = father_page_array[index - 1].second;
    auto right_page_id = father_page_array[index + 1].second;
    if (index < father_page->GetSize() - 1) {
      auto right_page_guard = bpm_->FetchPageWrite(right_page_id);
      auto right_page = right_page_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      auto right_array = reinterpret_cast<MappingType *>(right_page_guard.GetDataMut() + LEAF_PAGE_HEADER_SIZE);
      if (right_page->GetSize() > right_page->GetMinSize()) {
        array[page->GetSize()] = right_array[0];
        RemoveFromArray(0, right_page->GetSize(), right_array);
        father_page_array[index + 1].first = right_array[0].first;
        right_page->SetSize(right_page->GetSize() - 1);
        page->IncreaseSize(1);
        return;
      }
      right_page_guard.Drop();
    }
    if (index > 0) {
      auto left_page_guard = bpm_->FetchPageWrite(left_page_id);
      auto left_page = left_page_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      auto left_array = reinterpret_cast<MappingType *>(left_page_guard.GetDataMut() + LEAF_PAGE_HEADER_SIZE);
      if (left_page->GetSize() > left_page->GetMinSize()) {
        auto val = left_array[left_page->GetSize() - 1];
        InsertToArray(0, page->GetSize(), array, {val.first, val.second});
        father_page_array[index].first = array[0].first;
        left_page->SetSize(left_page->GetSize() - 1);
        page->IncreaseSize(1);
        return;
      }
      left_page_guard.Drop();
    }
    // merge two leaf nodes
    auto merge_index = index < father_page->GetSize() - 1 ? index + 1 : index - 1;
    auto merge_page_id = index < father_page->GetSize() - 1 ? right_page_id : left_page_id;
    auto merge_guard = bpm_->FetchPageWrite(merge_page_id);
    auto merge_page = merge_guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    auto merge_array = reinterpret_cast<MappingType *>(merge_guard.GetDataMut() + LEAF_PAGE_HEADER_SIZE);
    auto merge_size = merge_page->GetSize();

    auto size = page->GetSize();
    for (decltype(size) i = 0; i < merge_size; i++) {
      array[i + size] = merge_array[i];
    }

    page->SetSize(size + merge_size);
    auto success = bpm_->DeletePage(merge_page_id);

    // delete must be success because we hold the write lock
    assert(success);

    RemoveFromArray(merge_index, father_page->GetSize(), father_page_array);
    father_page->SetSize(father_page->GetSize() - 1);
    auto curr_page = father_page;
    while (curr_page->GetSize() < curr_page->GetMinSize()) {
      auto curr_array = reinterpret_cast<std::pair<KeyType, page_id_t> *>(ctx.write_set_.back().GetDataMut());
      if (ctx.write_set_.empty()) {
        if (curr_page->GetSize() > 1) {
          return;
        }
        auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
        auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
        auto root_page_id = header_page->root_page_id_;
        header_page->root_page_id_ = curr_array[0].second;
        header_page_guard.Drop();
        (void)ctx;
        bpm_->DeletePage(root_page_id);
        return;
      }
      ctx.write_set_.pop_back();
      auto father_page = ctx.write_set_.back().AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
      auto father_array =
          reinterpret_cast<std::pair<KeyType, page_id_t> *>(ctx.write_set_.back().GetDataMut() + INTERNAL_PAGE_HEADER_SIZE);
      index = BinarySearch(father_page, key, comparator_);
      auto left_page_id = father_array[index - 1].second;
      auto right_page_id = father_array[index + 1].second;
      if (index < father_page->GetSize() - 1) {
        auto right_page_guard = bpm_->FetchPageWrite(right_page_id);
        auto right_page = right_page_guard.template AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
        auto right_array =
            reinterpret_cast<std::pair<KeyType, page_id_t> *>(right_page_guard.GetDataMut() +
            INTERNAL_PAGE_HEADER_SIZE);
        if (right_page->GetSize() > right_page->GetMinSize()) {
          curr_array[page->GetSize()] = right_array[0];
          RemoveFromArray(0, right_page->GetSize(), right_array);
          father_array[index + 1].first = right_array[0].first;
          right_page->SetSize(right_page->GetSize() - 1);
          page->IncreaseSize(1);
          return;
        }
        right_page_guard.Drop();
      }
      if (index > 0) {
        auto left_page_guard = bpm_->FetchPageWrite(left_page_id);
        auto left_page = left_page_guard.template AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
        auto left_array =
            reinterpret_cast<std::pair<KeyType, page_id_t> *>(left_page_guard.GetDataMut() +
            INTERNAL_PAGE_HEADER_SIZE);
        if (left_page->GetSize() > left_page->GetMinSize()) {
          auto val = left_array[left_page->GetSize() - 1];
          InsertToArray(0, page->GetSize(), curr_array, {val.first, val.second});
          father_array[index].first = array[0].first;
          left_page->SetSize(left_page->GetSize() - 1);
          page->IncreaseSize(1);
          return;
        }
        left_page_guard.Drop();
      }
      // merge two internal nodes
      auto merge_index = index < father_page->GetSize() - 1 ? index + 1 : index - 1;
      auto merge_page_id = index < father_page->GetSize() - 1 ? right_page_id : left_page_id;
      auto merge_guard = bpm_->FetchPageWrite(merge_page_id);
      auto merge_page = merge_guard.template As<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
      auto merge_array =
          reinterpret_cast<std::pair<KeyType, page_id_t> *>(merge_guard.GetDataMut() + INTERNAL_PAGE_HEADER_SIZE);
      auto merge_size = merge_page->GetSize();
      auto size = page->GetSize();
      for (decltype(size) i = 0; i < merge_size; i++) {
        curr_array[i + size] = merge_array[i];
      }
      curr_page->SetSize(size + merge_size);
      merge_guard.Drop();
      bpm_->DeletePage(merge_page_id);
      RemoveFromArray(merge_index, father_page->GetSize(), father_page_array);
      father_page->SetSize(father_page->GetSize() - 1);
      curr_page = father_page;
    }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }

  auto root_page_id = GetRootPageId();
  auto guard = bpm_->FetchPageRead(root_page_id);
  while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
    auto array = reinterpret_cast<const std::pair<KeyType, page_id_t> *>(guard.GetData() + INTERNAL_PAGE_HEADER_SIZE);
    auto next_page_id = array[0].second;
    guard = std::move(bpm_->FetchPageRead(next_page_id));
  }
  return INDEXITERATOR_TYPE(std::move(guard), bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }

  auto guard = bpm_->FetchPageRead(GetRootPageId());

  // find leaf page
  while (!guard.template As<BPlusTreePage>()->IsLeafPage()) {
    auto p = guard.template As<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();

    auto index = BinarySearch(p, key, comparator_);

    guard = bpm_->FetchPageRead(p->ValueAt(index));
  }

  auto p = guard.template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();

  auto index = BinarySearch(p, key, comparator_);

  if (comparator_(p->KeyAt(index), key) != Equal) {
    return INDEXITERATOR_TYPE();
  }

  return INDEXITERATOR_TYPE(std::move(guard), bpm_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
//
}  // namespace bustub
