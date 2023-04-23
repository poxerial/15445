//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size_, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();

  frame_id_t fid;
  auto is_evict = false;
  if (!free_list_.empty()) {
    *page_id = AllocatePage();
    fid = free_list_.back();
    free_list_.pop_back();
    page_table_[*page_id] = fid;
    latch_.unlock();
  } else if (replacer_->Evict(&fid)) {
    *page_id = AllocatePage();
    is_evict = true;
    auto old_page_id = pages_[fid].GetPageId();
    page_table_.erase(old_page_id);
    page_table_[*page_id] = fid;
    latch_.unlock();
  } else {
    latch_.unlock();
    return nullptr;
  }

  auto page = pages_ + fid;

  page->WLatch();
  if (is_evict && page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  page->ResetMemory();
  page->pin_count_ = 1;
  page->page_id_ = *page_id;
  page->is_dirty_ = true;
  page->WUnlatch();

  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  return pages_ + fid;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();

  auto it = page_table_.find(page_id);
  frame_id_t fid;
  auto is_evict = false;
  if (it == page_table_.end()) {
    if (!free_list_.empty()) {
      fid = free_list_.front();
      free_list_.pop_front();
    } else if (replacer_->Evict(&fid)) {
      auto old_page_id = pages_[fid].GetPageId();
      page_table_.erase(old_page_id);
      is_evict = true;
    } else {
      latch_.unlock();
      return nullptr;
    }
    page_table_.insert({page_id, fid});
    latch_.unlock();
  } else {
    fid = it->second;
    latch_.unlock();
  }

  auto *page = pages_ + fid;

  page->WLatch();
  if (is_evict && page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = true;
  disk_manager_->ReadPage(page_id, page->GetData());
  page->WUnlatch();

  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  auto fid = it->second;
  latch_.unlock();

  auto& page = pages_[fid];
  page.WLatch();
  auto new_pin_count = --page.pin_count_;
  if (new_pin_count <= -1) {
    page.WUnlatch();
    return false;
  } 
  page.is_dirty_ |= is_dirty;
  page.WUnlatch();
  
  if (new_pin_count == 0) {
    replacer_->SetEvictable(fid, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  auto fid = it->second;
  latch_.unlock();

  auto& page = pages_[fid];
  page.WLatch();
  if (page.IsDirty()) {
    disk_manager_->WritePage(page_id, page.GetData());
    page.is_dirty_ = false;
  }
  page.WUnlatch();

  return true;
}

void BufferPoolManager::FlushAllPages() {
  auto pages = std::list<frame_id_t>{};

  latch_.lock();
  for (const auto& p : page_table_) {
    pages.push_back(p.second);
  }
  latch_.unlock();

  for (const auto& fid : pages) {
    auto& page = pages_[fid];
    page.WLatch();
    if (page.IsDirty()) {
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
      page.is_dirty_ = false;
    }
    page.WUnlatch();
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }

  auto fid = it->second;
  auto& page = pages_[fid];
  page.WLatch();
  if (page.GetPinCount() > 0) {
    page.WUnlatch();
    latch_.unlock();
    return false;
  }
  if (page.IsDirty()) {
    disk_manager_->WritePage(page_id, page.GetData());
  }
  page.page_id_ = INVALID_PAGE_ID;
  page.is_dirty_ = false;
  page.pin_count_ = 0;
  page.ResetMemory();
  page.WUnlatch();

  page_table_.erase(it);
  replacer_->Remove(fid);
  free_list_.push_back(fid);

  DeallocatePage(page_id);

  latch_.unlock();

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
