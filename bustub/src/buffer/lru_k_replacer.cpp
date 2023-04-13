//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();

  if (!node_sorted_inf_.empty()) {
    auto it = node_sorted_inf_.begin();
    *frame_id = (**it).FrameID();
    node_sorted_inf_.erase(it);
  } else if (!node_sorted_.empty()) {
    auto it = node_sorted_.begin();
    *frame_id = (**it).FrameID();
    node_sorted_.erase(it);
  } else {
    latch_.unlock();
    return false;
  }

  node_store_.erase(node_store_.find(*frame_id));

  latch_.unlock();

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    latch_.unlock();
    throw Exception("Invalid frame_id");
  }

  auto it = node_store_.find(frame_id);

  if (it == node_store_.end()) {
    auto new_node = std::make_shared<LRUKNode>(k_, frame_id);
    node_store_.insert({frame_id, std::move(new_node)});
    latch_.unlock();
    return;
  }

  if (!it->second->IsEvictable()) {
    it->second->Access();
    latch_.unlock();
    return;
  }

  if (it->second->HistroySize() == k_ - 1) {
    auto begin = node_sorted_inf_.find(it->second);
    while (**begin != *(it->second)) {
      begin++;
    }
    node_sorted_inf_.erase(begin);
    it->second->Access();
    node_sorted_.insert(it->second);
    latch_.unlock();
    return;
  }

  auto is_inf = it->second->HistroySize() < k_ - 1;

  if (is_inf) {
    auto begin = node_sorted_inf_.find(it->second);
    while (**begin != *(it->second)) {
      begin++;
    }
    node_sorted_inf_.erase(begin);
  } else {
    auto begin = node_sorted_.find(it->second);
    while (**begin != *(it->second)) {
      begin++;
    }
    node_sorted_.erase(begin);
  }

  it->second->Access();

  if (is_inf) {
    node_sorted_inf_.insert(it->second);
  } else {
    node_sorted_.insert(it->second);
  }

  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    latch_.unlock();
    throw Exception("Invalid frame_id");
  }

  auto &is_evictable = it->second->IsEvictable();
  if (is_evictable == set_evictable) {
    latch_.unlock();
    return;
  }

  is_evictable = set_evictable;
  if (set_evictable) {
    if (it->second->HistroySize() == k_) {
      node_sorted_.insert(it->second);
    } else {
      node_sorted_inf_.insert(it->second);
    }
  } else {
    if (it->second->HistroySize() == k_) {
      auto begin = node_sorted_.find(it->second);
      while (**begin != *(it->second)) {
        begin++;
      }
      node_sorted_.erase(begin);
    } else {
      auto begin = node_sorted_inf_.find(it->second);
      while (**begin != *(it->second)) {
        begin++;
      }
      node_sorted_inf_.erase(begin);
    }
  }

  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end() || !it->second->IsEvictable()) {
    latch_.unlock();
    throw Exception("Invalid frame_id");
  }

  if (it->second->HistroySize() == k_) {
    auto begin = node_sorted_.find(it->second);
    while (**begin != *(it->second)) {
      begin++;
    }
    node_sorted_.erase(begin);
  } else {
    auto begin = node_sorted_inf_.find(it->second);
    while (**begin != *(it->second)) {
      begin++;
    }
    node_sorted_inf_.erase(begin);
  }
  node_store_.erase(it);

  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  auto ret = node_sorted_.size() + node_sorted_inf_.size();
  latch_.unlock();
  return ret;
}

}  // namespace bustub
