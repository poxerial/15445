#include "primer/trie.h"
#include <cstddef>
#include <memory>
#include <stack>
#include <stdexcept>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (!root_) {
    return nullptr;
  }
  auto curr = root_;
  for (const auto &ch : key) {
    auto it = curr->children_.find(ch);
    if (it == curr->children_.end()) {
      return nullptr;
    }
    curr = it->second;
  }

  if (curr->is_value_node_) {
    auto p = dynamic_cast<const TrieNodeWithValue<T> *>(curr.get());
    if (p == nullptr) {
      return nullptr;
    }
    return p->value_.get();
  }
  return nullptr;
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  if (key.empty()) {
    if (root_) {
      auto m = std::map<char, std::shared_ptr<const TrieNode>>(root_->children_.cbegin(), root_->children_.cend());
      auto root = std::make_shared<TrieNodeWithValue<T>>(m, std::make_shared<T>(std::move(value)));
      return Trie(root);
    }
    return Trie(std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value))));
  }

  size_t i = 0;

  std::shared_ptr<TrieNode> root;
  std::shared_ptr<TrieNode> ret_curr;

  auto fix_tail = [&]() {
    for (; i < key.size() - 1; i++) {
      auto next = std::make_shared<TrieNode>();
      ret_curr->children_[key[i]] = next;
      ret_curr = next;
    }
    ret_curr->children_[key[key.size() - 1]] =
        std::make_shared<TrieNodeWithValue<T>>(decltype(ret_curr->children_){}, std::make_shared<T>(std::move(value)));
  };

  if (!root_) {
    root = std::make_shared<TrieNode>();
    ret_curr = root;
    fix_tail();
    return Trie(root);
  }
  root = std::shared_ptr<TrieNode>(root_->Clone());

  auto curr = root_;
  ret_curr = root;

  for (; i < key.size() - 1; i++) {
    auto it = curr->children_.find(key[i]);
    if (it == curr->children_.end()) {
      fix_tail();
      return Trie(root);
    }
    curr = it->second;
    auto copied = std::shared_ptr(curr->Clone());
    ret_curr->children_[key[i]] = copied;
    ret_curr = copied;
  }
  auto it = curr->children_.find(key[key.size() - 1]);
  auto m = std::map<char, std::shared_ptr<const TrieNode>>{};
  if (it != curr->children_.end()) {
    m = std::map<char, std::shared_ptr<const TrieNode>>(it->second->children_.cbegin(), it->second->children_.cend());
  }
  ret_curr->children_[key[key.size() - 1]] =
      std::make_shared<TrieNodeWithValue<T>>(m, std::make_shared<T>(std::move(value)));
  return Trie(root);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

// template <typename T>
// auto NextRetCurr(const char &ch, const std::shared_ptr<TrieNodeWithValue<T>> &next, std::shared_ptr<TrieNode>
// &ret_curr)
//     -> std::shared_ptr<TrieNode> {
//   auto copied_next = std::make_shared<TrieNodeWithValue<T>>(next->Clone());
//   ret_curr->children_[ch] = copied_next;
//   return copied_next;
// }

// auto NextRetCurr(const char &ch, const std::shared_ptr<TrieNode> &next, std::shared_ptr<TrieNode> &ret_curr)
//     -> std::shared_ptr<TrieNode> {
//   auto copied_next = std::make_shared<TrieNode>(next->Clone());
//   ret_curr->children_[ch] = copied_next;
//   return copied_next;
// }

auto Trie::Remove(std::string_view key) const -> Trie {
  if (!root_) {
    return *this;
  }
  auto curr = root_;
  auto deepest_node = root_->children_.size() > 1 ? root_ : decltype(root_){};
  for (size_t i = 0; i < key.size(); i++) {
    auto ch = key[i];
    auto it = curr->children_.find(ch);
    if (it == curr->children_.end()) {
      return *this;
    }
    curr = it->second;
    if (curr->children_.size() > 1 || (curr->is_value_node_ && i != key.size() - 1)) {
      deepest_node = curr;
    }
  }

  if (key.empty()) {
    auto root = std::make_shared<TrieNode>(
        std::map<char, std::shared_ptr<const TrieNode>>(root_->children_.cbegin(), root_->children_.cend()));
    return Trie(root);
  }

  if (!deepest_node && curr->children_.empty()) {
    return {};
  }

  auto last = curr;
  if (!last->children_.empty()) {
    deepest_node = last;
  }

  auto ret_curr = std::shared_ptr<TrieNode>(root_->Clone());
  auto ret = Trie(ret_curr);
  size_t i = 0;
  if (root_ != deepest_node) {
    curr = root_;
    for (; i < key.size(); i++) {
      const auto ch = key[i];
      curr = curr->children_.at(ch);
      if (curr == deepest_node) {
        break;
      }
      auto tmp = std::shared_ptr<TrieNode>(curr->Clone());
      ret_curr->children_[ch] = tmp;
      ret_curr = tmp;
    }
  }

  if (!last->children_.empty()) {
    auto tmp = std::make_shared<TrieNode>(
        std::map<char, std::shared_ptr<const TrieNode>>(curr->children_.cbegin(), curr->children_.cend()));
    ret_curr->children_[key[i]] = tmp;
    return ret;
  }

  auto tmp = std::shared_ptr<TrieNode>(curr->Clone());
  tmp->children_.erase(key[i + 1]);
  ret_curr->children_[key[i]] = tmp;

  return ret;
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub

auto i = sizeof(std::string_view);
