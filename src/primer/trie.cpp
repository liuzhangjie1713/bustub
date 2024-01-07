#include "primer/trie.h"
#include <iostream>
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (root_ == nullptr) {
    return nullptr;
  }

  std::shared_ptr<const TrieNode> cur = root_;
  for (char c : key) {
    if (cur->children_.find(c) == cur->children_.end()) {
      return nullptr;
    }
    cur = cur->children_.at(c);
  }
  if (!cur->is_value_node_) {
    return nullptr;
  }
  auto value_node = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cur);
  if (value_node == nullptr) {
    return nullptr;
  }
  return value_node->value_.get();

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // 我们在插入节点时，并非是在原Trie上插入节点，而是在搜索要插入节点的路径上不断 Clone( )，
  // 只有这样，我们才能得到非const的节点来对其children_进行操作。
  // 使用多个指针进行维护，初始时两个指针都指向父节点，然后令一个指针下探进行操作，另一个指针不变，等操作完后更新父节点，再下探。

  auto value_ptr = std::make_shared<T>(std::move(value));

  // 克隆原来的根节点
  std::shared_ptr<TrieNode> new_root;
  if (root_ == nullptr) {
    new_root = std::make_shared<TrieNode>();
  } else {
    new_root = root_->Clone();
  }

  // 如果key为空，则直接在根节点上插入值，并继承原来的子节点
  if (key.empty()) {
    auto new_trie = std::make_shared<Trie>();
    new_trie->root_ = std::make_shared<TrieNodeWithValue<T>>(new_root->children_, value_ptr);
    return *new_trie;
  }

  std::shared_ptr<TrieNode> parent = new_root;
  std::shared_ptr<TrieNode> child = nullptr;
  // 遍历到key的倒数第二个字母,在搜索路径上不断克隆新的节点
  auto it = key.begin();
  for (; it != key.end() && std::next(it) != key.end(); it++) {
    if (parent->children_.find(*it) == parent->children_.end()) {
      child = std::make_shared<TrieNode>();
    } else {
      child = parent->children_.at(*it)->Clone();
    }
    parent->children_[*it] = child;
    parent = child;
  }

  // 如果key的最后一个字母在搜索路径上不存在，则新建一个节点
  if (parent->children_.find(*it) == parent->children_.end()) {
    parent->children_[*it] = std::make_shared<TrieNodeWithValue<T>>(value_ptr);
  } else {
    auto child_node = parent->children_.at(*it);
    parent->children_[*it] = std::make_shared<TrieNodeWithValue<T>>(child_node->children_, value_ptr);
  }

  auto new_trie = std::make_shared<Trie>();
  new_trie->root_ = new_root;
  return *new_trie;

  // throw NotImplementedException("Trie::Put is not implemented.");

  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  auto new_trie = std::make_shared<Trie>();
  // 遍历原树，将查找路径上的节点克隆新节点并记录到path中
  std::vector<std::shared_ptr<TrieNode>> path;
  std::shared_ptr<TrieNode> cur = root_->Clone();
  path.push_back(cur);
  for (char c : key) {
    cur = cur->children_.at(c)->Clone();
    if (c == key.back()) {
      // 删除最后一个节点的值
      cur = std::make_shared<TrieNode>(cur->children_);
      cur->is_value_node_ = false;
    }
    path.push_back(cur);
  }

  // 遍历path中的节点从底向上建树
  // 如果一个节点不是value节点且没有子节点，则在父节点的children_中删除该节点, 否则，将该节点插入到父节点的children_中
  for (int i = path.size() - 1; i > 0; i--) {
    auto node = path[i];
    if (!node->is_value_node_ && node->children_.empty()) {
      path[i - 1]->children_.erase(key[i - 1]);
    } else {
      path[i - 1]->children_[key[i - 1]] = node;
    }
  }

  new_trie->root_ = path[0];
  return *new_trie;

  // throw NotImplementedException("Trie::Remove is not implemented.");

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
