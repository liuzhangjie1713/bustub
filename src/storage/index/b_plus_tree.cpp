#define LOG_LEVEL LOG_LEVEL_DEBUG

#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

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
  LOG_INFO("new b+tree %s, leaf_max_size %d, internal_max_size %d", index_name_.c_str(), leaf_max_size_,
           internal_max_size_);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->FetchPageWrite(header_page_id_);
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
  // std::cout << std::this_thread::get_id() << std::endl;
  // LOG_DEBUG("Get value %s\n", std::to_string(key.ToString()).c_str());
  auto head_guard = bpm_->FetchPageRead(header_page_id_);
  auto head_page = head_guard.As<BPlusTreeHeaderPage>();
  if (head_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto root_guard = bpm_->FetchPageRead(head_page->root_page_id_);
  auto root_page = root_guard.As<BPlusTreePage>();
  head_guard.Drop();
  if (root_page->GetSize() == 0) {
    return false;
  }

  auto cur_guard = std::move(root_guard);
  auto cur_page = cur_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    page_id_t next_page_id = INVALID_PAGE_ID;
    FindNextPage(cur_guard, key, comparator_, next_page_id);
    auto next_guard = bpm_->FetchPageRead(next_page_id);
    cur_guard.Drop();
    cur_guard = std::move(next_guard);
    cur_page = cur_guard.As<BPlusTreePage>();
  }
  return FindValue(cur_guard, key, result, comparator_);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  auto head_guard = bpm_->FetchPageWrite(header_page_id_);
  auto head_page = head_guard.AsMut<BPlusTreeHeaderPage>();
  // if current tree is empty, start new tree, update root page id
  if (head_page->root_page_id_ == INVALID_PAGE_ID) {
    // create the root node
    page_id_t root_page_id = INVALID_PAGE_ID;
    auto root_guard = bpm_->NewPageGuarded(&root_page_id);
    auto root_page = root_guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    head_page->root_page_id_ = root_page_id;

    head_guard.Drop();
    root_guard.Drop();
    return Insert(key, value, txn);
  }

  Context ctx;
  ctx.root_page_id_ = head_page->root_page_id_;
  ctx.header_page_ = std::move(head_guard);
  // std::cout << std::this_thread::get_id() << std::endl;
  // LOG_DEBUG("Insert key: %s", std::to_string(key.ToString()).c_str());
  // 如果插入的key是4377，打印出来
  if (key.ToString() == 4377 || key.ToString() == 4378) {
    std::cout << "Insert key: " << key.ToString() << std::endl;
  }
  return Insert(ctx.root_page_id_, key, value, ctx);
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
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // std::cout << std::this_thread::get_id() << std::endl;
  // LOG_DEBUG("Remove key %s", std::to_string(key.ToString()).c_str());
  if (key.ToString() == 4377 || key.ToString() == 4378) {
    std::cout << "Remove key: " << key.ToString() << std::endl;
  }
  auto head_guard = bpm_->FetchPageWrite(header_page_id_);
  auto head_page = head_guard.AsMut<BPlusTreeHeaderPage>();

  // if current tree is empty, return immediately
  if (head_page->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }

  Context ctx;
  ctx.root_page_id_ = head_page->root_page_id_;
  ctx.header_page_ = std::move(head_guard);

  Remove(head_page->root_page_id_, key, ctx, -1);

  ctx.header_page_ = std::nullopt;
  ctx.root_page_id_ = INVALID_PAGE_ID;
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
  LOG_DEBUG("Iterator Begin");
  auto head_guard = bpm_->FetchPageRead(header_page_id_);
  auto head_page = head_guard.As<BPlusTreeHeaderPage>();
  if (head_page->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }

  auto root_guard = bpm_->FetchPageRead(head_page->root_page_id_);
  auto root_page = root_guard.As<BPlusTreePage>();
  head_guard.Drop();
  if (root_page->GetSize() == 0) {
    return End();
  }

  auto cur_guard = std::move(root_guard);
  auto cur_page = cur_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    // page_id_t next_page_id = INVALID_PAGE_ID;
    // FindNextPage(cur_guard, KeyType(), comparator_, next_page_id);
    auto cur_internal_page = cur_guard.As<InternalPage>();
    page_id_t next_page_id = cur_internal_page->ValueAt(0);
    auto next_guard = bpm_->FetchPageRead(next_page_id);
    cur_guard = std::move(next_guard);
    cur_page = cur_guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(bpm_, cur_guard.PageId(), std::move(cur_guard), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  LOG_DEBUG("Iterator Begin key: %s", std::to_string(key.ToString()).c_str());
  auto head_guard = bpm_->FetchPageRead(header_page_id_);
  auto head_page = head_guard.As<BPlusTreeHeaderPage>();
  if (head_page->root_page_id_ == INVALID_PAGE_ID) {
    return End();
  }

  auto root_guard = bpm_->FetchPageRead(head_page->root_page_id_);
  auto root_page = root_guard.As<BPlusTreePage>();
  head_guard.Drop();
  if (root_page->GetSize() == 0) {
    return End();
  }

  auto cur_guard = std::move(root_guard);
  auto cur_page = cur_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    page_id_t next_page_id = INVALID_PAGE_ID;
    FindNextPage(cur_guard, key, comparator_, next_page_id);
    auto next_guard = bpm_->FetchPageRead(next_page_id);
    cur_guard = std::move(next_guard);
    cur_page = cur_guard.As<BPlusTreePage>();
  }
  auto cur_leaf_page = cur_guard.As<LeafPage>();
  int left = 0;
  int right = cur_leaf_page->GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator_(cur_leaf_page->KeyAt(mid), key);
    if (cmp < 0) {
      left = mid + 1;
    } else if (cmp > 0) {
      right = mid - 1;
    } else {
      return INDEXITERATOR_TYPE(bpm_, cur_guard.PageId(), std::move(cur_guard), mid);
    }
  }
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE{bpm_, INVALID_PAGE_ID, {}, 0}; }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto head_guard = bpm_->FetchPageWrite(header_page_id_);
  auto head_page = head_guard.As<BPlusTreeHeaderPage>();
  return head_page->root_page_id_;
}

/*****************************************************************************
 * HELPER METHODS FOR SEARCH
 *****************************************************************************/
/*
 * Find the next page associated with input key in the internal page
 * @return : parent index of the next page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindNextPage(ReadPageGuard &guard, const KeyType &key, const KeyComparator &comparator,
                                  page_id_t &next_page_id) -> int {
  // Binary search to find index of the largest key in the internal page that is less than or equal to the search key
  auto internal_page = guard.As<InternalPage>();
  int left = 1;
  int right = internal_page->GetSize() - 1;
  int index = 0;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator(internal_page->KeyAt(mid), key);
    if (cmp <= 0) {
      index = mid;
      if (cmp == 0) {
        break;
      }
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  next_page_id = internal_page->ValueAt(index);
  return index;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindNextPage(WritePageGuard &guard, const KeyType &key, const KeyComparator &comparator,
                                  page_id_t &next_page_id) -> int {
  // Binary search to find index of the largest key in the internal page that is less than or equal to the search key
  auto internal_page = guard.As<InternalPage>();
  int left = 1;
  int right = internal_page->GetSize() - 1;
  int index = 0;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator(internal_page->KeyAt(mid), key);
    if (cmp <= 0) {
      index = mid;
      if (cmp == 0) {
        break;
      }
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  next_page_id = internal_page->ValueAt(index);
  return index;
}

/*
 * Find the value associated with input key in the leaf page
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindValue(ReadPageGuard &guard, const KeyType &key, std::vector<ValueType> *result,
                               const KeyComparator &comparator) -> bool {
  auto leaf_page = guard.As<LeafPage>();
  int left = 0;
  int right = leaf_page->GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator(leaf_page->KeyAt(mid), key);
    if (cmp == 0) {
      result->push_back(leaf_page->ValueAt(mid));
      return true;
    }
    if (cmp < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return false;
}

/*****************************************************************************
 * HELPER METHODS FOR INSERTION
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(page_id_t cur_page_id, const KeyType &key, const ValueType &value, Context &ctx) -> bool {
  auto cur_guard = bpm_->FetchPageWrite(cur_page_id);
  auto cur_page = cur_guard.AsMut<BPlusTreePage>();

  // if the current node is insert safe, free the header page and the write set
  if (cur_page->IsInsertSafe()) {
    ctx.header_page_ = std::nullopt;
    while (!ctx.write_set_.empty()) {
      ctx.write_set_.front().Drop();
      ctx.write_set_.pop_front();
    }
  }

  bool res = false;
  // if the current node is leaf node
  if (cur_page->IsLeafPage()) {
    auto cur_leaf_page = cur_guard.AsMut<LeafPage>();
    res = cur_leaf_page->InsertByKey(key, value, comparator_);

    // check if the current node needs to be split
    if (cur_page->NeedSplit()) {
      TrySplit(cur_page_id, cur_guard, ctx);
    }
  } else {
    // recursive insert start
    page_id_t next_page_id = INVALID_PAGE_ID;
    FindNextPage(cur_guard, key, comparator_, next_page_id);
    ctx.write_set_.emplace_back(std::move(cur_guard));
    res = Insert(next_page_id, key, value, ctx);

    // recursive insert end
    if (!ctx.write_set_.empty()) {
      cur_guard = std::move(ctx.write_set_.back());
      ctx.write_set_.pop_back();
      cur_page = cur_guard.AsMut<BPlusTreePage>();

      // check if the current node needs to be split
      if (cur_page->NeedSplit()) {
        TrySplit(cur_page_id, cur_guard, ctx);
      }
    }
  }

  return res;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::TrySplit(page_id_t cur_page_id, WritePageGuard &cur_guard, Context &ctx) {
  // if the current node is root node
  if (cur_page_id == ctx.root_page_id_) {
    // create a new root node
    page_id_t right_page_id = INVALID_PAGE_ID;
    KeyType split_key{};
    Split(cur_guard, right_page_id, split_key);
    page_id_t new_root_page_id;
    auto new_root_guard = bpm_->NewPageGuarded(&new_root_page_id);
    auto new_root_page = new_root_guard.AsMut<InternalPage>();
    new_root_page->Init(internal_max_size_);
    new_root_page->IncreaseSize(1);
    new_root_page->SetValueAt(0, cur_page_id);
    new_root_page->InsertByKey(split_key, right_page_id, comparator_);
    // make the new root node the root node
    ctx.root_page_id_ = new_root_page_id;
    if (ctx.header_page_.has_value()) {
      ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_page_id;
    }
  } else {
    page_id_t right_page_id = INVALID_PAGE_ID;
    KeyType split_key{};
    Split(cur_guard, right_page_id, split_key);
    auto parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto parent_page = parent_guard.AsMut<InternalPage>();
    parent_page->InsertByKey(split_key, right_page_id, comparator_);
    ctx.write_set_.emplace_back(std::move(parent_guard));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Split(WritePageGuard &cur_guard, page_id_t &right_page_id, KeyType &spilt_key) {
  BasicPageGuard right_guard = bpm_->NewPageGuarded(&right_page_id);

  auto cur_page = cur_guard.AsMut<BPlusTreePage>();
  if (cur_page->IsLeafPage()) {
    auto cur_leaf_page = cur_guard.AsMut<LeafPage>();
    auto right_leaf_page = right_guard.AsMut<LeafPage>();
    right_leaf_page->Init(leaf_max_size_);
    cur_leaf_page->Split(right_leaf_page, spilt_key, comparator_);
    right_leaf_page->SetNextPageId(cur_leaf_page->GetNextPageId());
    cur_leaf_page->SetNextPageId(right_page_id);
  } else {
    auto cur_internal_page = cur_guard.AsMut<InternalPage>();
    auto right_internal_page = right_guard.AsMut<InternalPage>();
    right_internal_page->Init(internal_max_size_);
    cur_internal_page->Split(right_internal_page, spilt_key, comparator_);
  }
}

/*****************************************************************************
 * HELPER METHODS FOR REMOVE
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(page_id_t cur_page_id, const KeyType &key, Context &ctx, int parent_index) {
  auto cur_guard = bpm_->FetchPageWrite(cur_page_id);
  auto cur_page = cur_guard.AsMut<BPlusTreePage>();

  // if the current node is remove safe, free the header page and the write set
  if (cur_page->IsRemoveSafe()) {
    // ctx.header_page_ = std::nullopt;
    while (!ctx.write_set_.empty()) {
      ctx.write_set_.front().Drop();
      ctx.write_set_.pop_front();
    }
  }

  if (cur_page->IsLeafPage()) {
    auto cur_leaf_page = cur_guard.AsMut<LeafPage>();
    cur_leaf_page->RemoveByKey(key, comparator_);

    // check if the current node needs to be merged or redistributed
    if (cur_page->NeedMergeOreRedistribution()) {
      // std::cout<<"Page "<<cur_page_id<<" needs merge or redistribution"<<std::endl;
      TryMergeOreRedistribution(cur_page_id, cur_guard, ctx, parent_index);
    }
  } else {
    // recursive remove start
    page_id_t next_page_id = INVALID_PAGE_ID;
    int index = FindNextPage(cur_guard, key, comparator_, next_page_id);
    ctx.write_set_.emplace_back(std::move(cur_guard));
    Remove(next_page_id, key, ctx, index);

    // recursive remove end
    // remove current node from write set
    if (!ctx.write_set_.empty()) {
      cur_guard = std::move(ctx.write_set_.back());
      ctx.write_set_.pop_back();
      cur_page = cur_guard.AsMut<BPlusTreePage>();

      // check if the current node needs to be merged or redistributed
      if (cur_page->NeedMergeOreRedistribution()) {
        // std::cout<<"Page "<<cur_page_id<<" needs merge or redistribution"<<std::endl;
        TryMergeOreRedistribution(cur_page_id, cur_guard, ctx, parent_index);
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::TryMergeOreRedistribution(page_id_t cur_page_id, WritePageGuard &cur_guard, Context &ctx,
                                               int parent_index) {
  // if the current node is the root node, and only has one child node, make the child node the root node
  if (ctx.root_page_id_ == cur_page_id) {
    auto cur_page = cur_guard.AsMut<BPlusTreePage>();
    if (!cur_page->IsLeafPage() && cur_page->GetSize() == 1) {
      auto cur_internal_page = cur_guard.AsMut<InternalPage>();
      page_id_t new_root_page_id = cur_internal_page->ValueAt(0);
      if (ctx.header_page_.has_value()) {
        ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_page_id;
      }
      ctx.root_page_id_ = new_root_page_id;

      cur_guard.Drop();
      bpm_->DeletePage(cur_page_id);
    }
  } else {
    auto parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto parent_page = parent_guard.AsMut<InternalPage>();

    page_id_t sibling_page_id = INVALID_PAGE_ID;
    if (parent_index == parent_page->GetSize() - 1) {
      sibling_page_id = parent_page->ValueAt(parent_index - 1);
    } else {
      sibling_page_id = parent_page->ValueAt(parent_index + 1);
    }
    auto sibling_guard = bpm_->FetchPageWrite(sibling_page_id);
    auto cur_page = cur_guard.AsMut<BPlusTreePage>();
    auto sibling_page = sibling_guard.AsMut<BPlusTreePage>();
    if (cur_page->IsLeafPage()) {
      if (cur_page->GetSize() + sibling_page->GetSize() < leaf_max_size_) {
        if (parent_index == parent_page->GetSize() - 1) {
          Merge(sibling_guard, cur_guard, parent_guard, cur_page_id, sibling_page_id, parent_index);
        } else {
          Merge(cur_guard, sibling_guard, parent_guard, sibling_page_id, cur_page_id, parent_index + 1);
        }
      } else {
        Redistribution(cur_guard, sibling_guard, parent_guard, parent_index);
      }
    } else {
      if (cur_page->GetSize() + sibling_page->GetSize() <= internal_max_size_) {
        if (parent_index == parent_page->GetSize() - 1) {
          Merge(sibling_guard, cur_guard, parent_guard, cur_page_id, sibling_page_id, parent_index);
        } else {
          Merge(cur_guard, sibling_guard, parent_guard, sibling_page_id, cur_page_id, parent_index + 1);
        }
      } else {
        Redistribution(cur_guard, sibling_guard, parent_guard, parent_index);
      }
    }
    ctx.write_set_.emplace_back(std::move(parent_guard));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(WritePageGuard &left_guard, WritePageGuard &right_guard, WritePageGuard &parent_guard,
                           page_id_t left_page_id, page_id_t right_page_id, int index) {
  // LOG_DEBUG("Merge %d and %d, index %d", left_page_id, right_page_id, index);
  auto parent_page = parent_guard.AsMut<InternalPage>();
  auto left_page = left_guard.AsMut<BPlusTreePage>();
  // if cur node is leaf node, append all key-value pairs in the right node to the left node
  if (left_page->IsLeafPage()) {
    auto left_leaf_page = left_guard.AsMut<LeafPage>();
    auto right_leaf_page = right_guard.AsMut<LeafPage>();
    left_leaf_page->Merge(right_leaf_page);
    left_leaf_page->SetNextPageId(right_leaf_page->GetNextPageId());
    parent_page->RemoveByIndex(index);

    right_guard.Drop();
    bpm_->DeletePage(right_page_id);
  } else {
    auto left_internal_page = left_guard.AsMut<InternalPage>();
    auto right_internal_page = right_guard.AsMut<InternalPage>();

    left_internal_page->InsertByKey(parent_page->KeyAt(index), right_internal_page->ValueAt(0), comparator_);
    left_internal_page->Merge(right_internal_page, comparator_);
    parent_page->RemoveByIndex(index);

    right_guard.Drop();
    bpm_->DeletePage(right_page_id);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribution(WritePageGuard &cur_guard, WritePageGuard &sibling_guard,
                                    WritePageGuard &parent_guard, int index) {
  // LOG_DEBUG("Redistribution %d and %d, index %d", cur_guard.PageId(), sibling_guard.PageId(), index);
  auto parent_page = parent_guard.AsMut<InternalPage>();
  // if sibling_node is a predecessor of cur_node
  if (index == parent_page->GetSize() - 1) {
    auto cur_page = cur_guard.AsMut<BPlusTreePage>();
    // if cur_node is leaf node
    if (cur_page->IsLeafPage()) {
      auto cur_leaf_page = cur_guard.AsMut<LeafPage>();
      auto sibling_leaf_page = sibling_guard.AsMut<LeafPage>();

      // move the last key/value in the sibling node to the cur node
      KeyType last_key = sibling_leaf_page->KeyAt(sibling_leaf_page->GetSize() - 1);
      ValueType last_value = sibling_leaf_page->ValueAt(sibling_leaf_page->GetSize() - 1);
      sibling_leaf_page->RemoveByIndex(sibling_leaf_page->GetSize() - 1);
      cur_leaf_page->Insert(0, last_key, last_value);
      parent_page->SetKeyAt(index, last_key);
    } else {
      auto cur_internal_page = cur_guard.AsMut<InternalPage>();
      auto sibling_internal_page = sibling_guard.AsMut<InternalPage>();

      // move the last key/value in the sibling node to the cur node
      page_id_t last_page_id = sibling_internal_page->ValueAt(sibling_internal_page->GetSize() - 1);
      KeyType last_key = sibling_internal_page->KeyAt(sibling_internal_page->GetSize() - 1);
      sibling_internal_page->RemoveByIndex(sibling_internal_page->GetSize() - 1);
      page_id_t first_page_id = cur_internal_page->ValueAt(0);
      cur_internal_page->SetValueAt(0, last_page_id);
      cur_internal_page->Insert(1, parent_page->KeyAt(index), first_page_id);
      parent_page->SetKeyAt(index, last_key);
    }
  } else {
    auto cur_page = cur_guard.AsMut<BPlusTreePage>();
    // if cur_node is not a leaf node
    if (cur_page->IsLeafPage()) {
      auto cur_leaf_page = cur_guard.AsMut<LeafPage>();
      auto sibling_leaf_page = sibling_guard.AsMut<LeafPage>();

      // move the first key/value in the sibling node to the cur node
      KeyType first_key = sibling_leaf_page->KeyAt(0);
      ValueType first_value = sibling_leaf_page->ValueAt(0);
      sibling_leaf_page->RemoveByIndex(0);
      cur_leaf_page->Insert(cur_leaf_page->GetSize(), first_key, first_value);
      parent_page->SetKeyAt(index + 1, sibling_leaf_page->KeyAt(0));
    } else {
      auto cur_internal_page = cur_guard.AsMut<InternalPage>();
      auto sibling_internal_page = sibling_guard.AsMut<InternalPage>();

      // move the first key/value in the sibling node to the cur node
      page_id_t first_page_id = sibling_internal_page->ValueAt(0);
      KeyType first_key = parent_page->KeyAt(index + 1);
      cur_internal_page->Insert(cur_internal_page->GetSize(), first_key, first_page_id);
      page_id_t second_page_id = sibling_internal_page->ValueAt(1);
      KeyType second_key = sibling_internal_page->KeyAt(1);
      parent_page->SetKeyAt(index + 1, second_key);
      sibling_internal_page->RemoveByIndex(1);
      sibling_internal_page->SetValueAt(0, second_page_id);
    }
  }

  //  if (index != 0) {
  //    auto cur_page = cur_guard.AsMut<BPlusTreePage>();
  //    // if cur_node is not a leaf node
  //    if (!cur_page->IsLeafPage()) {
  //      auto cur_internal_page = cur_guard.AsMut<InternalPage>();
  //      auto sibling_internal_page = sibling_guard.AsMut<InternalPage>();
  //
  //      // move the last key/value in the sibling node to the cur node
  //      page_id_t last_page_id = sibling_internal_page->ValueAt(sibling_internal_page->GetSize() - 1);
  //      KeyType last_key = sibling_internal_page->KeyAt(sibling_internal_page->GetSize() - 1);
  //      sibling_internal_page->RemoveByIndex(sibling_internal_page->GetSize() - 1);
  //      page_id_t first_page_id = cur_internal_page->ValueAt(0);
  //      cur_internal_page->SetValueAt(0, last_page_id);
  //      cur_internal_page->Insert(1, parent_page->KeyAt(index), first_page_id);
  //      parent_page->SetKeyAt(index, last_key);
  //    } else {
  //      auto cur_leaf_page = cur_guard.AsMut<LeafPage>();
  //      auto sibling_leaf_page = sibling_guard.AsMut<LeafPage>();
  //
  //      // move the last key/value in the sibling node to the cur node
  //      KeyType last_key = sibling_leaf_page->KeyAt(sibling_leaf_page->GetSize() - 1);
  //      ValueType last_value = sibling_leaf_page->ValueAt(sibling_leaf_page->GetSize() - 1);
  //      sibling_leaf_page->RemoveByIndex(sibling_leaf_page->GetSize() - 1);
  //      cur_leaf_page->Insert(0, last_key, last_value);
  //      parent_page->SetKeyAt(index, last_key);
  //    }
  //  } else {
  //    auto cur_page = cur_guard.AsMut<BPlusTreePage>();
  //    // if cur_node is not a leaf node
  //    if (!cur_page->IsLeafPage()) {
  //      auto cur_internal_page = cur_guard.AsMut<InternalPage>();
  //      auto sibling_internal_page = sibling_guard.AsMut<InternalPage>();
  //
  //      // move the first key/value in the sibling node to the cur node
  //      page_id_t first_page_id = sibling_internal_page->ValueAt(0);
  //      KeyType first_key = parent_page->KeyAt(index + 1);
  //      cur_internal_page->Insert(cur_internal_page->GetSize(), first_key, first_page_id);
  //      page_id_t second_page_id = sibling_internal_page->ValueAt(1);
  //      KeyType second_key = sibling_internal_page->KeyAt(1);
  //      parent_page->SetKeyAt(index + 1, second_key);
  //      sibling_internal_page->RemoveByIndex(1);
  //      sibling_internal_page->SetValueAt(0, second_page_id);
  //    } else {
  //      auto cur_leaf_page = cur_guard.AsMut<LeafPage>();
  //      auto sibling_leaf_page = sibling_guard.AsMut<LeafPage>();
  //
  //      // move the first key/value in the sibling node to the cur node
  //      KeyType first_key = sibling_leaf_page->KeyAt(0);
  //      ValueType first_value = sibling_leaf_page->ValueAt(0);
  //      sibling_leaf_page->RemoveByIndex(0);
  //      cur_leaf_page->Insert(cur_leaf_page->GetSize(), first_key, first_value);
  //      parent_page->SetKeyAt(index + 1, sibling_leaf_page->KeyAt(0));
  //    }
  //  }
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

}  // namespace bustub
