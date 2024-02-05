//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get/set the value associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(int index, const KeyType &key, const bustub::page_id_t &value) {
  IncreaseSize(1);
  for (int i = GetSize() - 1; i > index; i--) {
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[index].first = key;
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertByKey(const KeyType &key, const ValueType &value,
                                                 const KeyComparator &comparator) {
  // std::cout << "insert by key: " << key.ToString() << " " << "value: " << value << std::endl;
  int index = 0;
  int left = 1;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    int cmp = comparator(array_[mid].first, key);
    if (cmp == 0) {
      return;
    }
    if (cmp < 0) {
      index = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  index++;
  IncreaseSize(1);
  for (int i = GetSize() - 1; i > index; i--) {
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[index].first = key;
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BPlusTreeInternalPage *other, KeyType &key,
                                           const KeyComparator &comparator) {
  BUSTUB_ENSURE(GetSize() == GetMaxSize() + 1, "Only full node can be split");

  key = KeyAt(GetMinSize());
  int len = GetSize() - GetMinSize();
  for (int i = 0; i < len; i++) {
    if (i == 0) {
      other->array_[i].first = KeyType{};
      other->array_[i].second = array_[i + GetMinSize()].second;
    } else {
      other->array_[i].first = array_[i + GetMinSize()].first;
      other->array_[i].second = array_[i + GetMinSize()].second;
    }
  }
  other->SetSize(len);
  SetSize(GetMinSize());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveByIndex(int index) {
  if (index <= 0 || index >= GetSize()) {
    // 打印index
    std::cout << "index out of range: " << index << std::endl;
    throw Exception(ExceptionType::OUT_OF_RANGE, "index out of bound");
  }
  // std::cout<<"remove by index: "<<index<<" "<<"key: "<<array_[index].first.ToString()<<std::endl;
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i].first = array_[i + 1].first;
    array_[i].second = array_[i + 1].second;
  }
  IncreaseSize(-1);
  // 打印 array_
  //  std::cout<<"after remove by index: "<<std::endl;
  //  for (int i = 0; i < GetSize(); i++) {
  //    std::cout << "array_[" << i << "]: " << array_[i].first.ToString() << " " << array_[i].second << std::endl;
  //  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveByKey(const KeyType &key, const KeyComparator &comparator) {
  int left = 1;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    int cmp = comparator(array_[mid].first, key);
    if (cmp == 0) {
      for (int i = mid; i < GetSize() - 1; i++) {
        array_[i].first = array_[i + 1].first;
        array_[i].second = array_[i + 1].second;
      }
      IncreaseSize(-1);
      return;
    }
    if (cmp < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(BPlusTreeInternalPage *other, const KeyComparator &comparator) {
  // 打印other array_
  //  std::cout<<"other page: "<<std::endl;
  //  for (int i = 0; i < other->GetSize(); i++) {
  //    std::cout << "array_[" << i << "]: " << other->KeyAt(i).ToString() << " " << other->ValueAt(i) << std::endl;
  //  }
  for (int i = 1; i < other->GetSize(); i++) {
    array_[GetSize() + i - 1].first = other->KeyAt(i);
    array_[GetSize() + i - 1].second = other->ValueAt(i);
  }
  IncreaseSize(other->GetSize() - 1);
  // 打印array_
  //  std::cout<<"after merge  page: " << std::endl;
  //  for (int i = 0; i < GetSize(); i++) {
  //    std::cout << "array_[" << i << "]: " << array_[i].first.ToString() << " " << array_[i].second << std::endl;
  //  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Clear() { SetSize(1); }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
