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

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  pages_ = nullptr;
  page_table_.clear();
  free_list_.clear();
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock latch(latch_);
  // return nullptr if all frames are currently in use and not evictable
  if (free_list_.empty() && replacer_->Size() == 0) {
    *page_id = INVALID_PAGE_ID;
    // LOG_DEBUG("newPage failed : all frames are currently in use and not evictable");
    return nullptr;
  }
  // pick the replacement frame from either the free list or the replacer (always find from the free list first)
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&frame_id);
    // If the replacement frame has a dirty page, you should write it back to the disk first
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    // LOG_DEBUG("evictPage : page_id %d, frame_id %d;", pages_[frame_id].page_id_, frame_id);
    //  remove the page from the page table
    page_table_.erase(pages_[frame_id].page_id_);
  }

  // call the AllocatePage() method to get a new page id
  *page_id = AllocatePage();
  // reset the memory and metadata for the new page
  Page *page = &pages_[frame_id];
  page->ResetMemory();
  page->page_id_ = *page_id;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  // insert the page into the page table
  page_table_[*page_id] = frame_id;
  // pin the frame
  pages_[frame_id].pin_count_++;
  // record the access history of the frame in the replacer for the lru-k algorithm to work
  replacer_->RecordAccess(frame_id);
  // LOG_DEBUG("newPage success : page_id %d, frame_id %d;", *page_id, frame_id);
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock latch(latch_);
  // return nullptr if page_id needs to be fetched from the disk but all frames are currently in use and not evictable
  if (page_table_.find(page_id) == page_table_.end() && free_list_.empty() && replacer_->Size() == 0) {
    //    LOG_DEBUG(
    //        "fetchPage failed : page_id %d page_id needs to be fetched from the disk but all frames are currently "
    //        "in use and not evictable",
    //         page_id);
    return nullptr;
  }

  // first search for page_id in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    // if found, pin the frame
    frame_id_t frame_id = page_table_[page_id];
    // LOG_DEBUG("fetchPage success : page_id %d, frame_id %d, fetched from the buffer pool;", page_id, frame_id);
    pages_[frame_id].pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    // record the access history of the frame in the replacer for the lru-k algorithm to work
    replacer_->RecordAccess(frame_id);
    return &pages_[frame_id];
  }
  // if not found
  // pick a replacement frame from the free list
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // pick a replacement frame from the replacer
    replacer_->Evict(&frame_id);
    // remove the page from the page table
    page_table_.erase(pages_[frame_id].page_id_);
    // LOG_DEBUG("evcitPage : page_id %d, frame_id %d;", pages_[frame_id].page_id_, frame_id);
    //  If the replacement frame has a dirty page, you should write it back to the disk first
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
  }
  // read the page from disk by calling disk_manager_->ReadPage()
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  // reset the metadata for the new page
  Page *page = &pages_[frame_id];
  page->page_id_ = page_id;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  // insert the page into the page table
  page_table_[page_id] = frame_id;
  // pin the frame
  pages_[frame_id].pin_count_++;
  // record the access history of the frame in the replacer for the lru-k algorithm to work
  replacer_->RecordAccess(frame_id);
  // LOG_DEBUG("fetchPage success : page_id %d, frame_id %d, fetched from the disk;", page_id, frame_id);
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock latch(latch_);
  // If page_id is not in the buffer pool or its pin count is already 0, return false.
  if (page_table_.find(page_id) == page_table_.end() || pages_[page_table_[page_id]].GetPinCount() == 0) {
    return false;
  }
  // set the dirty flag on the page to indicate if the page was modified.
  pages_[page_table_[page_id]].is_dirty_ |= is_dirty;

  // Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
  pages_[page_table_[page_id]].pin_count_--;
  // LOG_DEBUG("unpinPage success : page_id %d, frame_id %d, pin_count %d;", page_id, page_table_[page_id],
  // pages_[page_table_[page_id]].GetPinCount());
  if (pages_[page_table_[page_id]].pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock latch(latch_);
  // return false if the page could not be found in the page table
  if (page_table_.find(page_id) == page_table_.end()) {
    // LOG_DEBUG("flushPage failed : page_id %d not found", page_id);
    return false;
  }
  // use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());
  // unset the dirty flag of the page after flushing.
  pages_[page_table_[page_id]].is_dirty_ = false;
  // LOG_DEBUG("flushPage success : page_id %d", page_id);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock latch(latch_);
  // flush all the pages in the buffer pool to disk.
  for (auto &it : page_table_) {
    disk_manager_->WritePage(it.first, pages_[it.second].GetData());
    pages_[it.second].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock latch(latch_);

  // If page_id is not in the buffer pool, do nothing and return true
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  // If the page is pinned and cannot be deleted, return false immediately.
  if (pages_[page_table_[page_id]].GetPinCount() > 0) {
    return false;
  }

  // delete the page from the page table
  frame_id_t frame_id = page_table_[page_id];
  // LOG_DEBUG("deletePage : page_id %d, frame_id %d;", page_id, frame_id);
  page_table_.erase(page_id);
  // stop tracking the frame in the replacer
  replacer_->Remove(frame_id);
  // add the frame back to the free list.
  free_list_.push_back(frame_id);
  // reset the page's memory and metadata
  Page *page = &pages_[frame_id];
  page->WLatch();
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->WUnlatch();
  // call DeallocatePage() to imitate freeing the page on the disk.
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  // std::scoped_lock latch(latch_);
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // std::scoped_lock latch(latch_);
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // std::scoped_lock latch(latch_);
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
