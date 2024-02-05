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

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

LRUKReplacer::~LRUKReplacer() { node_store_.clear(); }

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock latch(latch_);
  // if the replacer is empty, return false
  if (curr_size_ == 0) {
    return false;
  }
  // iterate through the node_store_ to find the frame with largest backward k-distance
  LRUKNode evict_node{};
  auto it = node_store_.begin();
  for (; it != node_store_.end(); it++) {
    if (it->second->is_evictable_) {
      evict_node = *(it->second);
      break;
    }
  }

  for (; it != node_store_.end(); it++) {
    // if the node is not evictable, skip it
    if (!it->second->is_evictable_) {
      continue;
    }
    if (*(it->second) < evict_node) {
      evict_node = *(it->second);
    }
  }

  // if the node with largest backward k-distance is evictable, evict it and return true
  if (evict_node.is_evictable_) {
    // set the frame_id and remove the node
    *frame_id = evict_node.fid_;
    node_store_.erase(evict_node.fid_);
    curr_size_--;
    // LOG_DEBUG("remove frame: frame_id: %d", *frame_id);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock latch(latch_);
  current_timestamp_++;
  // if the frame_id is not found in node_store_, create a new node and insert it into node_store_
  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode node{};
    node.fid_ = frame_id;
    node.k_ = k_;
    node.history_.emplace_back(current_timestamp_);
    // if the replacer is full, evict a frame
    if (curr_size_ == replacer_size_) {
      LRUKNode evict_node{};
      auto it = node_store_.begin();
      for (; it != node_store_.end(); it++) {
        if (it->second->is_evictable_) {
          evict_node = *(it->second);
          break;
        }
      }

      for (; it != node_store_.end(); it++) {
        if (!it->second->is_evictable_) {
          continue;
        }
        if (*(it->second) < evict_node) {
          evict_node = *(it->second);
        }
      }
      if (evict_node.is_evictable_) {
        node_store_.erase(evict_node.fid_);
        curr_size_--;
      }
    }
    node_store_[frame_id] = std::make_shared<LRUKNode>(node);
    // LOG_DEBUG("insert frame: frame_id: %d,", frame_id);
  } else {
    // if the frame_id is found in node_store_, update the history_ of the node
    node_store_[frame_id]->history_.push_back(current_timestamp_);
  }
  // LOG_DEBUG("access frame: frame_id: %d, current_timestamp: %zu", frame_id, current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock latch(latch_);
  // if frame id is invalid, throw an exception or abort the process.
  if (node_store_.find(frame_id) == node_store_.end()) {
    LOG_DEBUG("frame_id: %d", frame_id);
    throw Exception(ExceptionType::INVALID, "invalid frame_id : frame_id not found in node_store_");
  }

  // if a frame was previously evictable and is to be set to non-evictable, then size should decrement.
  if (node_store_[frame_id]->is_evictable_ && !set_evictable) {
    node_store_[frame_id]->is_evictable_ = set_evictable;
    curr_size_--;
  }

  // if a frame was previously non-evictable and is to be set to evictable, then size should increment.
  if (!node_store_[frame_id]->is_evictable_ && set_evictable) {
    node_store_[frame_id]->is_evictable_ = set_evictable;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock latch(latch_);
  // If specified frame is not found, directly return from this function.
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  // If Remove is called on a non-evictable frame, throw an exception
  if (!node_store_[frame_id]->is_evictable_) {
    throw Exception(ExceptionType::INVALID, "frame_id is not evictable");
  }
  // remove the node from node_store_, decrease curr_size_
  node_store_.erase(frame_id);
  curr_size_--;
  // LOG_DEBUG("remove frame: frame_id: %d", frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock latch(latch_);
  size_t size = curr_size_;
  return size;
}

}  // namespace bustub
