/*
    B+ Tree Index
    - Use stx::btree_map to store the index
*/

#pragma once

#include "index.hpp"
#include "stx/btree_map.h"

namespace indexes {

template <typename KeyType, typename ValueType>
class BPlusTreeIndex : public Index<KeyType, ValueType> {
  using TupleType = std::tuple<TsType, KeyType, ValueType>;

 public:
  BPlusTreeIndex(TsType window_length) : Index<KeyType, ValueType>(window_length) {}

  auto insert(const TupleType &tuple) -> void override {
    // if the tuple key is already in the tree, do nothing
    if (tree_.find(Index<KeyType, ValueType>::get_key(tuple)) != tree_.end()) {
      return;
    }

    const auto &key = Index<KeyType, ValueType>::get_key(tuple);
    tree_.insert(key, tuple);
    keys_arrival_order_.push_back(key);

    // if the tree is full, remove the oldest tuple
    if (keys_arrival_order_.size() > this->window_length_) {
      tree_.erase(keys_arrival_order_.front());
      keys_arrival_order_.pop_front();
    }
  }

  auto range_search(const KeyType &start,
                    const KeyType &end) const -> std::vector<TupleType> override {
    std::vector<TupleType> result;

    auto it = tree_.lower_bound(start);  // find the first key >= start
    while (it != tree_.end() && it->first <= end) {
      result.push_back(it->second);
      ++it;
    }
    return result;
  }

  auto size() const -> size_t override { return tree_.size(); }

 private:
  stx::btree_map<KeyType, TupleType> tree_;
  std::deque<KeyType> keys_arrival_order_;
};
}  // namespace indexes