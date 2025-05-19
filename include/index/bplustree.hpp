#ifndef BPLUS_TREE_INDEX_HPP
#define BPLUS_TREE_INDEX_HPP

#include <deque>
#include <list>
#include <stdexcept>
#include "index/index.hpp"
#include "stx/btree_map.h"  // NOLINT

namespace stream {

template <typename KeyType, typename ValueType>
class BPlusTreeIndex : public WindowIndex<KeyType, ValueType> {
 public:
  BPlusTreeIndex() = default;

  ~BPlusTreeIndex() override = default;

  void Insert(const TupleType<KeyType, ValueType> &tuple) override;

  auto PopOldest() -> TupleType<KeyType, ValueType> override;

  auto GetOldest() const -> TupleType<KeyType, ValueType> override;

  auto GetOldestRef() -> TupleType<KeyType, ValueType> & override;

  auto RangeSearch(const std::pair<KeyType, KeyType> &key_range) const
      -> std::vector<TupleType<KeyType, ValueType>> override;

  auto Size() const -> size_t override;

  auto Empty() const -> bool override;

  const static std::string Name;

 private:
  stx::btree_map<KeyType, TupleType<KeyType, ValueType>> tree_;  // store multiple tuples for each key
  std::deque<KeyType> arrival_list;                              // tuples in the arrival order
};

template <typename KeyType, typename ValueType>
const std::string BPlusTreeIndex<KeyType, ValueType>::Name = "BPlusTreeIndex";

template <typename KeyType, typename ValueType>
void BPlusTreeIndex<KeyType, ValueType>::Insert(const TupleType<KeyType, ValueType> &tuple) {
  if (tree_.find(tuple.key_) != tree_.end()) {
    throw std::runtime_error("Key already exists in the B+ tree index");
  }
  tree_[tuple.key_] = tuple;
  arrival_list.push_back(tuple.key_);
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::PopOldest() -> TupleType<KeyType, ValueType> {
  if (arrival_list.empty()) {
    throw std::out_of_range("Index is empty");
  }
  auto oldest_key = arrival_list.front();
  arrival_list.pop_front();
  auto oldest_tuple = tree_[oldest_key];
  tree_.erase(oldest_key);
  return oldest_tuple;
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::GetOldest() const -> TupleType<KeyType, ValueType> {
  if (arrival_list.empty()) {
    throw std::out_of_range("Index is empty");
  }
  auto &oldest_key = arrival_list.front();
  auto it = tree_.find(oldest_key);
  return it->second;
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::GetOldestRef() -> TupleType<KeyType, ValueType> & {
  if (arrival_list.empty()) {
    throw std::out_of_range("Index is empty");
  }
  auto &oldest_key = arrival_list.front();
  return tree_[oldest_key];
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::RangeSearch(const std::pair<KeyType, KeyType> &key_range) const
    -> std::vector<TupleType<KeyType, ValueType>> {
  std::vector<TupleType<KeyType, ValueType>> result;
  auto it = tree_.lower_bound(key_range.first);
  while (it != tree_.end() && it->first <= key_range.second) {
    result.push_back(it->second);
    ++it;
  }
  return result;
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::Size() const -> size_t {
  return tree_.size();
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::Empty() const -> bool {
  return Size() == 0;
}

}  // namespace stream

#endif