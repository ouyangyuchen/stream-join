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
  stx::btree_map<KeyType, std::list<TupleType<KeyType, ValueType>>> tree_;  // store multiple tuples for each key
  std::deque<KeyType> arrival_list;                                         // tuples in the arrival order
  size_t size_{0};                                                          // number of tuples in the index
};

template <typename KeyType, typename ValueType>
const std::string BPlusTreeIndex<KeyType, ValueType>::Name = "BPlusTreeIndex";

template <typename KeyType, typename ValueType>
void BPlusTreeIndex<KeyType, ValueType>::Insert(const TupleType<KeyType, ValueType> &tuple) {
  tree_[tuple.key_].push_back(tuple);
  arrival_list.push_back(tuple.key_);
  size_++;
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::PopOldest() -> TupleType<KeyType, ValueType> {
  if (arrival_list.empty()) {
    throw std::out_of_range("Index is empty");
  }
  auto oldest_key = arrival_list.front();
  arrival_list.pop_front();
  auto oldest_tuple = tree_[oldest_key].front();
  tree_[oldest_key].pop_front();
  if (tree_[oldest_key].empty()) {
    tree_.erase(oldest_key);
  }
  size_--;
  return oldest_tuple;
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::GetOldest() const -> TupleType<KeyType, ValueType> {
  if (arrival_list.empty()) {
    throw std::out_of_range("Index is empty");
  }
  auto &oldest_key = arrival_list.front();
  auto it = tree_.find(oldest_key);
  auto oldest_tuple = it->second.front();
  return oldest_tuple;
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::GetOldestRef() -> TupleType<KeyType, ValueType> & {
  if (arrival_list.empty()) {
    throw std::out_of_range("Index is empty");
  }
  auto &oldest_key = arrival_list.front();
  return tree_[oldest_key].front();
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::RangeSearch(const std::pair<KeyType, KeyType> &key_range) const
    -> std::vector<TupleType<KeyType, ValueType>> {
  std::vector<TupleType<KeyType, ValueType>> result;
  auto it = tree_.lower_bound(key_range.first);
  while (it != tree_.end() && it->first <= key_range.second) {
    result.insert(result.end(), it->second.begin(), it->second.end());
    ++it;
  }
  return result;
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::Size() const -> size_t {
  return size_;
}

template <typename KeyType, typename ValueType>
auto BPlusTreeIndex<KeyType, ValueType>::Empty() const -> bool {
  return size_ == 0;
}

}  // namespace stream

#endif