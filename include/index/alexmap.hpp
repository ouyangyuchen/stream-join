#ifndef ALEX_MAP_INDEX_HPP
#define ALEX_MAP_INDEX_HPP

#include <list>
#include <set>
#include "alex_map.h"
#include "index.hpp"

namespace stream {
template <typename KeyType, typename ValueType>
class AlexMapWindowIndex : public WindowIndex<KeyType, ValueType> {
 public:
  AlexMapWindowIndex();

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
  alex::AlexMap<KeyType, TupleType<KeyType, ValueType> *> index_;  // for searching, get the pointer to the tuple
  std::list<TupleType<KeyType, ValueType>> arrival_list_;          // for maintaining the order of arrival
};

template <typename KeyType, typename ValueType>
const std::string AlexMapWindowIndex<KeyType, ValueType>::Name = "AlexMapWindowIndex";

template <typename KeyType, typename ValueType>
AlexMapWindowIndex<KeyType, ValueType>::AlexMapWindowIndex() : index_() {}

template <typename KeyType, typename ValueType>
void AlexMapWindowIndex<KeyType, ValueType>::Insert(const TupleType<KeyType, ValueType> &tuple) {
  if (index_.find(tuple.key_) != index_.end()) {
    throw std::runtime_error("Duplicate key insertion is not allowed in AlexMap Index");
  }
  arrival_list_.push_back(tuple);
  auto [it, success] = index_.insert(tuple.key_, &arrival_list_.back());
  if (!success || it.key() != tuple.key_) {
    throw std::runtime_error("Insertion failed in AlexMap Index");
  }
}

template <typename KeyType, typename ValueType>
auto AlexMapWindowIndex<KeyType, ValueType>::PopOldest() -> TupleType<KeyType, ValueType> {
  if (Empty()) {
    throw std::runtime_error("AlexMap Index is empty");
  }
  auto oldest_tuple = arrival_list_.front();
  arrival_list_.pop_front();
  index_.erase(oldest_tuple.key_);
  return oldest_tuple;
}

template <typename KeyType, typename ValueType>
auto AlexMapWindowIndex<KeyType, ValueType>::GetOldest() const -> TupleType<KeyType, ValueType> {
  if (Empty()) {
    throw std::runtime_error("AlexMap Index is empty");
  }
  return arrival_list_.front();
}

template <typename KeyType, typename ValueType>
auto AlexMapWindowIndex<KeyType, ValueType>::GetOldestRef() -> TupleType<KeyType, ValueType> & {
  if (Empty()) {
    throw std::runtime_error("AlexMap Index is empty");
  }
  return arrival_list_.front();
}

template <typename KeyType, typename ValueType>
auto AlexMapWindowIndex<KeyType, ValueType>::RangeSearch(const std::pair<KeyType, KeyType> &key_range) const
    -> std::vector<TupleType<KeyType, ValueType>> {
  if (key_range.first > key_range.second) {
    throw std::invalid_argument("Invalid key range: first key must be less than or equal to second key");
  }
  std::vector<TupleType<KeyType, ValueType>> result;
  auto it = index_.lower_bound(key_range.first);
  while (it != index_.cend() && it.key() <= key_range.second) {
    result.push_back(*(it.payload()));
    ++it;
  }
  return result;
}

template <typename KeyType, typename ValueType>
auto AlexMapWindowIndex<KeyType, ValueType>::Size() const -> size_t {
  return arrival_list_.size();
}

template <typename KeyType, typename ValueType>
auto AlexMapWindowIndex<KeyType, ValueType>::Empty() const -> bool {
  return arrival_list_.empty();
}

}  // namespace stream

#endif  // PGM_LEARNED_INDEX_HPP