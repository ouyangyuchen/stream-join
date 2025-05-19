#ifndef INDEX_LIST_HPP_
#define INDEX_LIST_HPP_

#include <deque>
#include <set>
#include "index/index.hpp"

namespace stream {

template <typename KeyType, typename ValueType>
class ListIndex : public WindowIndex<KeyType, ValueType> {
 public:
  ListIndex() = default;

  ~ListIndex() override = default;

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
  std::deque<TupleType<KeyType, ValueType>> list_;  // list to store tuples in the arrival order
  std::set<KeyType> keys_;                          // set to check for duplicate keys
};

}  // namespace stream

// Definition of static member
template <typename KeyType, typename ValueType>
const std::string stream::ListIndex<KeyType, ValueType>::Name = "ListIndex";

// Implementation of ListIndex methods

template <typename KeyType, typename ValueType>
void stream::ListIndex<KeyType, ValueType>::Insert(const TupleType<KeyType, ValueType> &tuple) {
  if (keys_.find(tuple.key_) != keys_.end()) {
    throw std::runtime_error("Key already exists in the list index");
  }
  list_.push_back(tuple);
  keys_.insert(tuple.key_);
}

template <typename KeyType, typename ValueType>
auto stream::ListIndex<KeyType, ValueType>::PopOldest() -> TupleType<KeyType, ValueType> {
  if (list_.empty()) {
    throw std::out_of_range("Index is empty");
  }
  auto oldest = list_.front();
  list_.pop_front();
  keys_.erase(oldest.key_);
  return oldest;
}

template <typename KeyType, typename ValueType>
auto stream::ListIndex<KeyType, ValueType>::GetOldest() const -> TupleType<KeyType, ValueType> {
  if (list_.empty()) {
    throw std::out_of_range("Index is empty");
  }
  return list_.front();
}

template <typename KeyType, typename ValueType>
auto stream::ListIndex<KeyType, ValueType>::GetOldestRef() -> TupleType<KeyType, ValueType> & {
  if (list_.empty()) {
    throw std::out_of_range("Index is empty");
  }
  return list_.front();
}

template <typename KeyType, typename ValueType>
auto stream::ListIndex<KeyType, ValueType>::RangeSearch(const std::pair<KeyType, KeyType> &key_range) const
    -> std::vector<TupleType<KeyType, ValueType>> {
  std::vector<TupleType<KeyType, ValueType>> result;
  for (const auto &tuple : list_) {
    auto &key = tuple.key_;
    if (key >= key_range.first && key <= key_range.second) {
      result.push_back(tuple);
    }
  }
  return result;
}

template <typename KeyType, typename ValueType>
auto stream::ListIndex<KeyType, ValueType>::Size() const -> size_t {
  return list_.size();
}

template <typename KeyType, typename ValueType>
auto stream::ListIndex<KeyType, ValueType>::Empty() const -> bool {
  return list_.empty();
}

#endif