#pragma once

#include <deque>

#include "index.hpp"

namespace indexes {

template <typename KeyType, typename ValueType>
class ListIndex : public Index<KeyType, ValueType> {
  using TupleType = std::tuple<TsType, KeyType, ValueType>;

 public:
  ListIndex(TsType window_length) : Index<KeyType, ValueType>(window_length) {}

  auto insert(const TupleType &tuple) -> void override {
    // if key is already in the list, do nothing
    auto key = Index<KeyType, ValueType>::get_key(tuple);
    auto it = std::find_if(tuples_.begin(), tuples_.end(), [key](const TupleType &t) {
      return Index<KeyType, ValueType>::get_key(t) == key;
    });
    if (it != tuples_.end()) {
      return;
    }

    // if list is full, pop the oldest tuple
    if (tuples_.size() >= this->window_length_) {
      tuples_.pop_front();
    }
    tuples_.emplace_back(tuple);
  }

  auto range_search(const KeyType &start,
                    const KeyType &end) const -> std::vector<TupleType> override {
    std::vector<TupleType> result;
    for (const auto &tuple : tuples_) {
      if (Index<KeyType, ValueType>::get_key(tuple) >= start &&
          Index<KeyType, ValueType>::get_key(tuple) <= end) {
        result.emplace_back(tuple);
      }
    }
    return result;
  }

  auto size() const -> size_t override { return tuples_.size(); }

 private:
  std::deque<TupleType> tuples_;
};

}  // namespace indexes
