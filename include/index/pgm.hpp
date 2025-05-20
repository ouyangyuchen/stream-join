#ifndef PGM_LEARNED_INDEX_HPP
#define PGM_LEARNED_INDEX_HPP

#include "index.hpp"
#include "pgm/pgm_index_dynamic.hpp"

namespace stream {

template <typename KeyType, typename ValueType>
class PGMWindowIndex : public WindowIndex<KeyType, ValueType> {
 public:
  PGMWindowIndex();

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
  pgm::DynamicPGMIndex<KeyType, TupleType<KeyType, ValueType> *> index_;  // for searching, get the pointer to the tuple
  std::list<TupleType<KeyType, ValueType>> arrival_list_;                 // for maintaining the order of arrival
};

template <typename KeyType, typename ValueType>
const std::string PGMWindowIndex<KeyType, ValueType>::Name = "PGMWindowIndex";

template <typename KeyType, typename ValueType>
PGMWindowIndex<KeyType, ValueType>::PGMWindowIndex() : index_() {}

template <typename KeyType, typename ValueType>
void PGMWindowIndex<KeyType, ValueType>::Insert(const TupleType<KeyType, ValueType> &tuple) {
  if (index_.find(tuple.key_) != index_.end()) {
    throw std::runtime_error("Duplicate key insertion is not allowed in PGM Index");
  }
  arrival_list_.push_back(tuple);
  index_.insert_or_assign(tuple.key_, &arrival_list_.back());
}

template <typename KeyType, typename ValueType>
auto PGMWindowIndex<KeyType, ValueType>::PopOldest() -> TupleType<KeyType, ValueType> {
  if (Empty()) {
    throw std::runtime_error("PGM Index is empty");
  }
  auto oldest_tuple = std::move(arrival_list_.front());
  arrival_list_.pop_front();
  index_.erase(oldest_tuple.key_);
  return oldest_tuple;
}

template <typename KeyType, typename ValueType>
auto PGMWindowIndex<KeyType, ValueType>::GetOldest() const -> TupleType<KeyType, ValueType> {
  if (Empty()) {
    throw std::runtime_error("PGM Index is empty");
  }
  return arrival_list_.front();
}

template <typename KeyType, typename ValueType>
auto PGMWindowIndex<KeyType, ValueType>::GetOldestRef() -> TupleType<KeyType, ValueType> & {
  if (Empty()) {
    throw std::runtime_error("PGM Index is empty");
  }
  return arrival_list_.front();
}

template <typename KeyType, typename ValueType>
auto PGMWindowIndex<KeyType, ValueType>::RangeSearch(const std::pair<KeyType, KeyType> &key_range) const
    -> std::vector<TupleType<KeyType, ValueType>> {
  std::vector<TupleType<KeyType, ValueType>> result;
  auto range_result = index_.range(key_range.first, key_range.second);
  for (const auto &pair : range_result) {
    result.push_back(*pair.second);
  }
  return result;
}

template <typename KeyType, typename ValueType>
auto PGMWindowIndex<KeyType, ValueType>::Size() const -> size_t {
  return index_.size();
}

template <typename KeyType, typename ValueType>
auto PGMWindowIndex<KeyType, ValueType>::Empty() const -> bool {
  return Size() == 0;
}
}  // namespace stream

#endif