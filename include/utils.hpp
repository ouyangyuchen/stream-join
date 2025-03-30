#ifndef UTILS_HPP
#define UTILS_HPP

#include <cstdint>
#include <tuple>

namespace stream {
using TsType = int64_t;  // timestamp type

template <typename KeyType, typename ValueType>
using KVPair = std::pair<KeyType, ValueType>;

template <typename KeyType, typename ValueType>
using TupleType = std::tuple<TsType, KVPair<KeyType, ValueType>>;

// helper functions to access tuple elements
template <typename KeyType, typename ValueType>
static auto GetTimestamp(const TupleType<KeyType, ValueType> &tuple) -> TsType {
  return std::get<0>(tuple);
}

template <typename KeyType, typename ValueType>
static auto GetKey(const TupleType<KeyType, ValueType> &tuple) -> KeyType {
  return std::get<1>(tuple).first;
}

template <typename KeyType, typename ValueType>
static auto GetValue(const TupleType<KeyType, ValueType> &tuple) -> ValueType {
  return std::get<1>(tuple).second;
}

template <typename KeyType, typename ValueType>
static auto GetTimestamp(TupleType<KeyType, ValueType> &tuple) -> TsType & {
  return std::get<0>(tuple);
}

template <typename KeyType, typename ValueType>
static auto GetKey(TupleType<KeyType, ValueType> &tuple) -> KeyType & {
  return std::get<1>(tuple).first;
}

template <typename KeyType, typename ValueType>
static auto GetValue(TupleType<KeyType, ValueType> &tuple) -> ValueType & {
  return std::get<1>(tuple).second;
}

template <typename KeyType, typename ValueType>
static auto MakeTuple(const TsType &timestamp, const KeyType &key, const ValueType &value)
    -> TupleType<KeyType, ValueType> {
  return std::make_tuple(timestamp, std::make_pair(key, value));
}

};  // namespace stream

#endif