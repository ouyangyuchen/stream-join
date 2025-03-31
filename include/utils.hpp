#ifndef UTILS_HPP
#define UTILS_HPP

#include <cstdint>
#include <tuple>

namespace stream {
using TsType = int64_t;  // timestamp type

template <typename KeyType, typename ValueType>
struct TupleType {
  TsType timestamp_;
  KeyType key_;
  ValueType value_;
};

// helper functions to access tuple elements
template <typename KeyType, typename ValueType>
static auto GetTimestamp(const TupleType<KeyType, ValueType> &tuple) -> TsType {
  return tuple.timestamp_;
}

template <typename KeyType, typename ValueType>
static auto GetKey(const TupleType<KeyType, ValueType> &tuple) -> KeyType {
  return tuple.key_;
}

template <typename KeyType, typename ValueType>
static auto GetValue(const TupleType<KeyType, ValueType> &tuple) -> ValueType {
  return tuple.value_;
}

template <typename KeyType, typename ValueType>
static auto GetTimestamp(TupleType<KeyType, ValueType> &tuple) -> TsType & {
  return tuple.timestamp_;
}

template <typename KeyType, typename ValueType>
static auto GetKey(TupleType<KeyType, ValueType> &tuple) -> KeyType & {
  return tuple.key_;
}

template <typename KeyType, typename ValueType>
static auto GetValue(TupleType<KeyType, ValueType> &tuple) -> ValueType & {
  return tuple.value_;
}

template <typename KeyType, typename ValueType>
static auto MakeTuple(const TsType &timestamp, const KeyType &key, const ValueType &value)
    -> TupleType<KeyType, ValueType> {
  return {timestamp, key, value};
}

};  // namespace stream

#endif