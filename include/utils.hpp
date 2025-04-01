#ifndef UTILS_HPP
#define UTILS_HPP

#include <cstdint>

namespace stream {
using TsType = int64_t;  // timestamp type

template <typename KeyType, typename ValueType>
struct TupleType {
  TsType timestamp_;
  KeyType key_;
  ValueType value_;
};

};  // namespace stream

#endif