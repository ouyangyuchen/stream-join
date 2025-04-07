#ifndef STREAMJOIN_TYPES_HPP
#define STREAMJOIN_TYPES_HPP

#include <cstdint>

namespace stream {
using TsType = int64_t;  // timestamp type

enum class TupleFlag {
  INPUT_R,  // input tuple is from R stream
  INPUT_S,  // input tuple is from S stream
};

template <typename KeyType, typename ValueType>
struct TupleType {
  TsType timestamp_;  // unique
  KeyType key_;
  ValueType value_;

  TupleFlag ctl_ = TupleFlag::INPUT_R;  // control/metainfo of this tuple
};

};  // namespace stream

#endif