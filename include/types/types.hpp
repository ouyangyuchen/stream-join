#ifndef STREAMJOIN_TYPES_HPP
#define STREAMJOIN_TYPES_HPP

#include <cstdint>
#include <ostream>
#include "msd/channel.hpp"

namespace stream {

// Tuple type for the stream join
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

template <typename KeyType, typename ValueType>
auto operator<<(std::ostream &os, const TupleType<KeyType, ValueType> &tuple) -> std::ostream & {
  std::string ctl_str;
  switch (tuple.ctl_) {
    case TupleFlag::INPUT_R:
      ctl_str = "R";
      break;
    case TupleFlag::INPUT_S:
      ctl_str = "S";
      break;
    default:
      ctl_str = "UNKNOWN";
  }

  os << "(ts = " << tuple.timestamp_ << ", key = " << tuple.key_ << ", value = " << tuple.value_
     << ", " << ctl_str << ")";
  return os;
}

// Channel
template <typename KeyType, typename ValueType>
using Channel = msd::channel<TupleType<KeyType, ValueType>>;

template <typename KeyType, typename ValueType>
using ChannelPointer = std::shared_ptr<Channel<KeyType, ValueType>>;

}  // namespace stream

#endif