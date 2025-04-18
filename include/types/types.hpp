#ifndef STREAMJOIN_TYPES_HPP
#define STREAMJOIN_TYPES_HPP

#include <fmt/format.h>
#include <cstdint>
#include <ostream>
#include <string>
#include "msd/channel.hpp"

namespace stream {

// Tuple type for the stream join
using TsType = int64_t;  // timestamp type

enum class TupleFlag {
  INVALID,  // invalid tuple (init state)
  INPUT_R,  // input tuple is from R stream
  INPUT_S,  // input tuple is from S stream
  ACK_S,    // ack message for S tuple (used in handshake join)
};

template <typename KeyType, typename ValueType>
struct TupleType {
  TsType timestamp_;  // unique
  KeyType key_;
  ValueType value_;

  TupleFlag ctl_ = TupleFlag::INVALID;  // control/metainfo of this tuple

  // handshake join flags
  bool forwarded_ = false;
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
    case TupleFlag::ACK_S:  // Added case for ACK_S if needed by operator<<
      ctl_str = "ACK_S";
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

// --- fmt::formatter specialization ---
// Needs to be outside the stream namespace
template <typename KeyType, typename ValueType>
struct fmt::formatter<stream::TupleType<KeyType, ValueType>> {
  // Presentation format: 's' - short, 'l' - long (default)
  char presentation_ = 'l';

  // Parses format specifications.
  constexpr auto parse(format_parse_context &ctx) -> decltype(ctx.begin()) {
    auto it = ctx.begin();
    auto end = ctx.end();
    if (it != end && (*it == 's' || *it == 'l')) {
      presentation_ = *it++;
    }

    if (it != end && *it != '}') {
      throw format_error("invalid format");
    }
    return it;
  }

  // Formats the TupleType
  template <typename FormatContext>
  auto format(const stream::TupleType<KeyType, ValueType> &tuple, FormatContext &ctx) const
      -> decltype(ctx.out()) {
    std::string ctl_str;
    switch (tuple.ctl_) {
      case stream::TupleFlag::INPUT_R:
        ctl_str = "R";
        break;
      case stream::TupleFlag::INPUT_S:
        ctl_str = "S";
        break;
      case stream::TupleFlag::ACK_S:
        ctl_str = "ACK_S";
        break;
      default:
        ctl_str = "UNKNOWN";
        break;
    }

    if (presentation_ == 's') {
      return fmt::format_to(ctx.out(), "T({},{},{})", tuple.timestamp_, tuple.key_, ctl_str);
    }
    return fmt::format_to(ctx.out(), "(ts={}, key={}, {})", tuple.timestamp_, tuple.key_, ctl_str);
  }
};

#endif  // STREAMJOIN_TYPES_HPP