/*
stream.hpp

Stream interface for reading from a sequence of key, value pairs,
the length of stream is not known in advance and could be infinite.
*/

#pragma once

#include <cstdint>

namespace stream {
typedef typename std::uint64_t TsType;  // timestamp: auto incrementing integer / ID

template <typename KeyType, typename ValueType>
class Stream {
 public:
  virtual ~Stream() = default;

  // Read the next key, value pair from the stream with the current timestamp.
  // Returns true if the next key, value pair is read successfully, false otherwise.
  virtual auto Read(TsType &timestamp, KeyType &key, ValueType &value) -> bool = 0;
};
}  // namespace stream