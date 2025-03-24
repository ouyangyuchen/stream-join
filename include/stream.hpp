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
  virtual auto read(TsType &timestamp, KeyType &key, ValueType &value) -> bool = 0;

  // Check if the stream is ready to read the next key, value pair non-blockingly.
  virtual auto available() -> bool = 0;

  // Check if the stream has reached the end.
  virtual auto eof() -> bool = 0;
};
}  // namespace stream