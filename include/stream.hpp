#ifndef STREAM_HPP_
#define STREAM_HPP_

#include "utils.hpp"

namespace stream {

template <typename KeyType, typename ValueType>
class Stream {
 public:
  virtual ~Stream() = default;

  // Read a tuple from the stream
  virtual Stream<KeyType, ValueType> &operator>>(TupleType<KeyType, ValueType> &tuple) = 0;

  // Check if there are more than one tuple available for reading
  virtual auto available() -> bool = 0;

  // Check if the end of the stream is reached
  virtual auto eof() -> bool = 0;
};
}  // namespace stream

#endif  // STREAM_HPP_