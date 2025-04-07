#ifndef STREAM_HPP_
#define STREAM_HPP_

#include "types/types.hpp"

namespace stream {

template <typename KeyType, typename ValueType>
class Stream {
 public:
  virtual ~Stream() = default;

  // Read a tuple from the stream
  virtual auto operator>>(TupleType<KeyType, ValueType> &tuple) -> Stream<KeyType, ValueType> & = 0;

  // Check if there are more than one tuple available for reading
  virtual auto Available() -> bool = 0;

  // Check if the end of the stream is reached
  virtual auto Eof() -> bool = 0;
};
}  // namespace stream

#endif  // STREAM_HPP_