#include <cstdint>
#include "stream/stream.hpp"

namespace stream {

class SequentialStream : public Stream<int64_t, int64_t> {
 public:
  SequentialStream(int64_t start, int64_t end) : start_(start), end_(end) {}

  auto Available() -> bool override { return start_ < end_; }

  auto Eof() -> bool override { return start_ >= end_; }

  auto operator>>(TupleType<int64_t, int64_t> &tuple) -> Stream<int64_t, int64_t> & override {
    tuple.timestamp_ = start_;
    tuple.key_ = start_;
    tuple.value_ = start_;
    ++start_;
    return *this;
  }

 private:
  int64_t start_;
  int64_t end_;
};

}  // namespace stream