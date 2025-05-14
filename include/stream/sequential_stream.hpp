#include <cstddef>
#include <cstdint>
#include "stream/stream.hpp"

namespace stream {

class SequentialStream : public Stream<int64_t, int64_t> {
 public:
  SequentialStream(int64_t start, int64_t end, int64_t step = 1) : start_(start), end_(end), step_(step) {
    if (step <= 0) {
      throw std::invalid_argument("step must be greater than 0");
    }
  }

  auto Available() -> bool override { return start_ < end_; }

  auto Eof() -> bool override { return start_ >= end_; }

  auto operator>>(TupleType<int64_t, int64_t> &tuple) -> Stream<int64_t, int64_t> & override {
    if (start_ < end_) {
      tuple.timestamp_ = start_;
      tuple.key_ = start_;
      tuple.value_ = start_;
      start_ += step_;
    } else {
      throw std::runtime_error("End of stream reached");
    }
    return *this;
  }

 private:
  int64_t start_;
  int64_t end_;
  int64_t step_;
};

}  // namespace stream