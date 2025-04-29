#include <cstddef>
#include <cstdint>
#include "stream/stream.hpp"

namespace stream {

class SequentialStream : public Stream<int64_t, int64_t> {
 public:
  SequentialStream(int64_t start, int64_t end, int64_t step = 1, size_t tail_rubbish_tuples = 0,
                   int64_t rubbish_key = -0x1000000000)
      : start_(start), end_(end), step_(step), tail_rubbish_tuples_(tail_rubbish_tuples), rubbish_key_(rubbish_key) {
    if (step <= 0) {
      throw std::invalid_argument("step must be greater than 0");
    }
  }

  auto Available() -> bool override { return start_ < end_ || tail_rubbish_tuples_ > 0; }

  auto Eof() -> bool override { return start_ >= end_ && tail_rubbish_tuples_ == 0; }

  auto operator>>(TupleType<int64_t, int64_t> &tuple) -> Stream<int64_t, int64_t> & override {
    if (start_ < end_) {
      tuple.timestamp_ = start_;
      tuple.key_ = start_;
      tuple.value_ = start_;
      start_ += step_;
    } else {
      if (tail_rubbish_tuples_ > 0) {
        tuple.timestamp_ = start_++;
        tuple.key_ = rubbish_key_++;  // avoid duplicate key to fail the insertion of index
        tuple.value_ = tuple.key_;
        --tail_rubbish_tuples_;
      } else {
        throw std::runtime_error("No more tuples available");
      }
    }
    return *this;
  }

 private:
  int64_t start_;
  int64_t end_;
  int64_t step_;
  size_t tail_rubbish_tuples_;
  int64_t rubbish_key_;
};

}  // namespace stream