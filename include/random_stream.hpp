#ifndef RANDOM_STREAM_HPP_
#define RANDOM_STREAM_HPP_

#include <random>
#include "stream.hpp"
#include "utils.hpp"

namespace stream {

// RandomStream generates random tuples with a specified key range and timestamp range
class RandomStream : public Stream<int32_t, int32_t> {
 public:
  RandomStream(const TsType end_timestamp, const std::pair<int32_t, int32_t> &key_range)
      : start_timestamp_(0),
        end_timestamp_(end_timestamp),
        key_range_(key_range),
        generator_(std::random_device()()) {
    if (key_range_.first > key_range_.second) {
      throw std::invalid_argument("Invalid key range");
    }
  }

  RandomStream &operator>>(TupleType<int32_t, int32_t> &tuple) override {
    if (this->eof()) throw std::runtime_error("End of stream reached");
    // generate a random number within the key range
    int32_t key = generator_() % (key_range_.second - key_range_.first + 1) + key_range_.first;
    tuple = make_tuple(start_timestamp_++, key, key);
    return *this;
  }

  auto available() -> bool override { return start_timestamp_ < end_timestamp_; }

  auto eof() -> bool override { return start_timestamp_ >= end_timestamp_; }

 private:
  TsType start_timestamp_;
  TsType end_timestamp_;
  std::pair<int32_t, int32_t> key_range_;
  std::mt19937 generator_;
};

}  // namespace stream

#endif  // RANDOM_STREAM_HPP_