#ifndef RANDOM_STREAM_HPP_
#define RANDOM_STREAM_HPP_

#include <random>
#include "stream/stream.hpp"
#include "types/types.hpp"

namespace stream {

// RandomStream generates random tuples with unique keys from [0, N), N is the number of tuples.
class RandomStream : public Stream<int64_t, int64_t> {
 public:
  RandomStream(const TsType end_timestamp, const TsType start_timestamp = 0)
      : end_timestamp_(end_timestamp), start_timestamp_(start_timestamp), generator_(std::random_device()()) {
    if (end_timestamp < start_timestamp_) {
      throw std::invalid_argument("End timestamp must be greater or equal than start timestamp");
    }
    size_t num_tuples = end_timestamp - start_timestamp_;
    keys_.reserve(num_tuples);
    for (size_t i = 0; i < num_tuples; ++i) {
      keys_.push_back(static_cast<int64_t>(i));
    }
    std::shuffle(keys_.begin(), keys_.end(), generator_);
  }

  auto operator>>(TupleType<int64_t, int64_t> &tuple) -> RandomStream & override {
    if (this->Eof()) {
      throw std::runtime_error("End of stream reached");
    }
    auto key = keys_[current_index_++];
    tuple = {start_timestamp_++, key, key};
    return *this;
  }

  auto Available() -> bool override { return current_index_ < keys_.size(); }

  auto Eof() -> bool override { return current_index_ >= keys_.size(); }

 private:
  const TsType end_timestamp_;
  TsType start_timestamp_;

  std::vector<int64_t> keys_;
  size_t current_index_{0};
  std::mt19937 generator_;
};

}  // namespace stream

#endif  // RANDOM_STREAM_HPP_