#include "random_stream.hpp"
#include "stream.hpp"

stream::RandomStream::RandomStream(const TsType end_timestamp,
                                   const std::pair<int32_t, int32_t> &key_range)
    : start_timestamp_(0),
      end_timestamp_(end_timestamp),
      key_range_(key_range),
      generator_(std::random_device()()) {
  if (key_range_.first > key_range_.second) {
    throw std::invalid_argument("Invalid key range");
  }
}

auto stream::RandomStream::Read(TsType &timestamp, int32_t &key, int32_t &value) -> bool {
  if (start_timestamp_ >= end_timestamp_) {
    return false;
  }
  timestamp = start_timestamp_++;
  key = generator_() % (key_range_.second - key_range_.first + 1) + key_range_.first;
  value = key;
  return true;
}