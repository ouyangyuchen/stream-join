/*
random_stream.hpp

Stream implementation for reading from a sequence of integers (key == value),
whose keys are uniformly and randomly generated from a given range.
*/

#pragma once

#include <cstdint>
#include <random>
#include <utility>
#include "stream.hpp"

namespace stream {
class RandomStream : public Stream<int32_t, int32_t> {
 public:
  RandomStream(const TsType end_timestamp, const std::pair<int32_t, int32_t> &key_range);

  auto read(TsType &timestamp, int32_t &key, int32_t &value) -> bool override;
  auto available() -> bool override;
  auto eof() -> bool override;

 private:
  TsType start_timestamp_;
  TsType end_timestamp_;
  std::pair<int32_t, int32_t> key_range_;
  std::mt19937 generator_;
};
}  // namespace stream
