#include <gtest/gtest.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>

#include "index/bplustree.hpp"
#include "index/list.hpp"
#include "join/broadcast_window.hpp"
#include "join/handshake_window.hpp"
#include "msd/channel.hpp"
#include "stream/random_stream.hpp"
#include "stream/sequential_stream.hpp"
#include "types/types.hpp"
#include "utils/decorator.hpp"

using stream::TsType;
using stream::TupleType;

struct TestConfig {
  static constexpr size_t WINDOW_SIZE = 40000;
  static constexpr int64_t DIFF = 340;
  static constexpr size_t TUPLES_R = 500000;
  static constexpr size_t TUPLES_S = 500000;

  static constexpr size_t BROADCAST_CHANNEL_BUFFER_SIZE = 10;
  static constexpr size_t BROADCAST_WORKERS = 8;

  static constexpr size_t HANDSHAKE_CHANNEL_BUFFER_SIZE = 64;
  static constexpr size_t HANDSHAKE_WORKERS = 8;

  // random stream
  static constexpr int64_t KEY_LOW = 0;
  static constexpr int64_t KEY_HIGH = 10;

  // broadcast window
  static constexpr size_t SUB_WINDOW_SIZE = 2;
};

TEST(WindowTest, BroadcastJoinerBasic) {
  // the **single-master** model of broadcast join should partition S stream just like not
  // partitioning, which is the same as using a single sub-window
  // -> the sum of all join results should be equal to the result of the BroadcastWindowBasic

  auto input_chan =
      std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(TestConfig::BROADCAST_CHANNEL_BUFFER_SIZE);

  auto r = std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_R);
  auto s = std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_S);
  // auto r = std::make_unique<stream::RandomStream>(TestConfig::TUPLES_R,
  //                                                 std::make_pair(TestConfig::KEY_LOW, TestConfig::KEY_HIGH));
  // auto s = std::make_unique<stream::RandomStream>(TestConfig::TUPLES_S,
  //                                                 std::make_pair(TestConfig::KEY_LOW, TestConfig::KEY_HIGH));

  stream::BroadcastJoiner<int64_t, int64_t, stream::BPlusTreeIndex<int64_t, int64_t>> joiner(
      TestConfig::BROADCAST_WORKERS, TestConfig::WINDOW_SIZE, TestConfig::BROADCAST_CHANNEL_BUFFER_SIZE, std::move(r),
      std::move(s), std::cout);

  joiner.Start(TestConfig::DIFF);
}

TEST(WindowTest, HandshakeJoiner) {
  auto r = std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_R);
  auto s = std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_S);
  // auto r = std::make_unique<stream::RandomStream>(TestConfig::TUPLES_R,
  //                                                 std::make_pair(TestConfig::KEY_LOW, TestConfig::KEY_HIGH));
  // auto s = std::make_unique<stream::RandomStream>(TestConfig::TUPLES_S,
  //                                                 std::make_pair(TestConfig::KEY_LOW, TestConfig::KEY_HIGH));

  stream::HandshakeJoiner<int64_t, int64_t, stream::BPlusTreeIndex<int64_t, int64_t>> joiner(
      TestConfig::HANDSHAKE_WORKERS, TestConfig::WINDOW_SIZE, TestConfig::HANDSHAKE_CHANNEL_BUFFER_SIZE, std::move(r),
      std::move(s), std::cout);

  joiner.Start(TestConfig::DIFF);

  decorator::printAllDurations();
}