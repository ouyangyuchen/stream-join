#include <gtest/gtest.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>

#include "index/alexmap.hpp"
#include "index/bplustree.hpp"
#include "index/list.hpp"
#include "index/pgm.hpp"
#include "join/broadcast_join.hpp"
#include "join/handshake_join.hpp"
#include "msd/channel.hpp"
#include "stream/random_stream.hpp"
#include "stream/sequential_stream.hpp"
#include "types/types.hpp"
#include "utils/decorator.hpp"

using stream::TsType;
using stream::TupleType;

struct TestConfig {
  static constexpr size_t WINDOW_SIZE = 4000;
  static constexpr int64_t DIFF = 200;
  static constexpr size_t TUPLES_R = 200000;
  static constexpr size_t TUPLES_S = 200000;

  static constexpr size_t BROADCAST_CHANNEL_BUFFER_SIZE = 128;
  static constexpr size_t BROADCAST_WORKERS = 8;

  static constexpr size_t HANDSHAKE_CHANNEL_BUFFER_SIZE = 128;
  static constexpr size_t HANDSHAKE_WORKERS = 8;
};

TEST(JoinTest, BroadcastJoinerBasic) {
  // the **single-master** model of broadcast join should partition S stream just like not
  // partitioning, which is the same as using a single sub-window
  // -> the sum of all join results should be equal to the result of the BroadcastWindowBasic

  auto input_chan =
      std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(TestConfig::BROADCAST_CHANNEL_BUFFER_SIZE);

  auto r = std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_R);
  auto s = std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_S);
  // auto r = std::make_unique<stream::RandomStream>(TestConfig::TUPLES_R);
  // auto s = std::make_unique<stream::RandomStream>(TestConfig::TUPLES_S);

  stream::BroadcastJoiner<int64_t, int64_t, stream::BPlusTreeIndex<int64_t, int64_t>> joiner(
      TestConfig::BROADCAST_WORKERS, TestConfig::WINDOW_SIZE, TestConfig::BROADCAST_CHANNEL_BUFFER_SIZE, std::move(r),
      std::move(s));

  joiner.Start(TestConfig::DIFF);
}

TEST(JoinTest, HandshakeJoiner) {
  auto r = std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_R);
  auto s = std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_S);
  // auto r = std::make_unique<stream::RandomStream>(TestConfig::TUPLES_R);
  // auto s = std::make_unique<stream::RandomStream>(TestConfig::TUPLES_S);

  stream::HandshakeJoiner<int64_t, int64_t, stream::BPlusTreeIndex<int64_t, int64_t>> joiner(
      TestConfig::HANDSHAKE_WORKERS, TestConfig::WINDOW_SIZE, TestConfig::HANDSHAKE_CHANNEL_BUFFER_SIZE, std::move(r),
      std::move(s), std::cout);

  // joiner.StartWatcher();
  joiner.Start(TestConfig::DIFF);

  decorator::printAllDurations();
}