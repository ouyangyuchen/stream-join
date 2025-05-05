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
#include "stream/sequential_stream.hpp"
#include "types/types.hpp"

using stream::TsType;
using stream::TupleType;

struct TestConfig {
  static constexpr size_t WINDOW_SIZE = 400;
  static constexpr int64_t DIFF = 340;
  static constexpr size_t TUPLES_R = 10000;
  static constexpr size_t TUPLES_S = 10000;

  static constexpr size_t BROADCAST_CHANNEL_BUFFER_SIZE = 10;
  static constexpr size_t BROADCAST_WORKERS = 8;

  static constexpr size_t HANDSHAKE_CHANNEL_BUFFER_SIZE = 64;
  static constexpr size_t HANDSHAKE_WORKERS = 8;

  // broadcast window
  static constexpr size_t SUB_WINDOW_SIZE = 2;
};

TEST(WindowTest, BroadcastWindowBasic) {
  size_t tuple_num_per_stream = 3;

  auto input_chan =
      std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(TestConfig::BROADCAST_CHANNEL_BUFFER_SIZE);
  stream::BroadcastWindow<int64_t, int64_t, stream::ListIndex<int64_t, int64_t>> window(
      TestConfig::SUB_WINDOW_SIZE, TestConfig::WINDOW_SIZE, input_chan, 0, std::cout);

  // producer thread to generate tuples and send to the input channel interleavingly
  auto producer = [&input_chan, tuple_num_per_stream]() {
    TsType curr_ts = 0;
    for (int i = 0; i < tuple_num_per_stream; ++i) {
      stream::TupleType<int64_t, int64_t> tuple{curr_ts++, i, i * 10, stream::TupleFlag::INPUT_R};
      (*input_chan) << tuple;
      tuple.timestamp_ = curr_ts++;
      tuple.ctl_ = stream::TupleFlag::INPUT_S;
      (*input_chan) << tuple;
    }
    input_chan->close();
  };
  std::thread producer_thread(producer);

  // start the consumer routine
  window.Start(TestConfig::DIFF);

  // wait for the producer thread to finish
  if (producer_thread.joinable()) {
    producer_thread.join();
  }
}

TEST(WindowTest, BroadcastJoinerBasic) {
  // the **single-master** model of broadcast join should partition S stream just like not
  // partitioning, which is the same as using a single sub-window
  // -> the sum of all join results should be equal to the result of the BroadcastWindowBasic

  auto input_chan =
      std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(TestConfig::BROADCAST_CHANNEL_BUFFER_SIZE);

  std::unique_ptr<stream::SequentialStream> r = std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_R);
  std::unique_ptr<stream::SequentialStream> s = std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_S);

  stream::BroadcastJoiner<int64_t, int64_t, stream::BPlusTreeIndex<int64_t, int64_t>, stream::SequentialStream> joiner(
      TestConfig::BROADCAST_WORKERS, TestConfig::WINDOW_SIZE, TestConfig::BROADCAST_CHANNEL_BUFFER_SIZE, std::move(r),
      std::move(s), std::cout);

  joiner.Start(TestConfig::DIFF);
}

TEST(WindowTest, HandshakeJoiner) {
  // rubbish tuples are used for flushing the valid tuples inside the windows
  constexpr auto rubbish_tuple_num = TestConfig::HANDSHAKE_WORKERS * (TestConfig::WINDOW_SIZE + 1);
  std::unique_ptr<stream::SequentialStream> r =
      std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_R, 1, rubbish_tuple_num, -0x1000000000);
  std::unique_ptr<stream::SequentialStream> s =
      std::make_unique<stream::SequentialStream>(0, TestConfig::TUPLES_S, 1, rubbish_tuple_num, 0x1000000000);

  stream::HandshakeJoiner<int64_t, int64_t, stream::BPlusTreeIndex<int64_t, int64_t>> joiner(
      TestConfig::HANDSHAKE_WORKERS, TestConfig::WINDOW_SIZE, TestConfig::HANDSHAKE_CHANNEL_BUFFER_SIZE, std::move(r),
      std::move(s), std::cout);

  joiner.Start(TestConfig::DIFF);

  std::chrono::milliseconds wait_after_start{3000};
  std::this_thread::sleep_for(wait_after_start);

  spdlog::info("Stopped the joiner after {} ms timeout", wait_after_start.count());
  joiner.Stop();
}