#include <gtest/gtest.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>

#include "index/list.hpp"
#include "join/broadcast_window.hpp"
#include "join/handshake_window.hpp"
#include "msd/channel.hpp"
#include "stream/sequential_stream.hpp"
#include "types/types.hpp"

using stream::TsType;
using stream::TupleType;

TEST(WindowTest, BroadcastWindowBasic) {
  size_t channel_buffer_size = 10;
  size_t window_size = 10;
  size_t sub_window_size = 2;
  int64_t diff = 5;
  size_t tuple_num_per_stream = 1;

  auto input_chan =
      std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(channel_buffer_size);
  stream::BroadcastWindow<int64_t, int64_t, stream::ListIndex<int64_t, int64_t>> window(
      sub_window_size, window_size, input_chan, 0, std::cout);

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
  window.Start(diff);

  // wait for the producer thread to finish
  if (producer_thread.joinable()) {
    producer_thread.join();
  }
}

TEST(WindowTest, BroadcastJoinerBasic) {
  // the **single-master** model of broadcast join should partition S stream just like not
  // partitioning, which is the same as using a single sub-window
  // -> the sum of all join results should be equal to the result of the BroadcastWindowBasic

  size_t num_workers = 2;
  size_t window_len = 4;
  size_t channel_buffer_size = 10;
  int64_t diff = 2;
  size_t tuples_r = 1000;
  size_t tuples_s = 1000;

  auto input_chan =
      std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(channel_buffer_size);

  std::unique_ptr<stream::SequentialStream> r =
      std::make_unique<stream::SequentialStream>(0, tuples_r);
  std::unique_ptr<stream::SequentialStream> s =
      std::make_unique<stream::SequentialStream>(0, tuples_s);

  stream::BroadcastJoiner<int64_t, int64_t, stream::ListIndex<int64_t, int64_t>,
                          stream::SequentialStream>
      joiner(num_workers, window_len, channel_buffer_size, std::move(r), std::move(s));

  joiner.Start(diff);
}

TEST(WindowTest, HandshakeWindowBasic) {
  size_t channel_buffer_size = 10;
  size_t window_size = 1;
  size_t forward_threshold = 1;
  int64_t diff = 5;
  size_t tuple_num_per_stream = 5;

  auto input_chan =
      std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(channel_buffer_size);
  stream::HandshakeWindow<int64_t, int64_t, stream::ListIndex<int64_t, int64_t>> window(
      window_size, forward_threshold, input_chan, nullptr, nullptr, 0, std::cout);

  // producer thread to generate tuples and send to the input channel interleavingly
  auto producer = [&input_chan, tuple_num_per_stream]() {
    TsType curr_ts = 0;
    for (int i = 0; i < tuple_num_per_stream; ++i) {
      stream::TupleType<int64_t, int64_t> tuple{curr_ts++, i, i, stream::TupleFlag::INPUT_R};
      (*input_chan) << tuple;
      tuple.timestamp_ = curr_ts++;
      tuple.ctl_ = stream::TupleFlag::INPUT_S;
      (*input_chan) << tuple;
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    input_chan->close();  // prevent the ForwardTuples() send ack to the null channel
  };
  std::thread producer_thread(producer);

  window.Start(diff);

  if (producer_thread.joinable()) {
    producer_thread.join();
  }
}

TEST(WindowTest, HandshakeJoiner) {
  size_t num_workers = 2;
  size_t window_len = 2;
  size_t channel_buffer_size = 1024;
  int64_t diff = 2;
  size_t tuples_r = 1000;
  size_t tuples_s = 1000;

  // rubbish tuples are used for flushing the valid tuples inside the windows
  std::unique_ptr<stream::SequentialStream> r = std::make_unique<stream::SequentialStream>(
      0, tuples_r, 1, num_workers * window_len, -0x1000000000);
  std::unique_ptr<stream::SequentialStream> s = std::make_unique<stream::SequentialStream>(
      0, tuples_s, 1, num_workers * window_len, 0x1000000000);

  stream::HandshakeJoiner<int64_t, int64_t, stream::ListIndex<int64_t, int64_t>> joiner(
      num_workers, window_len, channel_buffer_size, std::move(r), std::move(s), std::cout);

  joiner.Start(diff);
}