#include <gtest/gtest.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>

#include "index/list.hpp"
#include "join/broadcast_window.hpp"
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
  size_t tuple_num_per_stream = 10000;

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

  size_t num_workers = 5;
  size_t window_len = 50;
  size_t channel_buffer_size = 10;
  int64_t diff = 5;
  size_t tuples_r = 10000;
  size_t tuples_s = 10000;

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