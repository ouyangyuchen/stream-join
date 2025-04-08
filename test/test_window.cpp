#include <gtest/gtest.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>

#include "index/list.hpp"
#include "join/broadcast_window.hpp"
#include "join/window.hpp"
#include "msd/channel.hpp"
#include "stream/random_stream.hpp"
#include "stream/sequential_stream.hpp"
#include "stream/tpc_stream.hpp"
#include "types/types.hpp"

using stream::TsType;
using stream::TupleType;

TEST(WindowTest, SubWindowFlowThrough) {
  // Create a channel for the subwindow
  auto input_chan = std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(10);
  auto output_chan = std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(10);

  // Create a subwindow with no buffer, which acts as a pipe
  stream::SubWindow<int64_t, int64_t, stream::ListIndex<int64_t, int64_t>> subwindow(
      0, input_chan, nullptr, output_chan);

  // Start the flow through the subwindow in a separate thread
  std::thread flow_thread([&subwindow]() { subwindow.FlowThrough(); });
  flow_thread.detach();

  // Send some tuples to the channel, should be seen at the output channel
  for (int i = 0; i < 10; ++i) {
    TupleType<int64_t, int64_t> tuple{i, i + 1, i * 10};
    (*input_chan) << tuple;
  }
  for (int i = 0; i < 10; ++i) {
    TupleType<int64_t, int64_t> tuple;
    *output_chan >> tuple;
    ASSERT_EQ(tuple.timestamp_, i);
    ASSERT_EQ(tuple.key_, i + 1);
    ASSERT_EQ(tuple.value_, i * 10);
  }
  ASSERT_TRUE(output_chan->empty());
  ASSERT_FALSE(output_chan->closed());

  // send more tuples to the channel, close the channel, should see the tuples and closed flag
  for (int i = 10; i < 20; ++i) {
    TupleType<int64_t, int64_t> tuple{i, i + 1, i * 10};
    (*input_chan) << tuple;
  }
  input_chan->close();

  TsType ts = 10;
  for (const auto &tuple : *output_chan) {
    ASSERT_EQ(tuple.timestamp_, ts);
    ASSERT_EQ(tuple.key_, ts + 1);
    ASSERT_EQ(tuple.value_, ts * 10);
    ts++;
  }
  ASSERT_EQ(ts, 20);
  ASSERT_TRUE(output_chan->closed());
  ASSERT_TRUE(output_chan->empty());

  // Wait for the flow thread to finish
  if (flow_thread.joinable()) {
    flow_thread.join();
  }
}

TEST(WindowTest, WindowFlowRandomStream) {
  size_t num_workers = 10;
  size_t window_size = 0;
  size_t channel_buffer_size = 10;
  auto endpoint =
      std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(channel_buffer_size);

  size_t num_tuples = 1000;
  std::pair<int64_t, int64_t> range(0, 100);
  stream::RandomStream stream(num_tuples, range);

  stream::SlidingWindow<int64_t, int64_t, stream::ListIndex<int64_t, int64_t>, stream::RandomStream>
      window(num_workers, window_size, channel_buffer_size, stream, endpoint);

  // watcher pop tuples from the endpoint and check the tuples
  auto watcher = [&endpoint, range, num_tuples]() {
    stream::TupleType<int64_t, int64_t> tuple;
    stream::TsType ts = 0;
    for (const auto &tuple : *endpoint) {
      ASSERT_EQ(tuple.timestamp_, ts);
      ASSERT_GE(tuple.key_, range.first);
      ASSERT_LE(tuple.key_, range.second);
      ASSERT_EQ(tuple.key_, tuple.value_);
      ts++;
    }
    ASSERT_EQ(ts, num_tuples);
  };

  std::thread watcher_thread(watcher);

  window.Start();

  if (watcher_thread.joinable()) {
    watcher_thread.join();
  }
}

TEST(WindowTest, WindowFlowFileStream) {
  size_t num_workers = 10;
  size_t window_size = 0;
  size_t channel_buffer_size = 10;
  auto endpoint =
      std::make_shared<msd::channel<stream::TupleType<int64_t, std::string>>>(channel_buffer_size);

  stream::TPCStream stream("../data/tpc-h/customer.tbl", 1, 2);
  size_t num_tuples = 150000;

  stream::SlidingWindow<int64_t, std::string, stream::ListIndex<int64_t, std::string>,
                        stream::TPCStream>
      window(num_workers, window_size, channel_buffer_size, stream, endpoint);

  auto watcher = [&endpoint, num_tuples]() {
    stream::TupleType<int64_t, std::string> tuple;
    stream::TsType ts = 0;
    for (const auto &tuple : *endpoint) {
      ASSERT_EQ(tuple.timestamp_, ts);
      // ASSERT_EQ(tuple.key_, ts);
      ts++;
    }
    ASSERT_EQ(ts, num_tuples);
  };

  std::thread watcher_thread(watcher);

  window.Start();

  if (watcher_thread.joinable()) {
    watcher_thread.join();
  }
}

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
  size_t window_len = 10;
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