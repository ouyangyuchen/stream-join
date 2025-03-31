#include <gtest/gtest.h>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <utility>

#include "index_list.hpp"
#include "random_stream.hpp"
#include "tpc_stream.hpp"
#include "utils.hpp"
#include "window.hpp"

TEST(WindowTest, SubWindowFlowThrough) {
  // Create a channel for the subwindow
  auto input_chan = std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(10);
  auto output_chan = std::make_shared<msd::channel<stream::TupleType<int64_t, int64_t>>>(10);

  // Create a subwindow with no buffer, which acts as a pipe
  stream::SubWindow<int64_t, int64_t, stream::ListIndex<int64_t, int64_t>> subwindow(
      0, input_chan, nullptr, nullptr, output_chan);

  // Start the flow through the subwindow in a separate thread
  std::thread flow_thread([&subwindow]() { subwindow.FlowThrough(); });
  flow_thread.detach();

  // Send some tuples to the channel, should be seen at the output channel
  for (int i = 0; i < 10; ++i) {
    auto tuple = stream::MakeTuple<int64_t, int64_t>(i, i + 1, i * 10);
    (*input_chan) << tuple;
  }
  for (int i = 0; i < 10; ++i) {
    stream::TupleType<int64_t, int64_t> tuple;
    *output_chan >> tuple;
    ASSERT_EQ(stream::GetTimestamp(tuple), i);
    ASSERT_EQ(stream::GetKey(tuple), i + 1);
    ASSERT_EQ(stream::GetValue(tuple), i * 10);
  }
  ASSERT_TRUE(output_chan->empty());
  ASSERT_FALSE(output_chan->closed());

  // send more tuples to the channel, close the channel, should see the tuples and closed flag
  for (int i = 10; i < 20; ++i) {
    auto tuple = stream::MakeTuple<int64_t, int64_t>(i, i + 1, i * 10);
    (*input_chan) << tuple;
  }
  input_chan->close();
  for (int i = 10; i < 20; ++i) {
    stream::TupleType<int64_t, int64_t> tuple;
    *output_chan >> tuple;
    ASSERT_EQ(stream::GetTimestamp(tuple), i);
    ASSERT_EQ(stream::GetKey(tuple), i + 1);
    ASSERT_EQ(stream::GetValue(tuple), i * 10);
  }
  ASSERT_TRUE(output_chan->empty());
  std::this_thread::sleep_for(
      std::chrono::milliseconds(100));  // flow thread should close the channel
  ASSERT_TRUE(output_chan->closed());

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
    for (auto &tuple : *endpoint) {
      ASSERT_EQ(stream::GetTimestamp(tuple), ts);
      ASSERT_GE(stream::GetKey(tuple), range.first);
      ASSERT_LE(stream::GetKey(tuple), range.second);
      ASSERT_EQ(stream::GetKey(tuple), stream::GetValue(tuple));
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
    for (auto &tuple : *endpoint) {
      ASSERT_EQ(stream::GetTimestamp(tuple), ts);
      // ASSERT_EQ(stream::GetKey(tuple), ts);
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