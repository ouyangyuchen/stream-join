#ifndef WINDOW_HPP_
#define WINDOW_HPP_

#include <cassert>
#include <iostream>
#include <memory>
#include <ostream>
#include <thread>
#include <type_traits>
#include <vector>
#include "index.hpp"
#include "msd/channel.hpp"
#include "stream.hpp"
#include "utils.hpp"

namespace stream {

template <typename KeyType, typename ValueType, typename Container>
struct SubWindow {
  static_assert(std::is_base_of_v<WindowIndex<KeyType, ValueType>, Container>,
                "Container must be derived from Index<KeyType, ValueType>");

 public:
  using ChannelPointer = std::shared_ptr<msd::channel<TupleType<KeyType, ValueType>>>;

  SubWindow(size_t window_size, ChannelPointer input_prev, ChannelPointer input_next,
            ChannelPointer output_prev, ChannelPointer output_next, int32_t id = -1,
            std::ostream &os = std::cout)
      : window_size_(window_size),
        input_prev_(input_prev),
        input_next_(input_next),
        output_prev_(output_prev),
        output_next_(output_next),
        id_(id),
        os_(os) {}

  ~SubWindow() = default;

  SubWindow(const SubWindow &) = delete;
  auto operator=(const SubWindow &) -> SubWindow & = delete;
  SubWindow(SubWindow &&) = default;
  auto operator=(SubWindow &&) -> SubWindow & = default;

  /**
   * @brief Transfer and process tuples in the sub-window until the input is closed.
   * @details The sub-window is a fixed-size sliding window that stores tuples. The tuples are
   * transferred between the neighbor sub-windows using channels. The tuples are popped from the
   * input channel from the previous one and pushed to the output channel to the next one.
   * The tuples are also processed in the sub-window (todo).
   */
  auto FlowThrough() -> void;

  const size_t window_size_;  // fixed number of tuples stored in the sub-window

  Container index_{};  // stored tuples

  // transfer tuples between neighbor sub-windows/threads
  // input_prev ---> current sub-window ---> output_next
  // output_prev <--- current sub-window <--- input_next
  ChannelPointer input_prev_;   // not null
  ChannelPointer input_next_;   // null if this is the tail sub-window
  ChannelPointer output_prev_;  // null if this is the head sub-window
  ChannelPointer output_next_;  // could be null

  std::ostream &os_;  // output join results

  // debug:
  int32_t id_;
};

template <typename KeyType, typename ValueType, typename Container, typename StreamType>
class SlidingWindow {
  static_assert(std::is_base_of_v<WindowIndex<KeyType, ValueType>, Container>,
                "Container must be derived from Index<KeyType, ValueType>");
  static_assert(std::is_base_of_v<Stream<KeyType, ValueType>, StreamType>,
                "StreamType must be derived from Stream<KeyType, ValueType>");

 public:
  using ChannelPointer = std::shared_ptr<msd::channel<TupleType<KeyType, ValueType>>>;

  /**
   * @brief SlidingWindow constructor.
   * @param num_workers number of threads == number of subwindows
   * @param window_size total number of tuples in the sliding window
   * @param channel_buffer_size size of the channel buffer
   * @param stream underlying input stream
   * @param endpoint endpoint of the stream that tuples are pushed to
   */
  SlidingWindow(size_t num_workers, size_t window_size, size_t channel_buffer_size,
                StreamType &stream, ChannelPointer endpoint = nullptr);
  ~SlidingWindow();

  SlidingWindow(const SlidingWindow &) = delete;
  auto operator=(const SlidingWindow &) -> SlidingWindow & = delete;
  SlidingWindow(SlidingWindow &&) = default;
  auto operator=(SlidingWindow &&) -> SlidingWindow & = default;

  /**
   * @brief Start pushing tuples from the stream to the sub-windows.
   * @details The stream is divided into sub-windows, each of which is processed by a separate
   * thread. The tuples are pushed to the sub-windows at the head of the sliding window. The
   * sub-windows are connected by channels, which are used to transfer tuples between the neighbor
   * sub-windows.
   */
  auto Start() -> void;

 private:
  const size_t num_workers_{};          // number of threads == number of subwindows
  const size_t window_size_{};          // total number of tuples in the sliding window
  const size_t channel_buffer_size_{};  // size of the channel buffer

  std::vector<SubWindow<KeyType, ValueType, Container>> subwindows_;
  std::vector<std::thread> threads_;  // threads for each sub-window

  // stream --> [windows] ---> endpoint (---> aggregate function)
  StreamType &stream_;
};

};  // namespace stream

template <typename KeyType, typename ValueType, typename Container, typename StreamType>
stream::SlidingWindow<KeyType, ValueType, Container, StreamType>::SlidingWindow(
    size_t num_workers, size_t window_size, size_t channel_buffer_size, StreamType &stream,
    ChannelPointer endpoint)
    : num_workers_(num_workers),
      window_size_(window_size),
      channel_buffer_size_(channel_buffer_size),
      stream_(stream) {
  assert(num_workers_ > 0);
  assert(window_size % num_workers_ == 0);
  // create next and previous channels
  std::vector<ChannelPointer> next_chans(num_workers_);
  std::vector<ChannelPointer> prev_chans(num_workers_ - 1);
  for (size_t i = 0; i < num_workers_; ++i) {
    next_chans[i] =
        std::make_shared<msd::channel<TupleType<KeyType, ValueType>>>(channel_buffer_size);
  }
  for (size_t i = 0; i < num_workers_ - 1; ++i) {
    prev_chans[i] =
        std::make_shared<msd::channel<TupleType<KeyType, ValueType>>>(channel_buffer_size);
  }

  // create subwindows with connected channels
  size_t subwindow_size = window_size_ / num_workers_;
  for (size_t i = 0; i < num_workers_; ++i) {
    auto left_input = next_chans[i];
    auto left_output = (i == 0) ? nullptr : prev_chans[i - 1];
    auto right_input = (i == num_workers_ - 1) ? nullptr : prev_chans[i];
    auto right_output = (i == num_workers_ - 1) ? endpoint : next_chans[i + 1];

    subwindows_.emplace_back(subwindow_size, left_input, right_input, left_output, right_output, i);
  }

  // create threads for each subwindow to flow tuples
  for (size_t i = 0; i < num_workers_; ++i) {
    auto &subwindow = subwindows_[i];
    auto thread_func = [this, &subwindow]() {
      try {
        subwindow.FlowThrough();
      } catch (const std::exception &e) {
        std::cerr << "Error in subwindow " << subwindow.id_ << ": " << e.what() << std::endl;
      }
    };
    threads_.emplace_back(thread_func);
    threads_[i].detach();
  }
}

template <typename KeyType, typename ValueType, typename Container, typename StreamType>
stream::SlidingWindow<KeyType, ValueType, Container, StreamType>::~SlidingWindow() {
  for (auto &thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

template <typename KeyType, typename ValueType, typename Container, typename StreamType>
auto stream::SlidingWindow<KeyType, ValueType, Container, StreamType>::Start() -> void {
  while (!stream_.Eof()) {
    TupleType<KeyType, ValueType> tuple;
    stream_ >> tuple;
    *(subwindows_[0].input_prev_) << tuple;  // push the tuple to the first subwindow
  }
  subwindows_[0].input_prev_->close();  // start chained closing
}

template <typename KeyType, typename ValueType, typename Container>
auto stream::SubWindow<KeyType, ValueType, Container>::FlowThrough() -> void {
  for (auto &tuple : *input_prev_) {
    index_.Insert(tuple);

    if (index_.Size() > window_size_) {
      auto tuple_expired = index_.PopOldest();
      if (output_next_) {
        *output_next_ << tuple_expired;
      }
    }
  }
  if (output_next_) {
    output_next_->close();
  }
}

#endif