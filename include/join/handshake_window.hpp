#ifndef HANDSHAKE_WINDOW_HPP
#define HANDSHAKE_WINDOW_HPP

#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <ostream>
#include <thread>
#include <vector>
#include "index/index.hpp"
#include "stream/stream.hpp"
#include "types/types.hpp"

namespace stream {

/**
 * @brief HandshakeWindow class.
 * @details The HandshakeWindow class maintains index of 2 streams (R, S). When it receives a tuple
 * r, it searches in the local sub-index I_s, update the total index I_r and pop the expired r
 * tuple to the right channel. When it receives a tuple s, it searches in the local sub-index I_r
 * and update the index I_s. The window will defer deleting the expired s tuples until the
 * corresponding ack messages are received, which are sent by the left sub-window after it has
 * received the tuple s. The join results are sent to the output stream.
 */
template <typename KeyType, typename ValueType, typename Container>
class HandshakeWindow {
  static_assert(std::is_base_of_v<WindowIndex<KeyType, ValueType>, Container>,
                "Container must be derived from Index<KeyType, ValueType>");

 public:
  HandshakeWindow(size_t window_size, size_t forward_threshold,
                  ChannelPointer<KeyType, ValueType> input_chan,
                  ChannelPointer<KeyType, ValueType> output_left_chan,
                  ChannelPointer<KeyType, ValueType> output_right_chan, int32_t id = -1,
                  std::ostream &os = std::cout)
      : window_size_r_(window_size),
        window_size_s_(window_size),
        forward_threshold_(forward_threshold),
        output_left_chan_(output_left_chan),
        output_right_chan_(output_right_chan),
        input_chan_(input_chan),
        index_r_(std::make_unique<Container>()),
        index_s_(std::make_unique<Container>()),
        id_(id),
        os_(os) {
    if (input_chan_ == nullptr) {
      throw std::invalid_argument("input channel must not be null");
    }
  }

  /**
   * @brief Read tuples from the input channel and process them.
   * @param diff join condition: |r.key - s.key| <= diff
   * @details The function reads tuples from the input channel and processes them based on their
   * attributes, updating the indexes and sending results to the output channels as necessary.
   * It also handles the forwarding of tuples to the left and right channels based on the
   * forward threshold.
   */
  void Start(KeyType diff) {
    // process the input tuples as the paper Figure 9
    auto process_func = [this, diff]() {
      size_t join_count = 0;
      for (auto tuple : *input_chan_) {
        closed_guessed_ = false;  // restart the timer
        if (tuple.ctl_ == TupleFlag::INPUT_R || tuple.ctl_ == TupleFlag::ACK_S) {
          join_count += ProcessLeft(tuple, diff);
        } else if (tuple.ctl_ == TupleFlag::INPUT_S) {
          join_count += ProcessRight(tuple, diff);
        } else {
          throw std::runtime_error("Invalid tuple control flag");
        }
        ForwardTuples();
      }
      spdlog::info("Window {} join count: {}", id_, join_count);
    };

    std::thread process_thread(process_func);
    std::thread check_thread(&HandshakeWindow::CheckClosed, this);
    check_thread.join();  // check thread finishes, terminate the while loop in process thread
    input_chan_->close();
    process_thread.join();
  }

 private:
  /**
   * @brief Check if the input channel is empty for a while.
   * @details If the function returns, the input channel is empty for a while, which is assumed
   * that the window should stop listenning the input.
   */
  void CheckClosed() {
    while (!closed_guessed_) {
      closed_guessed_ = true;
      std::this_thread::sleep_for(check_flag_interval_);
    }
  }

  /**
   * @brief Check if the timestamps of two tuples satisfy the window limit.
   */
  inline auto TimeStampMatched(const TupleType<KeyType, ValueType> &r,
                               const TupleType<KeyType, ValueType> &s) -> bool {
    if (r.timestamp_ > s.timestamp_) {
      return r.timestamp_ <= s.timestamp_ + window_size_s_;
    }
    return s.timestamp_ >= r.timestamp_ && s.timestamp_ <= r.timestamp_ + window_size_r_;
  }

  auto ProcessLeft(TupleType<KeyType, ValueType> &tuple, KeyType diff) -> size_t {
    if (tuple.ctl_ == TupleFlag::INPUT_R) {
      auto results = index_s_->RangeSearch({tuple.key_ - diff, tuple.key_ + diff});
      size_t join_count = 0;
      for (const auto &tuple_s : results) {
        if (TimeStampMatched(tuple, tuple_s)) {
          spdlog::debug("{} | {}", tuple, tuple_s);
          ++join_count;
        }
      }
      index_r_->Insert(tuple);
      return join_count;
    }
    if (tuple.ctl_ == TupleFlag::ACK_S) {
      auto tuple_s = index_s_->PopOldest();
      assert(tuple_s.forwarded_);
      assert(tuple_s.ctl_ == TupleFlag::INPUT_S);
      // spdlog::debug("Window {} deletes: {}", id_, tuple_s);
      return 0;
    }
    throw std::runtime_error("Invalid tuple control flag");
    return 0;
  }

  auto ProcessRight(TupleType<KeyType, ValueType> &tuple, KeyType diff) -> size_t {
    if (tuple.ctl_ == TupleFlag::INPUT_S) {
      auto results = index_r_->RangeSearch({tuple.key_ - diff, tuple.key_ + diff});
      size_t join_count = 0;
      for (const auto &tuple_r : results) {
        if (TimeStampMatched(tuple_r, tuple)) {
          spdlog::debug("{} | {}", tuple_r, tuple);
          ++join_count;
        }
      }

      index_s_->Insert(tuple);
      SendToRight({tuple.timestamp_, tuple.key_, tuple.value_, TupleFlag::ACK_S});
      return join_count;
    }
    throw std::runtime_error("Invalid tuple control flag");
    return 0;
  }

  void ForwardTuples() {
    if (index_s_->Size() > forward_threshold_) {
      auto &tuple = index_s_->GetOldestRef();
      if (!tuple.forwarded_) {
        SendToLeft(tuple);
        tuple.forwarded_ = true;
        // spdlog::debug("Window {} forward: {}", id_, tuple);
      }
    }
    if (index_r_->Size() > forward_threshold_) {
      auto tuple = index_r_->PopOldest();
      assert(tuple.ctl_ == TupleFlag::INPUT_R);
      SendToRight(tuple);
      // spdlog::debug("Window {} forward: {}", id_, tuple);
    }
  }

  auto SendToLeft(const TupleType<KeyType, ValueType> &tuple) -> void {
    assert(tuple.ctl_ == TupleFlag::INPUT_S);
    if (output_left_chan_) {
      (*output_left_chan_) << tuple;
    } else {
      // left-most sub-window sends "s" tuple to the null, ack(s) will not be received
      // therefore, the left-most one should send ack(s) to itself
      // FIXME: deadlock if input channel is full
      auto ack_tuple{tuple};
      ack_tuple.ctl_ = TupleFlag::ACK_S;
      *(input_chan_) << ack_tuple;
    }
  }

  auto SendToRight(const TupleType<KeyType, ValueType> &tuple) -> void {
    assert(tuple.ctl_ == TupleFlag::ACK_S || tuple.ctl_ == TupleFlag::INPUT_R);
    if (output_right_chan_) {
      (*output_right_chan_) << tuple;
    }
  }

  ChannelPointer<KeyType, ValueType> output_left_chan_;   // send s tuples to the left
  ChannelPointer<KeyType, ValueType> output_right_chan_;  // send r tuples and ack(s) to the right
  ChannelPointer<KeyType, ValueType> input_chan_;

  std::unique_ptr<WindowIndex<KeyType, ValueType>> index_r_;  // total index of stream R
  const size_t window_size_r_;
  std::unique_ptr<WindowIndex<KeyType, ValueType>> index_s_;  // sub-index of stream S
  const size_t window_size_s_;

  const size_t forward_threshold_;  // threshold for forwarding tuples

  std::ostream &os_;  // output stream for join results

  int32_t id_;  // id of the window (debugging purpose)

  bool closed_guessed_{false};  // flag to indicate if the window should be closed
  const std::chrono::milliseconds check_flag_interval_{2000};
};

template <typename KeyType, typename ValueType, typename Container>
class HandshakeJoiner {
  static_assert(std::is_base_of_v<WindowIndex<KeyType, ValueType>, Container>,
                "Container must be derived from Index<KeyType, ValueType>");

 public:
  HandshakeJoiner(size_t num_workers, size_t window_len, size_t channel_buffer_size,
                  std::unique_ptr<Stream<KeyType, ValueType>> stream_r,
                  std::unique_ptr<Stream<KeyType, ValueType>> stream_s,
                  std::ostream &os = std::cout)
      : num_workers_(num_workers),
        window_len_(window_len),
        channel_buffer_size_(channel_buffer_size),
        tuple_reader_(std::move(stream_r), std::move(stream_s)),
        os_(os) {
    if (num_workers_ < 1) {
      throw std::invalid_argument("Number of workers must be greater than 0");
    }
    if (window_len_ % num_workers != 0) {
      throw std::invalid_argument("Window length must be divisible by number of workers");
    }

    // create the input channels
    auto input_channels = std::vector<ChannelPointer<KeyType, ValueType>>(num_workers_);
    for (size_t i = 0; i < num_workers_; ++i) {
      input_channels[i] = std::make_shared<Channel<KeyType, ValueType>>(channel_buffer_size_);
    }
    input_left_chan_ = input_channels[0];
    input_right_chan_ = input_channels[num_workers_ - 1];

    // create the windows
    for (size_t i = 0; i < num_workers_; ++i) {
      auto left_output_chan = (i == 0) ? nullptr : input_channels[i - 1];
      auto right_output_chan = (i == num_workers_ - 1) ? nullptr : input_channels[i + 1];
      auto forward_threshold =
          window_len / num_workers + 1;  // +1 to guarantee the completeness of join results
      windows_.emplace_back(window_len_, forward_threshold, input_channels[i], left_output_chan,
                            right_output_chan, i, os);
    }
  }

  ~HandshakeJoiner() {
    for (size_t i = 0; i < num_workers_; ++i) {
      if (workers_[i].joinable()) {
        workers_[i].join();
      }
    }
  }

  HandshakeJoiner(const HandshakeJoiner &) = delete;
  auto operator=(const HandshakeJoiner &) -> HandshakeJoiner & = delete;
  HandshakeJoiner(HandshakeJoiner &&) = default;
  auto operator=(HandshakeJoiner &&) -> HandshakeJoiner & = default;

  void Start(KeyType diff) {
    // start the worker threads
    for (size_t i = 0; i < num_workers_; ++i) {
      workers_.emplace_back([this, i, diff]() { windows_[i].Start(diff); });
    }

    // start the producer thread:
    while (true) {
      auto tuple_opt = tuple_reader_.GetNextTuple();
      if (!tuple_opt.has_value()) {
        break;
      }
      if (tuple_opt->ctl_ == TupleFlag::INPUT_R) {
        (*input_left_chan_) << *tuple_opt;
      } else if (tuple_opt->ctl_ == TupleFlag::INPUT_S) {
        (*input_right_chan_) << *tuple_opt;
      } else {
        throw std::runtime_error("Invalid tuple control flag");
      }
    }

    // workers will automatically close the input channels if they fail to receive tuples
    // for a while
  }

 private:
  size_t num_workers_;
  size_t window_len_;
  size_t channel_buffer_size_;

  TupleReader<KeyType, ValueType> tuple_reader_;

  std::vector<HandshakeWindow<KeyType, ValueType, Container>> windows_;
  std::vector<std::thread> workers_;

  ChannelPointer<KeyType, ValueType> input_left_chan_;  // send r tuples to the left most sub-window
  ChannelPointer<KeyType, ValueType>
      input_right_chan_;  // send s tuples to the right most sub-window

  std::ostream &os_;  // output stream for join results

  const std::chrono::milliseconds wait_after_close_{1000};
};

}  // namespace stream

#endif  // HANDSHAKE_WINDOW_HPP