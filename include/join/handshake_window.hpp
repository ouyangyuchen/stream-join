#ifndef HANDSHAKE_WINDOW_HPP
#define HANDSHAKE_WINDOW_HPP

#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <deque>
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
  HandshakeWindow(size_t window_size, size_t forward_threshold, ChannelPointer<KeyType, ValueType> input_chan_left,
                  ChannelPointer<KeyType, ValueType> input_chan_right,
                  ChannelPointer<KeyType, ValueType> output_left_chan,
                  ChannelPointer<KeyType, ValueType> output_right_chan, int32_t id = -1, std::ostream &os = std::cout)
      : window_size_r_(window_size),
        window_size_s_(window_size),
        forward_threshold_(forward_threshold),
        output_left_chan_(output_left_chan),
        output_right_chan_(output_right_chan),
        input_left_chan_(input_chan_left),
        input_right_chan_(input_chan_right),
        index_r_(std::make_unique<Container>()),
        index_s_(std::make_unique<Container>()),
        id_(id),
        os_(os) {}

  HandshakeWindow() = delete;
  HandshakeWindow(const HandshakeWindow &) = delete;
  auto operator=(const HandshakeWindow &) -> HandshakeWindow & = delete;
  HandshakeWindow(HandshakeWindow &&) = default;
  auto operator=(HandshakeWindow &&) -> HandshakeWindow & = default;

  /**
   * @brief Read tuples from the input channel and process them.
   * @param diff join condition: |r.key - s.key| <= diff
   * @details The function reads tuples from the input channel and processes them based on their
   * attributes, updating the indexes and sending results to the output channels as necessary.
   * It also handles the forwarding of tuples to the left and right channels based on the
   * forward threshold.
   */
  void Start(KeyType diff) {
    std::thread process_thread([this, diff]() { ProcessRoutine(diff); }  // process tuples in the input channel
    );
    process_thread.join();
  }

 private:
  inline bool ShouldTerminate() {
    return (input_left_chan_->closed() && input_left_chan_->empty()) &&
           (input_right_chan_->closed() && input_right_chan_->empty()) &&
           (output_left_chan_ == nullptr || output_left_chan_->closed()) &&
           (output_right_chan_ == nullptr || output_right_chan_->closed());
  }

  /**
   * @brief Process tuples in the input channel and forward them to left/right correspondingly.
   */
  void ProcessRoutine(KeyType diff) {
    size_t iteration{0};
    size_t join_count{0};
    size_t index_r_count_avg{0};  // average r tuple workload
    size_t index_s_count_avg{0};  // average s tuple workload

    while (!ShouldTerminate()) {
      ++iteration;
      if (!input_left_chan_->empty()) {
        TupleType<KeyType, ValueType> tuple;
        *input_left_chan_ >> tuple;
        assert(tuple.ctl_ == TupleFlag::INPUT_R || tuple.ctl_ == TupleFlag::ACK_S);
        join_count += ProcessLeft(tuple, diff);
      }
      if (!input_right_chan_->empty()) {  // process tuple non-blockingly
        TupleType<KeyType, ValueType> tuple;
        *input_right_chan_ >> tuple;
        assert(tuple.ctl_ == TupleFlag::INPUT_S);
        join_count += ProcessRight(tuple, diff);
      }

      ForwardTuples();
      FlushPendings();

      index_r_count_avg += index_r_->Size();
      index_s_count_avg += index_s_->Size();
    }

    index_r_count_avg /= iteration;
    index_s_count_avg /= iteration;
    spdlog::info("Window {} join count: {}", id_, join_count);
    spdlog::info("Window {} index r size: {}, index s size: {}", id_, index_r_count_avg, index_s_count_avg);
  }

  /**
   * @brief Check if the timestamps of two tuples satisfy the window limit.
   */
  inline auto TimeStampMatched(const TupleType<KeyType, ValueType> &r, const TupleType<KeyType, ValueType> &s) -> bool {
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
      ProcessAck(tuple);
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
        if (TimeStampMatched(tuple_r, tuple) && !tuple_r.forwarded_) {
          spdlog::debug("{} | {}", tuple_r, tuple);
          ++join_count;
        }
      }

      index_s_->Insert(tuple);
      auto tuple_ack{tuple};
      tuple_ack.ctl_ = TupleFlag::ACK_S;
      assert(tuple_ack.forwarded_ == false);
      SendToRight(tuple_ack);
      return join_count;
    }
    throw std::runtime_error("Invalid tuple control flag");
    return 0;
  }

  void ForwardTuples() {
    if (index_s_->Size() > forward_threshold_) {
      auto &tuple_head = index_s_->GetOldestRef();
      if (!tuple_head.forwarded_) {
        if (output_left_chan_ == nullptr) {
          // left-most sub-window sends "s" tuple to the null, ack(s) will not be received
          // therefore, the left-most one directly processes the ack as if it is received
          tuple_head.forwarded_ = true;  // optional: for assertion check
          ProcessAck(tuple_head);
        } else {
          SendToLeft(tuple_head);
          // spdlog::debug("Window {} forward: {}", id_, tuple);
        }
      }
    }
    if (index_r_->Size() > forward_threshold_) {
      auto &tuple = index_r_->GetOldestRef();  // delete is done by FlushPendings()
      if (!tuple.forwarded_) {
        if (output_right_chan_ == nullptr) {
          // right-most sub-window sends "r" tuple to the null
          index_r_->PopOldest();
        } else {
          SendToRight(tuple);
          // spdlog::debug("Window {} forward: {}", id_, tuple);
        }
      }
    }
  }

  /**
   * @brief Flush pending tuples to the left/right output channels except when the channels are
   * almost full.
   */
  void FlushPendings() {
    if (output_left_chan_ && !output_left_chan_->closed()) {
      while (!pending_list_left_.empty()) {
        if (output_left_chan_->full(FULL_THRESHOLD)) {
          break;  // avoid full the channel when sending the tuple concurrently
        }
        auto tuple_sent = pending_list_left_.front();
        pending_list_left_.pop_front();
        *output_left_chan_ << tuple_sent;
      }
      if (pending_list_left_.empty()) {
        if ((input_right_chan_->closed() && input_right_chan_->empty()) && index_s_->Size() <= forward_threshold_) {
          output_left_chan_->close();  // no more s tuples to be sent when no s tuples will be received
          spdlog::info("Window {} closes left channel", id_);
        }
      }
    }
    if (output_right_chan_ && !output_right_chan_->closed()) {
      while (!pending_list_right_.empty()) {
        if (output_right_chan_->full(FULL_THRESHOLD)) {
          break;
        }
        auto tuple_sent = pending_list_right_.front();
        pending_list_right_.pop_front();

        // result completeness: tuple r is deleted from index until its pending tuple is flushed
        if (tuple_sent.ctl_ == TupleFlag::INPUT_R) {
          auto tuple_del = index_r_->PopOldest();
          assert(tuple_del.key_ == tuple_sent.key_);
          assert(tuple_sent.timestamp_ == tuple_del.timestamp_);
        }
        *output_right_chan_ << tuple_sent;
      }
      if (pending_list_right_.empty()) {
        if ((input_left_chan_->closed() && input_left_chan_->empty()) &&
            (input_right_chan_->closed() && input_right_chan_->empty()) && (index_r_->Size() <= forward_threshold_)) {
          output_right_chan_->close();  // no more r tuples or s ack to be sent when no input anymore
          spdlog::info("Window {} closes right channel", id_);
        }
      }
    }
  }

  auto SendToLeft(TupleType<KeyType, ValueType> &tuple) -> void {
    assert(tuple.ctl_ == TupleFlag::INPUT_S);
    assert(tuple.forwarded_ == false);
    if (output_left_chan_) {
      pending_list_left_.emplace_back(tuple);
      tuple.forwarded_ = true;
    }
  }

  auto SendToRight(TupleType<KeyType, ValueType> &tuple) -> void {
    assert(tuple.ctl_ == TupleFlag::ACK_S || tuple.ctl_ == TupleFlag::INPUT_R);
    assert(tuple.forwarded_ == false);
    if (output_right_chan_) {
      pending_list_right_.emplace_back(tuple);
      tuple.forwarded_ = true;
    }
  }

  auto ProcessAck(const TupleType<KeyType, ValueType> &tuple) -> void {
    auto &tuple_s = index_s_->GetOldestRef();
    assert(tuple_s.forwarded_);
    assert(tuple_s.ctl_ == TupleFlag::INPUT_S);
    assert(tuple_s.key_ == tuple.key_);
    assert(tuple_s.timestamp_ == tuple.timestamp_);
    assert(tuple_s.value_ == tuple.value_);
    index_s_->PopOldest();
    // spdlog::debug("Window {} deletes: {}", id_, tuple_s);
  }

  ChannelPointer<KeyType, ValueType> output_left_chan_;          // send s tuples to the left
  std::deque<TupleType<KeyType, ValueType>> pending_list_left_;  // pending tuples to be sent

  ChannelPointer<KeyType, ValueType> output_right_chan_;          // send r tuples and ack(s) to the right
  std::deque<TupleType<KeyType, ValueType>> pending_list_right_;  // pending tuples to be sent

  // ChannelPointer<KeyType, ValueType> input_chan_;        // input from left/right/master
  ChannelPointer<KeyType, ValueType> input_left_chan_;   // receive r tuples or ack(s) from the left
  ChannelPointer<KeyType, ValueType> input_right_chan_;  // receive s tuples from the right

  std::unique_ptr<WindowIndex<KeyType, ValueType>> index_r_;  // total index of stream R
  const size_t window_size_r_;
  std::unique_ptr<WindowIndex<KeyType, ValueType>> index_s_;  // sub-index of stream S
  const size_t window_size_s_;

  const size_t forward_threshold_;            // threshold for forwarding tuples
  static constexpr size_t FULL_THRESHOLD{3};  // threshold for considering the channel is full

  std::ostream &os_;  // output stream for join results

  int32_t id_;  // id of the window (debugging purpose)
};

template <typename KeyType, typename ValueType, typename Container>
class HandshakeJoiner {
  static_assert(std::is_base_of_v<WindowIndex<KeyType, ValueType>, Container>,
                "Container must be derived from Index<KeyType, ValueType>");

 public:
  HandshakeJoiner(size_t num_workers, size_t window_len, size_t channel_buffer_size,
                  std::unique_ptr<Stream<KeyType, ValueType>> stream_r,
                  std::unique_ptr<Stream<KeyType, ValueType>> stream_s, std::ostream &os = std::cout)
      : num_workers_(num_workers),
        window_len_(window_len),
        channel_buffer_size_(channel_buffer_size),
        tuple_reader_(std::move(stream_r), std::move(stream_s)),
        os_(os) {
    if (num_workers_ < 1) {
      throw std::invalid_argument("Number of workers must be greater than 0");
    }

    // create the bidirectional channels
    auto right_direct_channels = std::vector<ChannelPointer<KeyType, ValueType>>(num_workers_);
    auto left_direct_channels = std::vector<ChannelPointer<KeyType, ValueType>>(num_workers_);
    for (size_t i = 0; i < num_workers_; ++i) {
      right_direct_channels[i] = std::make_shared<Channel<KeyType, ValueType>>(channel_buffer_size_);
      left_direct_channels[i] = std::make_shared<Channel<KeyType, ValueType>>(channel_buffer_size_);
    }
    send_r_chan_ = right_direct_channels[0];
    send_s_chan_ = left_direct_channels[num_workers_ - 1];

    auto forward_threshold = window_len_ / num_workers_ + 1;  // +1 to guarantee the completeness of join results
    for (size_t i = 0; i < num_workers_; ++i) {
      auto left_output_chan = (i == 0) ? nullptr : left_direct_channels[i - 1];
      auto right_output_chan = (i == num_workers_ - 1) ? nullptr : right_direct_channels[i + 1];
      auto left_input_chan = right_direct_channels[i];
      auto right_input_chan = left_direct_channels[i];
      windows_.emplace_back(window_len_, forward_threshold, left_input_chan, right_input_chan, left_output_chan,
                            right_output_chan, i, os_);
    }
  }

  ~HandshakeJoiner() = default;

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
        (*send_r_chan_) << *tuple_opt;
      } else if (tuple_opt->ctl_ == TupleFlag::INPUT_S) {
        (*send_s_chan_) << *tuple_opt;
      } else {
        throw std::runtime_error("Invalid tuple control flag");
      }
    }
    spdlog::info("Master closes r input channel");
    send_r_chan_->close();
    spdlog::info("Master closes s input channel");
    send_s_chan_->close();

    for (size_t i = 0; i < num_workers_; ++i) {
      if (workers_[i].joinable()) {
        workers_[i].join();
      }
    }
  }

 private:
  size_t num_workers_;
  size_t window_len_;
  size_t channel_buffer_size_;

  TupleReader<KeyType, ValueType> tuple_reader_;

  std::vector<HandshakeWindow<KeyType, ValueType, Container>> windows_;
  std::vector<std::thread> workers_;

  ChannelPointer<KeyType, ValueType> send_r_chan_;  // send r tuples to the left most sub-window
  ChannelPointer<KeyType, ValueType> send_s_chan_;  // send s tuples to the right most sub-window

  std::ostream &os_;  // output stream for join results

  const std::chrono::milliseconds wait_after_close_{1000};
};

}  // namespace stream

#endif  // HANDSHAKE_WINDOW_HPP