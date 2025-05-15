#ifndef HANDSHAKE_JOINER_HPP
#define HANDSHAKE_JOINER_HPP

#include "handshake_window.hpp"

namespace stream {

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
        size_r_(num_workers_),
        size_s_(num_workers_),
        PUSH_TUPLE_TOLERANCE(10),  // +n guarantee tuple will be pushed
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

    for (size_t i = 0; i < num_workers_; ++i) {
      auto left_output_chan = (i == 0) ? nullptr : left_direct_channels[i - 1];
      auto right_output_chan = (i == num_workers_ - 1) ? nullptr : right_direct_channels[i + 1];
      auto left_input_chan = right_direct_channels[i];
      auto right_input_chan = left_direct_channels[i];
      auto *size_r_right = (i == num_workers_ - 1) ? nullptr : &size_r_[i + 1];
      auto *size_s_left = (i == 0) ? nullptr : &size_s_[i - 1];
      windows_.emplace_back(window_len_, num_workers_, left_input_chan, right_input_chan, left_output_chan,
                            right_output_chan, &size_r_[i], &size_s_[i], size_r_right, size_s_left, &newest_r_ts_,
                            &newest_s_ts_, &oldest_r_ts_, &oldest_s_ts_, i, os_);
    }
  }

  ~HandshakeJoiner() = default;

  HandshakeJoiner(const HandshakeJoiner &) = delete;
  auto operator=(const HandshakeJoiner &) -> HandshakeJoiner & = delete;
  HandshakeJoiner(HandshakeJoiner &&) = default;
  auto operator=(HandshakeJoiner &&) -> HandshakeJoiner & = default;

  void Start(KeyType diff) {
    // start the worker threads
    workers_.clear();
    workers_.reserve(num_workers_);
    for (size_t i = 0; i < num_workers_; ++i) {
      workers_.emplace_back([this, i, diff]() { windows_[i].Start(diff); });
    }

    // start the producer thread:
    Producer();

    for (size_t i = 0; i < num_workers_; ++i) {
      if (workers_[i].joinable()) {
        workers_[i].join();
      }
    }
  }

 private:
  void Producer() {
    while (true) {
      auto tuple_opt = tuple_reader_.GetNextTuple();
      if (!tuple_opt.has_value()) {
        break;
      }
      if (tuple_opt->ctl_ == TupleFlag::INPUT_R) {
        // while (!ShouldPushR(tuple_opt->timestamp_)) {
        //   // wait for the right end to pop the expired tuples and update the oldest timestamp
        // }
        *send_r_chan_ << *tuple_opt;
      } else if (tuple_opt->ctl_ == TupleFlag::INPUT_S) {
        // while (!ShouldPushS(tuple_opt->timestamp_)) {
        //   // wait for the left end to pop the expired tuples and update the oldest timestamp
        // }
        *send_s_chan_ << *tuple_opt;
      } else {
        throw std::runtime_error("Invalid tuple control flag");
      }
    }
    auto eof_r = TupleType<KeyType, ValueType>();
    eof_r.ctl_ = TupleFlag::EOF_R;
    *send_r_chan_ << eof_r;
    auto eof_s = TupleType<KeyType, ValueType>();
    eof_s.ctl_ = TupleFlag::EOF_S;
    *send_s_chan_ << eof_s;

    spdlog::debug("Master closes r input channel");
    send_r_chan_->close();
    spdlog::debug("Master closes s input channel");
    send_s_chan_->close();
  }

  auto ShouldPushR(const TsType &timestamp) -> bool {
    TsType upper_bound = oldest_r_ts_ + window_len_ + PUSH_TUPLE_TOLERANCE;
    return timestamp <= upper_bound;
  }

  auto ShouldPushS(const TsType &timestamp) -> bool {
    TsType upper_bound = oldest_s_ts_ + window_len_ + PUSH_TUPLE_TOLERANCE;
    return timestamp <= upper_bound;
  }

  size_t num_workers_;
  size_t window_len_;
  size_t channel_buffer_size_;

  TupleReader<KeyType, ValueType> tuple_reader_;

  std::vector<HandshakeWindow<KeyType, ValueType, Container>> windows_;
  std::vector<std::atomic_uint64_t> size_r_;
  std::vector<std::atomic_uint64_t> size_s_;
  std::vector<std::thread> workers_;

  ChannelPointer<KeyType, ValueType> send_r_chan_;  // send r tuples to the left most sub-window
  ChannelPointer<KeyType, ValueType> send_s_chan_;  // send s tuples to the right most sub-window
  ChannelPointer<KeyType, ValueType> pop_r_chan_;   // pop r tuples from the right most sub-window
  ChannelPointer<KeyType, ValueType> pop_s_chan_;   // pop s tuples from the left most sub-window

  volatile TsType newest_r_ts_{0};    // timestamp of the newest r tuple in the whole windows
  volatile TsType newest_s_ts_{0};    // timestamp of the newest s tuple in the whole windows
  volatile TsType oldest_r_ts_{0};    // timestamp of the oldest r tuple popped from the whole windows
  volatile TsType oldest_s_ts_{0};    // timestamp of the oldest s tuple popped from the whole windows
  const TsType PUSH_TUPLE_TOLERANCE;  // timestamp tolerance for pushing tuples to the windows

  std::ostream &os_;  // output stream for join results

  std::thread right_end_routine_;  // thread for the right end of the joiner
  std::thread left_end_routine_;   // thread for the left end of the joiner
};

}  // namespace stream

#endif  // HANDSHAKE_JOINER_HPP