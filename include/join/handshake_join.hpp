#ifndef HANDSHAKE_JOINER_HPP
#define HANDSHAKE_JOINER_HPP

#include "handshake_window.hpp"

namespace stream {

struct forward_context;

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
        stream_r_(std::move(stream_r)),
        stream_s_(std::move(stream_s)),
        size_r_(num_workers_),
        size_s_(num_workers_),
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
      forward_context context;
      context.size_r = &size_r_[i];
      context.size_s = &size_s_[i];
      context.size_r_right = (i == num_workers_ - 1) ? nullptr : &size_r_[i + 1];
      context.size_s_left = (i == 0) ? nullptr : &size_s_[i - 1];
      context.newest_r_ts = &newest_r_ts_;
      context.newest_s_ts = &newest_s_ts_;
      context.oldest_r_ts = &oldest_r_ts_;
      context.oldest_s_ts = &oldest_s_ts_;
      windows_.emplace_back(window_len_, num_workers_, channel_buffer_size_, left_input_chan, right_input_chan,
                            left_output_chan, right_output_chan, context, i, os_);
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

  void StartWatcher(std::chrono::milliseconds interval = std::chrono::milliseconds(20000)) {
    auto watcher = std::thread([this, interval]() { Watcher(interval); });
    watcher.detach();
  }

  void Preload() {
    size_t avg_preload_r = window_len_ / num_workers_;
    size_t avg_preload_s = window_len_ / num_workers_;
    // preload R from rightmost sub-window to leftmost sub-window in the chronological order
    for (int i = num_workers_ - 1; i >= 0; --i) {
      for (size_t j = 0; j < avg_preload_r; ++j) {
        TupleType<KeyType, ValueType> tuple;
        *stream_r_ >> tuple;
        tuple.ctl_ = TupleFlag::INPUT_R;
        windows_[i].index_r_->Insert(tuple);
        newest_r_ts_ = tuple.timestamp_;
      }
      size_r_[i].store(windows_[i].index_r_->Size());
    }
    // preload S from leftmost sub-window to rightmost sub-window in the chronological order
    for (int i = 0; i < int(num_workers_); ++i) {
      for (size_t j = 0; j < avg_preload_s; ++j) {
        TupleType<KeyType, ValueType> tuple;
        *stream_s_ >> tuple;
        tuple.ctl_ = TupleFlag::INPUT_S;
        windows_[i].index_s_->Insert(tuple);
        newest_s_ts_ = tuple.timestamp_;
      }
      size_s_[i].store(windows_[i].index_s_->Size());
    }
    spdlog::info("Preloaded {} tuples from R and {} tuples from S for each worker", avg_preload_r, avg_preload_s);
  }

 private:
  void Watcher(std::chrono::milliseconds interval) {
    while (true) {
      // print newest and oldest timestamps
      std::cout << "newest_r_ts_: " << newest_r_ts_ << ", oldest_r_ts_: " << oldest_r_ts_
                << ", newest_s_ts_: " << newest_s_ts_ << ", oldest_s_ts_: " << oldest_s_ts_ << std::endl;
      // print the size of each window
      std::cout << "Size of R workers: ";
      for (size_t i = 0; i < num_workers_; ++i) {
        std::cout << size_r_[i].load() << " ";
      }
      std::cout << std::endl;
      std::cout << "Size of S workers: ";
      for (size_t i = 0; i < num_workers_; ++i) {
        std::cout << size_s_[i].load() << " ";
      }
      std::cout << std::endl;
      std::this_thread::sleep_for(interval);
    }
  }

  void Producer() {
    auto producer_routine = [this](ChannelPointer<KeyType, ValueType> send_chan, Stream<KeyType, ValueType> &stream,
                                   TupleFlag ctl) {
      assert(send_chan != nullptr);
      assert(ctl == TupleFlag::INPUT_R || ctl == TupleFlag::INPUT_S);
      while (!stream.Eof()) {
        TupleType<KeyType, ValueType> tuple;
        stream >> tuple;
        tuple.ctl_ = ctl;

        while ((ctl == TupleFlag::INPUT_S && !ShouldPushS()) || (ctl == TupleFlag::INPUT_R && !ShouldPushR())) {
        }
        *send_chan << tuple;
      }
      auto eof = TupleType<KeyType, ValueType>();
      eof.ctl_ = (ctl == TupleFlag::INPUT_R) ? TupleFlag::EOF_R : TupleFlag::EOF_S;
      *send_chan << eof;
      send_chan->close();
      spdlog::debug("Master closes {} input channel", ctl == TupleFlag::INPUT_R ? "r" : "s");
    };
    auto producer_r =
        std::thread([this, producer_routine]() { producer_routine(send_r_chan_, *stream_r_, TupleFlag::INPUT_R); });
    auto producer_s =
        std::thread([this, producer_routine]() { producer_routine(send_s_chan_, *stream_s_, TupleFlag::INPUT_S); });

    producer_r.join();
    producer_s.join();
  }

  auto ShouldPushR() -> bool {
    TsType upper_bound = oldest_r_ts_ + window_len_ + PUSH_TUPLE_TOLERANCE;
    return newest_r_ts_ <= upper_bound;
  }

  auto ShouldPushS() -> bool {
    TsType upper_bound = oldest_s_ts_ + window_len_ + PUSH_TUPLE_TOLERANCE;
    return newest_s_ts_ <= upper_bound;
  }

  size_t num_workers_;
  size_t window_len_;
  size_t channel_buffer_size_;

  std::unique_ptr<Stream<KeyType, ValueType>> stream_r_;
  std::unique_ptr<Stream<KeyType, ValueType>> stream_s_;

  std::vector<HandshakeWindow<KeyType, ValueType, Container>> windows_;
  std::vector<std::atomic_uint64_t> size_r_;
  std::vector<std::atomic_uint64_t> size_s_;
  std::vector<std::thread> workers_;

  ChannelPointer<KeyType, ValueType> send_r_chan_;  // send r tuples to the left most sub-window
  ChannelPointer<KeyType, ValueType> send_s_chan_;  // send s tuples to the right most sub-window
  ChannelPointer<KeyType, ValueType> pop_r_chan_;   // pop r tuples from the right most sub-window
  ChannelPointer<KeyType, ValueType> pop_s_chan_;   // pop s tuples from the left most sub-window

  volatile TsType newest_r_ts_{0};                   // timestamp of the newest r tuple in the whole windows
  volatile TsType newest_s_ts_{0};                   // timestamp of the newest s tuple in the whole windows
  volatile TsType oldest_r_ts_{0};                   // timestamp of the oldest r tuple popped from the whole windows
  volatile TsType oldest_s_ts_{0};                   // timestamp of the oldest s tuple popped from the whole windows
  static constexpr TsType PUSH_TUPLE_TOLERANCE{50};  // timestamp tolerance for pushing tuples to the windows

  std::ostream &os_;  // output stream for join results

  std::thread right_end_routine_;  // thread for the right end of the joiner
  std::thread left_end_routine_;   // thread for the left end of the joiner
};

}  // namespace stream

#endif  // HANDSHAKE_JOINER_HPP