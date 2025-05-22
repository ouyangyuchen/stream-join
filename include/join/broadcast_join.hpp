#ifndef BROADCAST_JOIN_HPP
#define BROADCAST_JOIN_HPP

#include "broadcast_window.hpp"

namespace stream {
/**
 * @brief BroadcastJoiner class.
 * @details The BroadcastJoiner class is used to join two streams R and S using the broadcast
 * join algorithm. Each subwindow in the joiner maintains a total index of one stream (R), and a
 * sub-index of another stream (S). Every time a new tuple r is received, it sends the tuple to all
 * subwindows, and each subwindow searches in the local sub-index I_s[i] and update the total index
 * I_r; tuple s is processed oppositely to r but only sent to one subwindow. The join results are
 * sent to the output channel.
 */
template <typename KeyType, typename ValueType, typename Container>
class BroadcastJoiner {
 public:
  BroadcastJoiner(size_t num_workers, size_t window_size, size_t channel_buffer_size,
                  std::unique_ptr<Stream<KeyType, ValueType>> R, std::unique_ptr<Stream<KeyType, ValueType>> S,
                  bool preload = false, std::ostream &os = std::cout)
      : num_workers_(num_workers),
        window_size_(window_size),
        channel_buffer_size_(channel_buffer_size),
        tuple_reader_(std::move(R), std::move(S)),
        channels_(num_workers),
        subwindows_() {
    if (num_workers_ == 0) {
      throw std::invalid_argument("num_workers must be greater than 0");
    }
    subwindows_.reserve(num_workers_);
    for (size_t i = 0; i < num_workers_; ++i) {
      channels_[i] = std::make_shared<Channel<KeyType, ValueType>>(channel_buffer_size_);
      subwindows_.emplace_back(window_size_, window_size_, channels_[i], i, os);
    }

    if (preload) {
      Preload();
    }
  }

  ~BroadcastJoiner() {}

  BroadcastJoiner(const BroadcastJoiner &) = delete;
  auto operator=(const BroadcastJoiner &) -> BroadcastJoiner & = delete;
  BroadcastJoiner(BroadcastJoiner &&) = default;
  auto operator=(BroadcastJoiner &&) -> BroadcastJoiner & = default;

  /**
   * @brief Start running the broadcast joiner.
   * @details The start function starts the master thread and the consumer threads for the
   * subwindows.
   */
  void Start(KeyType diff) {
    for (size_t i = 0; i < num_workers_; ++i) {
      workers_.emplace_back([this, i, diff]() { subwindows_[i].Start(diff); });
    }

    master_thread_ = std::thread([this]() { ProducerRoutine(); });
    for (size_t i = 0; i < num_workers_; ++i) {
      if (workers_[i].joinable()) {
        workers_[i].join();
      }
    }
    if (master_thread_.joinable()) {
      master_thread_.join();
    }
  }

  /**
   * @brief Preload tuples (window_size for R, window_size / num_workers for S) into subwindows.
   */
  void Preload() {
    size_t count_r = 0;
    size_t count_s = 0;
    while (count_r < window_size_ && count_s < window_size_) {
      auto tuple_opt = tuple_reader_.GetNextTuple();
      if (!tuple_opt.has_value()) {
        throw std::runtime_error("No more tuples available during preloading");
      }
      auto &tuple = tuple_opt.value();
      if (tuple.ctl_ == TupleFlag::INPUT_R) {
        count_r++;
        for (size_t i = 0; i < num_workers_; ++i) {
          subwindows_[i].index_r_->Insert(tuple);
        }
      } else if (tuple.ctl_ == TupleFlag::INPUT_S) {
        count_s++;
        size_t sub_window_index = GetSubWindowIndex(tuple);
        subwindows_[sub_window_index].index_s_->Insert(tuple);
      } else {
        throw std::runtime_error("Invalid tuple control flag");
      }
    }
    spdlog::info("Preloaded {} tuples from R and {} tuples from S", count_r, count_s);
  }

  void StartWatcher(std::chrono::milliseconds interval) {
    std::thread watcher_thread([this, interval]() { WatcherRoutine(interval); });
    watcher_thread.detach();
  }

 private:
  /**
   * @brief Producer/Master routine for the joiner.
   * @details The producer routine reads tuples from the input streams R and S,
   * and assigns them with unique incremental timestamps.
   * The tuples are then sent to the channels of all subwindows if from R;
   * or sent to the channel of one subwindow if from S based on load balancing rule.
   */
  void ProducerRoutine() {
    std::optional<TupleType<KeyType, ValueType>> tuple_opt;
    while ((tuple_opt = tuple_reader_.GetNextTuple()).has_value()) {
      // TODO: new reader, in memory, passed by data
      // TODO: measure while loop time
      TupleType<KeyType, ValueType> tuple = tuple_opt.value();

      if (tuple.ctl_ == TupleFlag::INPUT_R) {
        for (size_t i = 0; i < num_workers_; ++i) {
          (*channels_[i]) << tuple;
        }
      } else if (tuple.ctl_ == TupleFlag::INPUT_S) {
        size_t sub_window_index = GetSubWindowIndex(tuple);
        (*channels_[sub_window_index]) << tuple;
      } else {
        throw std::runtime_error("Invalid tuple control flag");
      }
    }

    // close all channels
    for (size_t i = 0; i < num_workers_; ++i) {
      channels_[i]->close();
    }
  }

  /**
   * @brief Load balancing rule for the joiner.
   */
  auto GetSubWindowIndex(const TupleType<KeyType, ValueType> &tuple) -> size_t {
    // load balancing rule: round robin / hashing
    return tuple.timestamp_ % num_workers_;
  }

  void WatcherRoutine(std::chrono::milliseconds interval) {
    while (true) {
      std::cout << "Size of R workers: ";
      for (size_t i = 0; i < num_workers_; ++i) {
        std::cout << subwindows_[i].index_r_->Size() << " ";
      }
      std::cout << std::endl;
      std::cout << "Size of S workers: ";
      for (size_t i = 0; i < num_workers_; ++i) {
        std::cout << subwindows_[i].index_s_->Size() << " ";
      }
      std::cout << std::endl;
      std::this_thread::sleep_for(interval);  // print preloading result
    }
  }

  const size_t num_workers_{};          // number of consumer threads == number of subwindows
  const size_t window_size_{};          // window limit for timestamp matching
  const size_t channel_buffer_size_{};  // size of the channel buffer

  TupleReader<KeyType, ValueType> tuple_reader_;  // tuple reader for the input streams (R and S)

  std::vector<ChannelPointer<KeyType, ValueType>> channels_;                // input channels for the subwindows
  std::vector<BroadcastWindow<KeyType, ValueType, Container>> subwindows_;  // subwindows for the join workers
  std::vector<std::thread> workers_;                                        // working threads for the subwindows
  std::thread master_thread_;                                               // master thread for the joiner
};

}  // namespace stream

#endif  // BROADCAST_JOIN_HPP