#ifndef JOIN_BROADCAST_WINDOW_HPP
#define JOIN_BROADCAST_WINDOW_HPP

#include <spdlog/spdlog.h>
#include <iostream>
#include <memory>
#include <optional>
#include <thread>
#include <utility>
#include <vector>
#include "index/index.hpp"
#include "stream/stream.hpp"
#include "types/types.hpp"

namespace stream {

/**
 * @brief BroadcastWindow class.
 * @details The BroadcastWindow class is used in the broadcast join that
 * maintains a total index of one stream (R), and a sub-index of another stream (S).
 * Every time a new tuple r is received, it searches in the local sub-index I_s[i] and
 * update the total index I_r; tuple s is processed oppositely to r.
 */
template <typename KeyType, typename ValueType, typename Container>
class BroadcastWindow {
 public:
  /**
   * @brief Constructor of BroadcastWindow.
   * @param diff join condition: |r.key - s.key| <= diff
   * @param window_len_S size of the sub window size of S stream
   * @param window_len_R size of the total window size of R stream
   * @param input_chan input channel for the tuples/events
   * @param id id of the window (debugging purpose)
   * @param os output stream for logging/debugging
   */
  BroadcastWindow(size_t window_len_S, size_t window_len_R, ChannelPointer<KeyType, ValueType> input_chan,
                  int32_t id = -1, std::ostream &os = std::cout)
      : window_size_r_(window_len_R),
        window_size_s_(window_len_S),
        input_chan_(input_chan),
        os_(os),
        id_(id),
        index_r_(std::make_unique<Container>()),
        index_s_(std::make_unique<Container>()) {
    if (window_size_s_ == 0 or window_size_r_ == 0) {
      throw std::invalid_argument("window size must be greater than 0");
    }
  }

  ~BroadcastWindow() = default;

  BroadcastWindow(const BroadcastWindow &) = delete;
  auto operator=(const BroadcastWindow &) -> BroadcastWindow & = delete;
  BroadcastWindow(BroadcastWindow &&) = default;
  auto operator=(BroadcastWindow &&) -> BroadcastWindow & = default;

  /**
   * @brief Popping tuples from the input channel and process them.
   * @param diff join condition: |r.key - s.key| <= diff, diff >= 0
   * @details The tuples are popped from the input channel either from R stream or S stream.
   * A new tuple is processed by searching in the index of opposite stream, flush the join results,
   * and update the index of the current stream.
   */
  auto Start(KeyType diff) {
    if (diff < 0) {
      throw std::invalid_argument("diff must be greater than or equal to 0");
    }
    WorkRoutine(diff);
  };

 private:
  auto ProcessR(TupleType<KeyType, ValueType> tuple, KeyType diff) -> size_t {
    // delete expired tuples in the opposite stream index I_s
    TsType ts = tuple.timestamp_;
    TsType ts_lower_bound = ts - window_size_s_;
    while (!index_s_->Empty() && index_s_->GetOldest().timestamp_ < ts_lower_bound) {
      index_s_->PopOldest();
    }

    // delete expired tuples in the same stream index + insert the new tuple into I_R
    ts_lower_bound = ts - window_size_r_;
    while (!index_r_->Empty() && index_r_->GetOldest().timestamp_ < ts_lower_bound) {
      index_r_->PopOldest();
    }
    if (!index_r_->Insert(tuple)) {
      return 0;  // duplicate key, skip
    }

    // get the join results by range search [key - diff, key + diff]
    std::pair<KeyType, KeyType> key_range(tuple.key_ - diff, tuple.key_ + diff);
    auto results = index_s_->RangeSearch(key_range);
    for (const auto &result : results) {
      spdlog::debug("{} | {}", tuple, result);
    }

    return results.size();
  }

  auto ProcessS(TupleType<KeyType, ValueType> tuple, KeyType diff) -> size_t {
    // delete expired tuples in the opposite stream index I_R
    TsType ts = tuple.timestamp_;
    TsType ts_lower_bound = ts - window_size_r_;
    while (!index_r_->Empty() && index_r_->GetOldest().timestamp_ < ts_lower_bound) {
      index_r_->PopOldest();
    }

    // delete expired tuples in the same stream index + insert the new tuple into I_s
    ts_lower_bound = ts - window_size_s_;
    while (!index_s_->Empty() && index_s_->GetOldest().timestamp_ < ts_lower_bound) {
      index_s_->PopOldest();
    }
    if (!index_s_->Insert(tuple)) {
      return 0;  // duplicate key, skip
    }

    // get the join results by range search [key - diff, key + diff]
    std::pair<KeyType, KeyType> key_range(tuple.key_ - diff, tuple.key_ + diff);
    auto results = index_r_->RangeSearch(key_range);
    for (const auto &result : results) {
      spdlog::debug("{} | {}", result, tuple);
    }

    return results.size();
  }

  void WorkRoutine(KeyType diff) {
    size_t join_count = 0;
    for (const TupleType<KeyType, ValueType> tuple : *this->input_chan_) {
      if (tuple.ctl_ == TupleFlag::INPUT_R) {
        join_count += ProcessR(std::move(tuple), diff);
      } else if (tuple.ctl_ == TupleFlag::INPUT_S) {
        join_count += ProcessS(std::move(tuple), diff);
      } else {
        throw std::runtime_error("Invalid tuple control flag");
      }
    }
    spdlog::info("Window {}: Join count: {}", id_, join_count);
  }

  size_t window_size_r_;
  size_t window_size_s_;

  ChannelPointer<KeyType, ValueType> input_chan_;  // input channel for the tuples/events

  std::ostream &os_;  // output stream for logging/debugging

  // debug:
  int32_t id_;

  std::unique_ptr<WindowIndex<KeyType, ValueType>> index_r_;  // total index of stream R
  std::unique_ptr<WindowIndex<KeyType, ValueType>> index_s_;  // sub-index of stream S
};

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
                  std::ostream &os = std::cout)
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

#endif  // JOIN_BROADCAST_WINDOW_HPP