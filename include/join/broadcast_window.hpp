#ifndef JOIN_BROADCAST_WINDOW_HPP
#define JOIN_BROADCAST_WINDOW_HPP

#include <iostream>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "index/index.hpp"
#include "join/window.hpp"
#include "stream/stream.hpp"
#include "types/types.hpp"

namespace stream {

/**
 * @brief BroadcastWindow class.
 * @details The BroadcastWindow class is a SubWindow class used in the broadcast join that
 * maintains a total index of one stream (R), and a sub-index of another stream (S).
 * Every time a new tuple r is received, it searches in the local sub-index I_s[i] and
 * update the total index I_r; tuple s is processed oppositely to r.
 */
template <typename KeyType, typename ValueType, typename Container>
class BroadcastWindow : public SubWindow<KeyType, ValueType, Container> {
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
  BroadcastWindow(size_t window_len_S, size_t window_len_R,
                  ChannelPointer<KeyType, ValueType> input_chan, int32_t id = -1,
                  std::ostream &os = std::cout)
      : SubWindow<KeyType, ValueType, Container>(window_len_S, input_chan, nullptr, nullptr, id,
                                                 os),
        window_size_s_(window_len_S),
        window_size_r_(window_len_R),
        index_s_(std::move(SubWindow<KeyType, ValueType, Container>::index_)),
        index_r_(std::make_unique<Container>()) {
    if (window_size_s_ == 0 or window_size_r_ == 0) {
      throw std::invalid_argument("window size must be greater than 0");
    }
  }

  ~BroadcastWindow() {
    if (thread_.joinable()) {
      thread_.join();
    }
  };

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
    if (is_running_) {
      throw std::runtime_error("BroadcastWindow is already running");
    }
    is_running_ = true;

    thread_ = std::thread([this, diff]() {
      try {
        WorkRoutine(diff);
      } catch (const std::exception &e) {
        std::cerr << "Error in BroadcastWindow[" << this->id_ << "]: " << e.what() << std::endl;
      }
    });
    // thread_.detach();
  };

 private:
  auto ProcessR(TupleType<KeyType, ValueType> tuple, KeyType diff) -> size_t {
    // delete expired tuples in the opposite stream index I_s
    TsType ts = tuple.timestamp_;
    TsType ts_lower_bound = ts - window_size_s_;
    while (!index_s_->Empty() && index_s_->GetOldest().timestamp_ < ts_lower_bound) {
      index_s_->PopOldest();
    }
    // get the join results by range search [key - diff, key + diff]
    std::pair<KeyType, KeyType> key_range(tuple.key_ - diff, tuple.key_ + diff);
    auto results = index_s_->RangeSearch(key_range);
    for (const auto &result : results) {
      //   this->os_ << tuple << " | " << result << "\n";
    }

    // delete expired tuples in the same stream index + insert the new tuple into I_R
    ts_lower_bound = ts - window_size_r_;
    while (!index_r_->Empty() && index_r_->GetOldest().timestamp_ < ts_lower_bound) {
      index_r_->PopOldest();
    }
    index_r_->Insert(tuple);

    return results.size();
  }

  auto ProcessS(TupleType<KeyType, ValueType> tuple, KeyType diff) -> size_t {
    // delete expired tuples in the opposite stream index I_R
    TsType ts = tuple.timestamp_;
    TsType ts_lower_bound = ts - window_size_r_;
    while (!index_r_->Empty() && index_r_->GetOldest().timestamp_ < ts_lower_bound) {
      index_r_->PopOldest();
    }
    // get the join results by range search [key - diff, key + diff]
    std::pair<KeyType, KeyType> key_range(tuple.key_ - diff, tuple.key_ + diff);
    auto results = index_r_->RangeSearch(key_range);
    for (const auto &result : results) {
      //   this->os_ << tuple << " | " << result << "\n";
    }

    // delete expired tuples in the same stream index + insert the new tuple into I_s
    ts_lower_bound = ts - window_size_s_;
    while (!index_s_->Empty() && index_s_->GetOldest().timestamp_ < ts_lower_bound) {
      index_s_->PopOldest();
    }
    index_s_->Insert(tuple);

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
    std::cout << "Total join count: " << join_count << "\n";
  }

  std::unique_ptr<WindowIndex<KeyType, ValueType>> index_r_;  // total index of stream R
  size_t window_size_r_;
  std::unique_ptr<WindowIndex<KeyType, ValueType>> index_s_;  // sub-index of stream S
  size_t window_size_s_;

  bool is_running_ = false;  // flag to indicate if the window is running
  std::thread thread_;       // working thread for the broadcast window
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
template <typename KeyType, typename ValueType, typename Container, typename StreamType>
class BroadcastJoiner {
  static_assert(std::is_base_of_v<stream::Stream<KeyType, ValueType>, StreamType>,
                "StreamType must be derived from msd::stream::Stream");

 public:
  BroadcastJoiner(size_t num_workers, size_t window_size, size_t channel_buffer_size,
                  std::unique_ptr<StreamType> R, std::unique_ptr<StreamType> S,
                  std::ostream &os = std::cout)
      : num_workers_(num_workers),
        channels_(num_workers),
        window_size_(window_size),
        sub_window_size_(window_size / num_workers),
        channel_buffer_size_(channel_buffer_size),
        r_(std::move(R)),
        s_(std::move(S)) {
    if (num_workers_ == 0) {
      throw std::invalid_argument("num_workers must be greater than 0");
    }
    if (window_size_ % num_workers_ != 0) {
      throw std::invalid_argument(
          "currently window_size must be divisible by num_workers (maybe relax later)");
    }

    // create N subwindows, each with a input event channel
    for (size_t i = 0; i < num_workers_; ++i) {
      channels_[i] = std::make_shared<Channel<KeyType, ValueType>>(channel_buffer_size_);
      subwindows_.emplace_back(sub_window_size_, window_size_, channels_[i], i, os);
    }
  }

  ~BroadcastJoiner() {
    for (size_t i = 0; i < num_workers_; ++i) {
      if (workers_[i].joinable()) {
        workers_[i].join();
      }
    }
    if (master_thread_.joinable()) {
      master_thread_.join();
    }
  }

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
    TsType curr_ts = 0;
    while ((tuple_opt = GetNextTuple()) && tuple_opt.has_value()) {
      TupleType<KeyType, ValueType> tuple = tuple_opt.value();
      tuple.timestamp_ = curr_ts++;  // assign globally incremental timestamp

      //   std::cerr << "Producer: " << tuple << "\n";

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

  /**
   * @brief Get the next tuple from the input streams R and S. (timestamp may not be unique and
   * globally incremental)
   * @return next tuple either from R or S stream, or nullopt if no more tuples
   */
  auto GetNextTuple() -> std::optional<TupleType<KeyType, ValueType>> {
    static bool read_preference{true};  // prefer to read R stream first, false for S stream first
    while (!r_->Eof() || !s_->Eof()) {
      if (read_preference) {
        if (r_->Available()) {
          TupleType<KeyType, ValueType> tuple;
          *r_ >> tuple;
          tuple.ctl_ = TupleFlag::INPUT_R;
          read_preference = false;
          return tuple;
        }
        if (s_->Available()) {
          TupleType<KeyType, ValueType> tuple;
          *s_ >> tuple;
          tuple.ctl_ = TupleFlag::INPUT_S;
          read_preference = true;
          return tuple;
        }
      } else {
        if (s_->Available()) {
          TupleType<KeyType, ValueType> tuple;
          *s_ >> tuple;
          tuple.ctl_ = TupleFlag::INPUT_S;
          read_preference = true;
          return tuple;
        }
        if (r_->Available()) {
          TupleType<KeyType, ValueType> tuple;
          *r_ >> tuple;
          tuple.ctl_ = TupleFlag::INPUT_R;
          read_preference = false;
          return tuple;
        }
      }
    }
    return std::nullopt;  // no more tuples
  }

  const size_t num_workers_{};          // number of consumer threads == number of subwindows
  const size_t window_size_{};          // total number of tuples in the all subwindows
  const size_t sub_window_size_{};      // number of tuples in each subwindow
  const size_t channel_buffer_size_{};  // size of the channel buffer

  std::unique_ptr<Stream<KeyType, ValueType>> r_;  // input stream R
  std::unique_ptr<Stream<KeyType, ValueType>> s_;  // input stream S

  std::vector<BroadcastWindow<KeyType, ValueType, Container>>
      subwindows_;                                            // subwindows for the join workers
  std::vector<ChannelPointer<KeyType, ValueType>> channels_;  // input channels for the subwindows
  std::vector<std::thread> workers_;                          // working threads for the subwindows
  std::thread master_thread_;                                 // master thread for the joiner
};
}  // namespace stream

#endif  // JOIN_BROADCAST_WINDOW_HPP