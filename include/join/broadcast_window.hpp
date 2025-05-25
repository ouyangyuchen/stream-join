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

template <typename KeyType, typename ValueType, typename Container>
class BroadcastJoiner;

/**
 * @brief BroadcastWindow class.
 * @details The BroadcastWindow class is used in the broadcast join that
 * maintains a total index of one stream (R), and a sub-index of another stream (S).
 * Every time a new tuple r is received, it searches in the local sub-index I_s[i] and
 * update the total index I_r; tuple s is processed oppositely to r.
 */
template <typename KeyType, typename ValueType, typename Container>
class BroadcastWindow {
  friend class BroadcastJoiner<KeyType, ValueType, Container>;

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

    index_r_->Insert(tuple);

    // get the join results by range search [key - diff, key + diff]
    KeyType lower_range = (std::numeric_limits<KeyType>::min() + diff <= tuple.key_)
                              ? tuple.key_ - diff
                              : std::numeric_limits<KeyType>::min();
    KeyType upper_range = (std::numeric_limits<KeyType>::max() - diff >= tuple.key_)
                              ? tuple.key_ + diff
                              : std::numeric_limits<KeyType>::max();
    auto results = index_s_->RangeSearch({lower_range, upper_range});
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

    index_s_->Insert(tuple);

    // get the join results by range search [key - diff, key + diff]
    KeyType lower_range = (std::numeric_limits<KeyType>::min() + diff <= tuple.key_)
                              ? tuple.key_ - diff
                              : std::numeric_limits<KeyType>::min();
    KeyType upper_range = (std::numeric_limits<KeyType>::max() - diff >= tuple.key_)
                              ? tuple.key_ + diff
                              : std::numeric_limits<KeyType>::max();
    auto results = index_r_->RangeSearch({lower_range, upper_range});
    for (const auto &result : results) {
      spdlog::debug("{} | {}", result, tuple);
    }

    return results.size();
  }

  void WorkRoutine(KeyType diff) {
    (void)diff;
    size_t join_count = 0;
    for (const TupleType<KeyType, ValueType> tuple : *this->input_chan_) {
      (void)tuple;
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

}  // namespace stream

#endif  // JOIN_BROADCAST_WINDOW_HPP