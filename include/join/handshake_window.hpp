#ifndef HANDSHAKE_WINDOW_HPP
#define HANDSHAKE_WINDOW_HPP

#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
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
  HandshakeWindow(size_t window_size, size_t num_workers, ChannelPointer<KeyType, ValueType> input_chan_left,
                  ChannelPointer<KeyType, ValueType> input_chan_right,
                  ChannelPointer<KeyType, ValueType> output_left_chan,
                  ChannelPointer<KeyType, ValueType> output_right_chan, std::atomic_uint64_t *size_r,
                  std::atomic_uint64_t *size_s, std::atomic_uint64_t *size_r_right, std::atomic_uint64_t *size_s_left,
                  volatile TsType *newest_r_ts, volatile TsType *newest_s_ts, volatile TsType *oldest_r_ts,
                  volatile TsType *oldest_s_ts, int32_t id = -1, std::ostream &os = std::cout)
      : window_size_(window_size),
        output_left_chan_(output_left_chan),
        output_right_chan_(output_right_chan),
        input_left_chan_(input_chan_left),
        input_right_chan_(input_chan_right),
        index_r_(std::make_unique<Container>()),
        index_s_(std::make_unique<Container>()),
        size_r_(size_r),
        size_s_(size_s),
        size_r_right_(size_r_right),
        size_s_left_(size_s_left),
        newest_r_ts_(newest_r_ts),
        newest_s_ts_(newest_s_ts),
        oldest_r_ts_(oldest_r_ts),
        oldest_s_ts_(oldest_s_ts),
        END_BALANCING_THRESHOLD(window_size_ / num_workers + 100),
        os_(os),
        id_(id) {}

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
  void Start(KeyType diff) { ProcessRoutine(diff); }

 private:
  inline bool ShouldTerminate() {
    return (input_left_chan_->closed() && input_left_chan_->empty()) &&
           (input_right_chan_->closed() && input_right_chan_->empty()) &&
           (output_left_chan_ == nullptr || output_left_chan_->closed()) &&
           (output_right_chan_ == nullptr || output_right_chan_->closed());
  }

  bool ShouldForwardLeft() {
    if (index_s_->Empty()) {
      return false;
    }
    if (IsLeftMost()) {
      // the left-most sub-window sends the oldest s tuple to the null if it is expired
      const auto &tuple_oldest = index_s_->GetOldestRef();
      auto current_newest_r_ts = *newest_r_ts_;  // Read volatile once
      return tuple_oldest.timestamp_ <= current_newest_r_ts &&
             !TimeStampMatched(tuple_oldest.timestamp_, current_newest_r_ts);
    }
    return size_s_->load() > size_s_left_->load();
  }

  bool ShouldForwardRight() {
    if (index_r_->Empty()) {
      return false;
    }
    if (IsRightMost()) {
      // the right-most sub-window sends the oldest r tuple to the null if it is expired
      const auto &tuple_oldest = index_r_->GetOldestRef();
      auto current_newest_s_ts = *newest_s_ts_;  // Read volatile once
      return tuple_oldest.timestamp_ <= current_newest_s_ts &&
             !TimeStampMatched(tuple_oldest.timestamp_, current_newest_s_ts);
    }
    return size_r_->load() > size_r_right_->load();
  }

  inline bool IsLeftMost() const { return size_s_left_ == nullptr; }

  inline bool IsRightMost() const { return size_r_right_ == nullptr; }

  /**
   * @brief Wait after receiving a tuple and before processing it to guarantee the chronological tuple order
   * and make sure the master thread always pushes tuples.
   * @param tuple the tuple has received from the input channel and not processed yet
   */
  void WaitBeforeProcessed(const TupleType<KeyType, ValueType> &tuple) const {
    // warn: this is only for incremental timestamp assignment for streams
    if (tuple.ctl_ == TupleFlag::INPUT_R) {
      while (tuple.timestamp_ > *newest_s_ts_ + 1) {
      }
    } else if (tuple.ctl_ == TupleFlag::INPUT_S) {
      while (tuple.timestamp_ > *newest_r_ts_) {
      }
    } else {
      throw std::runtime_error("Invalid tuple control flag");
    }
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

      if (!input_left_chan_->empty() && (!IsLeftMost() || index_r_->Size() < END_BALANCING_THRESHOLD)) {
        TupleType<KeyType, ValueType> tuple;
        *input_left_chan_ >> tuple;
        assert(tuple.ctl_ == TupleFlag::INPUT_R || tuple.ctl_ == TupleFlag::ACK_S || tuple.ctl_ == TupleFlag::EOF_R);
        if (tuple.ctl_ == TupleFlag::EOF_R) {
          *newest_r_ts_ = INT64_MAX;
        } else {
          if (IsLeftMost()) {
            // WaitBeforeProcessed(tuple);
            *newest_r_ts_ = tuple.timestamp_;
          }
          join_count += ProcessLeft(tuple, diff);
        }
      }
      if (!input_right_chan_->empty() && (!IsRightMost() || index_s_->Size() < END_BALANCING_THRESHOLD)) {
        TupleType<KeyType, ValueType> tuple;
        *input_right_chan_ >> tuple;
        assert(tuple.ctl_ == TupleFlag::INPUT_S || tuple.ctl_ == TupleFlag::EOF_S);
        if (tuple.ctl_ == TupleFlag::EOF_S) {
          *newest_s_ts_ = INT64_MAX;
        } else {
          if (IsRightMost()) {
            // WaitBeforeProcessed(tuple);
            *newest_s_ts_ = tuple.timestamp_;
          }
          join_count += ProcessRight(tuple, diff);
        }
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
  inline auto TimeStampMatched(volatile const TsType &ts1, volatile const TsType &ts2) -> bool {
    TsType diff = (ts1 > ts2) ? (ts1 - ts2) : (ts2 - ts1);
    return static_cast<size_t>(diff) <= window_size_;
  }

  auto ProcessLeft(TupleType<KeyType, ValueType> &tuple, KeyType diff) -> size_t {
    if (tuple.ctl_ == TupleFlag::INPUT_R) {
      size_t join_count = 0;
      if (index_r_->Insert(tuple)) {
        size_r_->store(index_r_->Size());

        auto search_range = std::make_pair(tuple.key_ - diff, tuple.key_ + diff);
        auto results = index_s_->RangeSearch(search_range);

        for (const auto &tuple_s : results) {
          if (TimeStampMatched(tuple.timestamp_, tuple_s.timestamp_)) {
            // spdlog::debug("{} | {}", tuple, tuple_s);
            ++join_count;
          }
        }
      }
      return join_count;
    }
    if (tuple.ctl_ == TupleFlag::ACK_S) {
      ProcessAck(tuple);
      return 0;
    }
    throw std::runtime_error("Invalid tuple control flag");
    return 0;  // Should not reach here
  }

  auto ProcessRight(TupleType<KeyType, ValueType> &tuple, KeyType diff) -> size_t {
    if (tuple.ctl_ == TupleFlag::INPUT_S) {
      size_t join_count = 0;
      if (index_s_->Insert(tuple)) {
        size_s_->store(index_s_->Size());

        auto search_range = std::make_pair(tuple.key_ - diff, tuple.key_ + diff);
        auto results = index_r_->RangeSearch(search_range);

        for (const auto &tuple_r : results) {
          if (TimeStampMatched(tuple_r.timestamp_, tuple.timestamp_) && !tuple_r.forwarded_) {
            // spdlog::debug("{} | {}", tuple_r, tuple);
            ++join_count;
          }
        }
      }

      auto tuple_ack{tuple};
      tuple_ack.ctl_ = TupleFlag::ACK_S;
      assert(tuple_ack.forwarded_ == false);
      SendToRight(tuple_ack);
      return join_count;
    }
    throw std::runtime_error("Invalid tuple control flag");
    return 0;  // Should not reach here
  }

  void ForwardTuples() {
    if (ShouldForwardLeft()) {
      auto &tuple_head = index_s_->GetOldestRef();
      if (!tuple_head.forwarded_) {
        SendToLeft(tuple_head);
      }
    }
    if (ShouldForwardRight()) {
      auto &tuple = index_r_->GetOldestRef();  // delete is done by FlushPendings()
      if (!tuple.forwarded_) {
        SendToRight(tuple);
      }
    }
  }

  /**
   * @brief Flush pending tuples to the left/right output channels except when the channels are
   * almost full.
   */
  void FlushPendings() {
    if (!IsLeftMost() && !output_left_chan_->closed()) {
      while (!pending_list_left_.empty()) {
        if (output_left_chan_->full(FULL_THRESHOLD)) {
          break;  // avoid full the channel when sending the tuple concurrently
        }
        auto tuple_sent = pending_list_left_.front();
        pending_list_left_.pop_front();
        *output_left_chan_ << tuple_sent;
      }
      if (pending_list_left_.empty()) {
        if ((input_right_chan_->closed() && input_right_chan_->empty()) && index_s_->Empty()) {
          output_left_chan_->close();  // no more s tuples to be sent when no s tuples will be received
          spdlog::debug("Window {} closes left channel", id_);
        }
      }
    } else if (IsLeftMost()) {
      // left-most sub-window sends "s" tuple to the null, ack(s) will not be received
      // therefore, the left-most one directly processes the ack as if it is received
      for (auto &tuple_del : pending_list_left_) {
        tuple_del.forwarded_ = true;  // for assertion check
        TsType oldest_s_ts = tuple_del.timestamp_;
        assert(tuple_del.timestamp_ <= *newest_s_ts_);
        assert(!TimeStampMatched(*newest_r_ts_, tuple_del.timestamp_));
        ProcessAck(tuple_del);
        *oldest_s_ts_ = oldest_s_ts;
      }
      pending_list_left_.clear();
    }

    if (!IsRightMost() && !output_right_chan_->closed()) {
      while (!pending_list_right_.empty()) {
        if (output_right_chan_->full(FULL_THRESHOLD)) {
          break;
        }
        auto tuple_sent = pending_list_right_.front();
        pending_list_right_.pop_front();

        if (tuple_sent.ctl_ == TupleFlag::INPUT_R) {
          auto tuple_del = index_r_->PopOldest();
          (void)tuple_del;
          size_r_->store(index_r_->Size());
          assert(tuple_del.key_ == tuple_sent.key_);
          assert(tuple_sent.timestamp_ == tuple_del.timestamp_);
        }
        *output_right_chan_ << tuple_sent;
      }
      if (pending_list_right_.empty()) {
        if ((input_left_chan_->closed() && input_left_chan_->empty()) &&
            (input_right_chan_->closed() && input_right_chan_->empty()) && index_r_->Empty()) {
          output_right_chan_->close();  // no more r tuples or s ack to be sent when no input anymore
          spdlog::debug("Window {} closes right channel", id_);
        }
      }
    } else if (IsRightMost()) {
      // right-most sub-window sends "r" tuple to the null: directly pop the oldest tuple
      for (const auto &tuple : pending_list_right_) {
        if (tuple.ctl_ == TupleFlag::ACK_S) {
          continue;
        }
        auto tuple_del = index_r_->PopOldest();
        size_r_->store(index_r_->Size());
        assert(tuple_del.timestamp_ <= *newest_r_ts_);
        assert(!TimeStampMatched(tuple.timestamp_, *newest_s_ts_));
        *oldest_r_ts_ = tuple_del.timestamp_;
      }
      pending_list_right_.clear();
    }
  }

  auto SendToLeft(TupleType<KeyType, ValueType> &tuple) -> void {
    assert(tuple.ctl_ == TupleFlag::INPUT_S);
    assert(tuple.forwarded_ == false);
    pending_list_left_.emplace_back(tuple);
    tuple.forwarded_ = true;
  }

  auto SendToRight(TupleType<KeyType, ValueType> &tuple) -> void {
    assert(tuple.ctl_ == TupleFlag::ACK_S || tuple.ctl_ == TupleFlag::INPUT_R);
    assert(tuple.forwarded_ == false);
    pending_list_right_.emplace_back(tuple);
    tuple.forwarded_ = true;
  }

  auto ProcessAck(const TupleType<KeyType, ValueType> &tuple) -> void {
    auto &tuple_s = index_s_->GetOldestRef();
    (void)tuple_s;
    (void)tuple;
    assert(tuple_s.forwarded_);
    assert(tuple_s.ctl_ == TupleFlag::INPUT_S);
    assert(tuple_s.key_ == tuple.key_);
    assert(tuple_s.timestamp_ == tuple.timestamp_);
    assert(tuple_s.value_ == tuple.value_);

    index_s_->PopOldest();
    size_s_->store(index_s_->Size());
  }

  const size_t window_size_;

  ChannelPointer<KeyType, ValueType> output_left_chan_;           // send s tuples to the left
  std::deque<TupleType<KeyType, ValueType>> pending_list_left_;   // pending tuples to be sent
  ChannelPointer<KeyType, ValueType> output_right_chan_;          // send r tuples and ack(s) to the right
  std::deque<TupleType<KeyType, ValueType>> pending_list_right_;  // pending tuples to be sent
  static constexpr size_t FULL_THRESHOLD{0};                      // threshold for considering the channel is full

  ChannelPointer<KeyType, ValueType> input_left_chan_;   // receive r tuples or ack(s) from the left
  ChannelPointer<KeyType, ValueType> input_right_chan_;  // receive s tuples from the right

  std::unique_ptr<WindowIndex<KeyType, ValueType>> index_r_;  // total index of stream R
  std::unique_ptr<WindowIndex<KeyType, ValueType>> index_s_;  // sub-index of stream S

  std::atomic_uint64_t *size_r_;                       // size of index r, used for left window to forward tuples
  std::atomic_uint64_t *size_s_;                       // size of index s, used for right window to forward tuples
  volatile const std::atomic_uint64_t *size_r_right_;  // size of index r of the left neighbor window, null if left-most
  volatile const std::atomic_uint64_t
      *size_s_left_;  // size of index s of the right neighbor window, null if right-most
  // forwarding tuples at the ends
  volatile TsType *newest_r_ts_;         // timestamp of the newest tuple in the whole windows
  volatile TsType *newest_s_ts_;         // timestamp of the newest tuple in the whole windows
  volatile TsType *oldest_r_ts_;         // timestamp of the oldest tuple popped from the whole windows
  volatile TsType *oldest_s_ts_;         // timestamp of the oldest tuple popped from the whole windows
  const size_t END_BALANCING_THRESHOLD;  // max threshold to stop receiving new tuples at the ends

  std::ostream &os_;  // output stream for join results

  int32_t id_;  // id of the window (debugging purpose)
};

}  // namespace stream

#endif  // HANDSHAKE_WINDOW_HPP