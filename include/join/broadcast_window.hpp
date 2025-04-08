#ifndef JOIN_BROADCAST_WINDOW_HPP
#define JOIN_BROADCAST_WINDOW_HPP

#include <memory>
#include <utility>
#include "index/index.hpp"
#include "join/window.hpp"
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
  using ChannelPointer = std::shared_ptr<msd::channel<TupleType<KeyType, ValueType>>>;

  /**
   * @brief Constructor of BroadcastWindow.
   * @param diff join condition: |r.key - s.key| <= diff
   * @param window_len_S size of the sub window size of S stream
   * @param window_len_R size of the total window size of R stream
   * @param input_chan input channel for the tuples/events
   * @param id id of the window (debugging purpose)
   * @param os output stream for logging/debugging
   */
  BroadcastWindow(size_t window_len_S, size_t window_len_R, ChannelPointer input_chan,
                  int32_t id = -1, std::ostream &os = std::cout)
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
      this->os_ << tuple << " | " << result << "\n";
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
      this->os_ << tuple << " | " << result << "\n";
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

}  // namespace stream

#endif  // JOIN_BROADCAST_WINDOW_HPP