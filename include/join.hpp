#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <ostream>

#include "index.hpp"
#include "stream.hpp"

namespace sjoin {

/**
 * @brief The base interface for all join algorithms.
 */
template <typename KeyType, typename ValueType>
class Join {
 public:
  enum struct StreamID { R, S };
  using TupleType = typename stream::Index<KeyType, ValueType>::TupleType;

  /**
   * @brief Constructor.
   * @param R The first input stream.
   * @param S The second input stream.
   * @param R_index The index struct for the first input stream.
   * @param S_index The index struct for the second input stream.
   */
  Join(std::unique_ptr<stream::Stream<KeyType, ValueType>> R,
       std::unique_ptr<stream::Stream<KeyType, ValueType>> S,
       std::unique_ptr<stream::Index<KeyType, ValueType>> R_index,
       std::unique_ptr<stream::Index<KeyType, ValueType>> S_index, std::ostream &os = std::cout)
      : R_(std::move(R)),
        S_(std::move(S)),
        R_index_(std::move(R_index)),
        S_index_(std::move(S_index)),
        os_(os) {}

  /**
   * @brief Execute the join of the 2 input streams and write the result to the output stream.
   * @param diff is the filter function: |key1 - key2| <= diff
   */
  virtual auto execute(const KeyType &diff) -> void = 0;

 protected:
  /**
   * @brief Get the next tuple from the join.
   * @return The next tuple and which stream it is from. Or std::nullopt if no tuple is available.
   */
  auto get_next_tuple() -> std::pair<StreamID, std::optional<TupleType>> {
    if (!rp_ && R_->available()) {
      rp_ = std::make_optional(TupleType());
      auto &timestamp = R_index_->get_timestamp(*rp_);
      auto &key = R_index_->get_key(*rp_);
      auto &value = R_index_->get_value(*rp_);
      if (!R_->read(timestamp, key, value)) {
        rp_ = std::nullopt;
      }
    }
    if (!sp_ && S_->available()) {
      sp_ = std::make_optional(TupleType());
      auto &timestamp = S_index_->get_timestamp(*sp_);
      auto &key = S_index_->get_key(*sp_);
      auto &value = S_index_->get_value(*sp_);
      if (!S_->read(timestamp, key, value)) {
        sp_ = std::nullopt;
      }
    }

    // return the tuple with the smallest timestamp
    if (rp_ && (!sp_ || R_index_->get_timestamp(*rp_) <= S_index_->get_timestamp(*sp_))) {
      auto result = std::make_optional(std::move(*rp_));
      rp_ = std::nullopt;
      return std::make_pair(StreamID::R, result);
    } else if (sp_ && (!rp_ || S_index_->get_timestamp(*sp_) < R_index_->get_timestamp(*rp_))) {
      auto result = std::make_optional(std::move(*sp_));
      sp_ = std::nullopt;
      return std::make_pair(StreamID::S, result);
    }
    return std::make_pair(StreamID::R, std::nullopt);
  };

  std::unique_ptr<stream::Stream<KeyType, ValueType>> R_;
  std::unique_ptr<stream::Stream<KeyType, ValueType>> S_;
  std::unique_ptr<stream::Index<KeyType, ValueType>> R_index_;
  std::unique_ptr<stream::Index<KeyType, ValueType>> S_index_;

  std::ostream &os_;  // output stream

 private:
  std::optional<TupleType> rp_;
  std::optional<TupleType> sp_;
};

}  // namespace sjoin
