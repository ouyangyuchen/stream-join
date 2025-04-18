#ifndef STREAM_HPP_
#define STREAM_HPP_

#include <spdlog/spdlog.h>
#include <memory>
#include <optional>
#include "types/types.hpp"

namespace stream {

template <typename KeyType, typename ValueType>
class Stream {
 public:
  virtual ~Stream() = default;

  // Read a tuple from the stream
  virtual auto operator>>(TupleType<KeyType, ValueType> &tuple) -> Stream<KeyType, ValueType> & = 0;

  // Check if there are more than one tuple available for reading
  virtual auto Available() -> bool = 0;

  // Check if the end of the stream is reached
  virtual auto Eof() -> bool = 0;
};

template <typename KeyType, typename ValueType>
class TupleReader {
 public:
  TupleReader(std::unique_ptr<Stream<KeyType, ValueType>> r_stream,
              std::unique_ptr<Stream<KeyType, ValueType>> s_stream)
      : r_stream_(std::move(r_stream)), s_stream_(std::move(s_stream)) {
    if (!r_stream_ && !s_stream_) {
      throw std::invalid_argument("Streams cannot both be null");
    }
  }

  ~TupleReader() = default;
  TupleReader(const TupleReader &) = delete;
  auto operator=(const TupleReader &) -> TupleReader & = delete;
  TupleReader(TupleReader &&) = default;
  auto operator=(TupleReader &&) -> TupleReader & = default;

  /**
   * @brief Get the next tuple from the input streams R and S according to the timestamp.
   * @return the next tuple either from R or S stream, or nullopt if no more tuples.
   */
  auto GetNextTuple() -> std::optional<TupleType<KeyType, ValueType>> {
    auto return_tuple = [this](std::optional<TupleType<KeyType, ValueType>> &tuple_opt) {
      auto result = std::move(tuple_opt);
      tuple_opt.reset();
      last_ts_ = tuple_opt->timestamp_;
      return result;
    };

    while (r_stream_ && (!r_stream_->Eof() || r_tuple_) ||
           s_stream_ && (!s_stream_->Eof() || s_tuple_)) {
      ReadFrom(r_stream_, r_tuple_);
      ReadFrom(s_stream_, s_tuple_);

      if (!r_tuple_.has_value() && !s_tuple_.has_value()) {
        continue;  // both streams are not available now, wait for next round
      }
      if (r_tuple_.has_value() && !s_tuple_.has_value()) {
        r_tuple_->ctl_ = TupleFlag::INPUT_R;
        return return_tuple(r_tuple_);
      }
      if (!r_tuple_.has_value() && s_tuple_.has_value()) {
        s_tuple_->ctl_ = TupleFlag::INPUT_S;
        return return_tuple(s_tuple_);
      }
      if (r_tuple_.has_value() && s_tuple_.has_value()) {
        // both readable, return the earlier one
        auto r_ts = r_tuple_->timestamp_;
        auto s_ts = s_tuple_->timestamp_;
        if (r_ts <= s_ts) {
          r_tuple_->ctl_ = TupleFlag::INPUT_R;
          return return_tuple(r_tuple_);
        }
        s_tuple_->ctl_ = TupleFlag::INPUT_S;
        return return_tuple(s_tuple_);
      }
    }
    return std::nullopt;  // no more tuples
  }

 private:
  void ReadFrom(std::unique_ptr<Stream<KeyType, ValueType>> &stream,
                std::optional<TupleType<KeyType, ValueType>> &tuple) {
    if (stream != nullptr && !tuple.has_value() && stream->Available()) {
      tuple = std::make_optional<TupleType<KeyType, ValueType>>();
      *stream >> *tuple;
      if (tuple->timestamp_ < last_ts_) {
        throw std::runtime_error("TupleReader: Timestamp is decreasing");
      }
    }
  }

  TsType last_ts_{0};  // the last timestamp that returns to the user
  std::optional<TupleType<KeyType, ValueType>> r_tuple_;
  std::optional<TupleType<KeyType, ValueType>> s_tuple_;

  // The streams to read from
  std::unique_ptr<Stream<KeyType, ValueType>> r_stream_;
  std::unique_ptr<Stream<KeyType, ValueType>> s_stream_;
};

}  // namespace stream

#endif  // STREAM_HPP_