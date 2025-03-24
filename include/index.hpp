/*
index.hpp

Index interface for stream join.
*/

#pragma once

#include <cstddef>
#include <tuple>
#include <vector>

#include "stream.hpp"

namespace stream {

/**
 * @brief Index interface for stream join.
 * @tparam KeyType The type of the key.
 * @tparam ValueType The type of the value.
 */
template <typename KeyType, typename ValueType>
class Index {
 public:
  using TupleType = std::tuple<TsType, KeyType, ValueType>;

  Index(TsType window_length) : window_length_(window_length) {}

  virtual ~Index() = default;

  /**
   * @brief Insert a tuple into the index, remove the tuples that are older than the tuple timestamp
   * - window_length.
   */
  virtual auto insert(const TupleType &tuple) -> void = 0;

  /**
   * @brief Search in the index for keys in the range [start, end].
   */
  virtual auto range_search(const KeyType &start,
                            const KeyType &end) const -> std::vector<TupleType> = 0;

  virtual auto size() const -> size_t = 0;

  auto get_window_length() const -> TsType { return window_length_; }

  static auto get_key(const TupleType &tuple) -> const KeyType & { return std::get<1>(tuple); }

  static auto get_value(const TupleType &tuple) -> const ValueType & { return std::get<2>(tuple); }

  static auto get_timestamp(const TupleType &tuple) -> const TsType & { return std::get<0>(tuple); }

  static auto get_key(TupleType &tuple) -> KeyType & { return std::get<1>(tuple); }

  static auto get_value(TupleType &tuple) -> ValueType & { return std::get<2>(tuple); }

  static auto get_timestamp(TupleType &tuple) -> TsType & { return std::get<0>(tuple); }

 protected:
  TsType window_length_;
};

}  // namespace stream