#ifndef INDEX_HPP_
#define INDEX_HPP_

#include <utility>
#include "types/types.hpp"

namespace stream {

template <typename KeyType, typename ValueType>
class WindowIndex {
 public:
  virtual ~WindowIndex() = default;

  /**
   * @brief Insert a tuple (timestamp + key value) into the index.
   *
   * @param tuple The tuple to be inserted.
   */
  virtual void Insert(const TupleType<KeyType, ValueType> &tuple) = 0;

  /**
   * @brief Remove the tuple with the oldest timestamp from the index.
   * This function should be called when the window is sliding.
   *
   * @throw std::out_of_range if the index is empty.
   */
  virtual auto PopOldest() -> TupleType<KeyType, ValueType> = 0;

  /**
   * @brief Get the tuple with the oldest timestamp without removing it from the index.
   *
   * @throw std::out_of_range if the index is empty.
   */
  virtual auto GetOldest() const -> TupleType<KeyType, ValueType> = 0;

  virtual auto GetOldestRef() -> TupleType<KeyType, ValueType> & = 0;

  /**
   * @brief Search all tuples within the given key range.
   *
   * @param key_range The range of keys to search.
   * @param result The vector to store the search results.
   */
  virtual auto RangeSearch(const std::pair<KeyType, KeyType> &key_range) const
      -> std::vector<TupleType<KeyType, ValueType>> = 0;

  /**
   * @brief Get the number of tuples in the index.
   *
   * @return The number of tuples in the index.
   */
  virtual auto Size() const -> size_t = 0;

  /**
   * @brief Check if the index is empty.
   *
   * @return true if the index is empty, false otherwise.
   */
  virtual auto Empty() const -> bool = 0;
};

}  // namespace stream

#endif  // INDEX_HPP_