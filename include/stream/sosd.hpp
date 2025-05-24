#ifndef SOSD_STREAM_HPP
#define SOSD_STREAM_HPP

#include <fstream>
#include <iostream>
#include <vector>
#include "stream.hpp"

namespace stream {

template <typename T>
auto load_binary(std::string filename) -> std::vector<T> {
  std::ifstream ifs(filename, std::ios::in | std::ios::binary);

  if (!ifs.is_open()) {
    throw std::runtime_error("Could not open file: " + filename);
  }

  std::vector<T> v;
  T size;

  ifs.read(reinterpret_cast<char *>(&size), sizeof(T));
  v.resize(size);

  ifs.read(reinterpret_cast<char *>(v.data()), size * sizeof(T));
  ifs.close();

  return v;
}

template <typename KeyType, typename ValueType = KeyType>
class SOSDStream : public Stream<KeyType, ValueType> {
 public:
  SOSDStream(std::string filename, size_t maxsize, bool shuffle = false);

  ~SOSDStream() override = default;

  auto operator>>(TupleType<KeyType, ValueType> &tuple) -> SOSDStream<KeyType, ValueType> & override;

  auto Available() -> bool override;

  auto Eof() -> bool override;

 private:
  std::vector<KeyType> keys_;
  size_t current_index_{0};
  TsType current_timestamp_{0};
};

template <typename KeyType, typename ValueType>
SOSDStream<KeyType, ValueType>::SOSDStream(std::string filename, size_t maxsize, bool shuffle)
    : keys_(load_binary<KeyType>(filename)) {
  if (shuffle) {
    std::shuffle(keys_.begin(), keys_.end(), std::mt19937(std::random_device()()));
  }

  if (maxsize > keys_.size()) {
    std::cerr << "Warning: maxsize is larger than the number of keys in the file. "
              << "Using the full size of the file: " << keys_.size() << std::endl;
    maxsize = keys_.size();
  }
  keys_.resize(maxsize);
}

template <typename KeyType, typename ValueType>
auto SOSDStream<KeyType, ValueType>::operator>>(TupleType<KeyType, ValueType> &tuple)
    -> SOSDStream<KeyType, ValueType> & {
  if (Eof()) {
    throw std::runtime_error("End of stream reached");
  }
  tuple.timestamp_ = current_timestamp_++;
  tuple.key_ = keys_[current_index_++];
  tuple.value_ = tuple.key_;  // Assuming value is the same as key
  return *this;
}

template <typename KeyType, typename ValueType>
auto SOSDStream<KeyType, ValueType>::Available() -> bool {
  return current_index_ < keys_.size();
}

template <typename KeyType, typename ValueType>
auto SOSDStream<KeyType, ValueType>::Eof() -> bool {
  return current_index_ >= keys_.size();
}

}  // namespace stream

#endif  // SOSD_STREAM_HPP