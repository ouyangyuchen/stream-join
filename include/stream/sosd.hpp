#ifndef SOSD_STREAM_H
#define SOSD_STREAM_H

#include <fstream>
#include <vector>
#include "stream.hpp"

namespace stream {

template <typename T>
auto load_sosd(std::string filename, bool shuffle = true) -> std::vector<T> {
  std::ifstream ifs(filename, std::ios::in | std::ios::binary);
  std::vector<T> v;

  if (!ifs.is_open()) {
    throw std::runtime_error("SOSDStream: Failed to open file " + filename);
  }

  T size;
  ifs.read(reinterpret_cast<char *>(&size), sizeof(T));
  v.resize(size);

  ifs.read(reinterpret_cast<char *>(v.data()), size * sizeof(T));  // read all data

  ifs.close();

  if (shuffle) {
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(v.begin(), v.end(), g);
  }

  return v;
}

template <typename KeyType, typename ValueType = KeyType>
class SOSDStream : public Stream<KeyType, ValueType> {
 public:
  SOSDStream(const std::vector<KeyType> &file_data, size_t maxsize);

  auto operator>>(TupleType<KeyType, ValueType> &tuple) -> Stream<KeyType, ValueType> & override;

  auto Available() -> bool override;

  auto Eof() -> bool override;

 private:
  const std::vector<KeyType> &keys_;  // vector of keys
  size_t current_index_{0};           // current index in the vector
  TsType current_timestamp_{0};       // current timestamp
  size_t maxsize_;                    // max size of the stream/vector
};

template <typename KeyType, typename ValueType>
SOSDStream<KeyType, ValueType>::SOSDStream(const std::vector<KeyType> &file_data, size_t maxsize)
    : keys_(file_data), maxsize_(maxsize) {
  if (maxsize_ > keys_.size()) {
    throw std::runtime_error("SOSDStream: maxsize exceeds the number of keys in the file");
  }
}

template <typename KeyType, typename ValueType>
auto SOSDStream<KeyType, ValueType>::operator>>(TupleType<KeyType, ValueType> &tuple) -> Stream<KeyType, ValueType> & {
  if (this->Eof()) {
    throw std::runtime_error("SOSDStream: End of stream reached");
  }
  auto timestamp = current_timestamp_++;
  auto &key = keys_[current_index_++];
  tuple = {timestamp, key, key};
  return *this;
}

template <typename KeyType, typename ValueType>
auto SOSDStream<KeyType, ValueType>::Available() -> bool {
  return current_index_ < maxsize_;
}

template <typename KeyType, typename ValueType>
auto SOSDStream<KeyType, ValueType>::Eof() -> bool {
  return current_index_ >= maxsize_;
}

}  // namespace stream

#endif
