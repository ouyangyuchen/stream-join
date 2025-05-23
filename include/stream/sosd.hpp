#ifndef SOSD_STREAM_H
#define SOSD_STREAM_H

#include <fstream>
#include <vector>
#include "stream.hpp"

namespace stream {

template <typename T>
auto load_sosd(std::string filename) -> std::vector<T> {
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

  return v;
}

template <typename KeyType, typename ValueType = KeyType>
class SOSDStream : public Stream<KeyType, ValueType> {
 public:
  SOSDStream(std::string filename, size_t maxsize, bool shuffle = true);

  auto operator>>(TupleType<KeyType, ValueType> &tuple) -> Stream<KeyType, ValueType> & override;

  auto Available() -> bool override;

  auto Eof() -> bool override;

 private:
  std::vector<KeyType> keys_;    // vector of keys
  size_t current_index_{0};      // current index in the vector
  TsType current_timestamp_{0};  // current timestamp
  const size_t maxsize_;         // max size of the stream/vector
};

template <typename KeyType, typename ValueType>
SOSDStream<KeyType, ValueType>::SOSDStream(std::string filename, size_t maxsize, bool shuffle)
    : keys_(load_sosd<KeyType>(filename)), maxsize_(maxsize) {
  if (keys_.empty()) {
    throw std::runtime_error("SOSDStream: No data loaded");
  }

  // filter out the duplicate keys
  //   size_t original_size = keys_.size();
  //   auto last = std::unique(keys_.begin(), keys_.end());
  //   keys_.erase(last, keys_.end());
  //   size_t filtered_size = keys_.size();
  //   if (filtered_size != original_size) {
  //     std::cerr << "SOSDStream: Filtered out " << (original_size - filtered_size) << " duplicate keys" << std::endl;
  //   }

  if (shuffle) {
    std::shuffle(keys_.begin(), keys_.end(), std::default_random_engine(std::random_device()()));
  }
  if (maxsize_ > keys_.size()) {
    std::cerr << "Warn: Maxsize is too large, truncated to " << keys_.size() << std::endl;
  }
  keys_.resize(std::min(maxsize_, keys_.size()));
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
  return current_index_ < keys_.size();
}

template <typename KeyType, typename ValueType>
auto SOSDStream<KeyType, ValueType>::Eof() -> bool {
  return current_index_ >= keys_.size();
}

}  // namespace stream

#endif