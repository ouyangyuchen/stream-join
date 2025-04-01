#ifndef TPC_STREAM_HPP_
#define TPC_STREAM_HPP_

#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include "stream.hpp"
#include "utils.hpp"

namespace stream {
// TPCStream reads tuples from a TPC-H dataset file. The keys should be comparable.
class TPCStream : public Stream<int64_t, std::string> {
 public:
  TPCStream(const std::string &file_path, const int key_column, const int value_column)
      : file_(file_path), key_column_(key_column), value_column_(value_column) {
    if (!file_.is_open()) {
      throw std::runtime_error("Failed to open file: " + file_path);
    }
    if (key_column_ < 0 || value_column_ < 0) {
      throw std::invalid_argument("Invalid column index");
    }
  }

  ~TPCStream() override { file_.close(); }

  // Read a tuple from the stream
  auto operator>>(TupleType<int64_t, std::string> &tuple) -> TPCStream & override {
    if (this->Eof()) {
      throw std::runtime_error("End of stream reached");
    }

    std::string line;
    if (!std::getline(file_, line)) {
      throw std::runtime_error("Failed to read line from file");
    }
    std::stringstream ss(line);

    // read the key and value columns from the line string
    std::string key_token;
    std::string value_token;
    if (key_column_ < value_column_) {  // key is before value
      for (int i = 0; i < key_column_; ++i) {
        if (ss.eof()) {
          throw std::runtime_error("Unexpected end of file");
        }
        std::getline(ss, key_token, '|');
      }
      for (int i = key_column_; i < value_column_; ++i) {
        if (ss.eof()) {
          throw std::runtime_error("Unexpected end of file");
        }
        std::getline(ss, value_token, '|');
      }
    } else {  // value is before key
      for (int i = 0; i < value_column_; ++i) {
        if (ss.eof()) {
          throw std::runtime_error("Unexpected end of file");
        }
        std::getline(ss, value_token, '|');
      }
      for (int i = value_column_; i < key_column_; ++i) {
        if (ss.eof()) {
          throw std::runtime_error("Unexpected end of file");
        }
        std::getline(ss, key_token, '|');
      }
    }

    tuple = {timestamp_++, std::stoi(key_token), value_token};
    return *this;
  }

  auto Available() -> bool override { return file_.good(); }

  auto Eof() -> bool override { return file_.eof(); }

 private:
  std::ifstream file_;
  int key_column_;
  int value_column_;
  TsType timestamp_{};
};

}  // namespace stream

#endif  // TPC_STREAM_HPP_