/*
tpc_stream.cpp

Stream implementation for reading from a TPC-H dataset.
*/

#include <cstdint>
#include <sstream>

#include "tpc_stream.hpp"

stream::TPCStream::TPCStream(const std::string &file_path, int32_t key_column, int32_t value_column)
    : timestamp_(0), file_(file_path), key_column_(key_column), value_column_(value_column) {
  if (!file_.is_open()) {
    throw std::runtime_error("Failed to open file: " + file_path);
  }
  if (key_column < 0 || value_column < 0) {
    throw std::invalid_argument("Key and value columns must be positive");
  }
}

stream::TPCStream::~TPCStream() { file_.close(); }

auto stream::TPCStream::read(TsType &timestamp, std::string &key, std::string &value) -> bool {
  std::string line;
  if (!std::getline(file_, line)) {
    return false;
  }
  std::stringstream ss(line);

  std::string key_token;
  std::string value_token;
  if (key_column_ < value_column_) {  // key is before value
    for (int i = 0; i < key_column_; ++i) {
      if (ss.eof()) {
        return false;
      }
      std::getline(ss, key_token, '|');
    }
    for (int i = key_column_; i < value_column_; ++i) {
      if (ss.eof()) {
        return false;
      }
      std::getline(ss, value_token, '|');
    }
  } else {  // value is before key
    for (int i = 0; i < value_column_; ++i) {
      if (ss.eof()) {
        return false;
      }
      std::getline(ss, value_token, '|');
    }
    for (int i = value_column_; i < key_column_; ++i) {
      if (ss.eof()) {
        return false;
      }
      std::getline(ss, key_token, '|');
    }
  }

  key = std::move(key_token);
  value = std::move(value_token);
  timestamp = timestamp_++;
  return true;
}

auto stream::TPCStream::available() -> bool { return file_.good(); }

auto stream::TPCStream::eof() -> bool { return file_.eof(); }
