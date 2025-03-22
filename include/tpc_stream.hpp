/*
tpc_stream.hpp

Stream implementation for reading from a TPC-H dataset.
*/

#pragma once

#include <fstream>
#include <string>

#include "stream.hpp"

namespace stream {

class TPCStream : public Stream<std::string, std::string> {
 public:
  TPCStream(const std::string &file_path, int key_column, int value_column);

  ~TPCStream() override;

  auto Read(TsType &timestamp, std::string &key, std::string &value) -> bool override;

 private:
  std::ifstream file_;
  int key_column_;
  int value_column_;
  TsType timestamp_;
};

}  // namespace stream