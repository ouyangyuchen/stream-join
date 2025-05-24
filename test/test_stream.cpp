#include <gtest/gtest.h>
#include <cstddef>
#include <optional>
#include "stream/random_stream.hpp"
#include "stream/sequential_stream.hpp"
#include "stream/sosd.hpp"
#include "stream/tpc_stream.hpp"
#include "types/types.hpp"

TEST(StreamTest, RandomStreamBasic) {
  stream::RandomStream stream(100);
  stream::TupleType<int64_t, int64_t> tuple;
  std::set<int64_t> keys;

  // Check if the stream is not at EOF
  ASSERT_FALSE(stream.Eof());
  ASSERT_TRUE(stream.Available());

  // Read a tuple from the stream
  stream >> tuple;

  // Check if the tuple is valid
  ASSERT_EQ(tuple.timestamp_, 0);
  ASSERT_GE(tuple.key_, 0);
  ASSERT_LT(tuple.key_, 100);
  keys.insert(tuple.key_);

  // Check if the stream is not at EOF after reading one tuple
  ASSERT_FALSE(stream.Eof());

  // Read until EOF
  for (int i = 1; i < 100; ++i) {
    ASSERT_TRUE(stream.Available());
    stream >> tuple;
    ASSERT_EQ(tuple.timestamp_, i);
    ASSERT_GE(tuple.key_, 0);
    ASSERT_LT(tuple.key_, 100);
    ASSERT_FALSE(keys.count(tuple.key_));  // Check if the key is unique
    keys.insert(tuple.key_);
  }
  ASSERT_EQ(keys.size(), 100);  // Check if all keys are unique

  // Check if the stream is at EOF after reading all tuples
  ASSERT_FALSE(stream.Available());
  ASSERT_TRUE(stream.Eof());
}

TEST(StreamTest, TPCStreamBasic) {
  const std::string file_path = "../data/tpc-h/nation.tbl";
  const int key_column = 3;
  const int value_column = 2;

  stream::TPCStream stream(file_path, key_column, value_column);
  stream::TupleType<int64_t, std::string> tuple;

  // Check if the stream is not at EOF
  ASSERT_FALSE(stream.Eof());
  ASSERT_TRUE(stream.Available());

  // Read a tuple from the stream
  stream >> tuple;
  ASSERT_EQ(tuple.timestamp_, 0);
  ASSERT_EQ(tuple.key_, 0);
  ASSERT_EQ(tuple.value_, "ALGERIA");

  stream >> tuple;
  ASSERT_EQ(tuple.timestamp_, 1);
  ASSERT_EQ(tuple.key_, 1);
  ASSERT_EQ(tuple.value_, "ARGENTINA");

  stream >> tuple;
  ASSERT_EQ(tuple.timestamp_, 2);
  ASSERT_EQ(tuple.key_, 1);
  ASSERT_EQ(tuple.value_, "BRAZIL");

  // Check if the stream is not at EOF after reading one tuple
  ASSERT_FALSE(stream.Eof());

  // read until eof
  for (int i = 3; i < 25; i++) {
    ASSERT_TRUE(stream.Available());
    stream >> tuple;
    ASSERT_EQ(tuple.timestamp_, i);
    // ASSERT_EQ(stream::get_key(tuple), i);
  }

  ASSERT_FALSE(stream.Available());
  ASSERT_TRUE(stream.Eof());
}

TEST(StreamTest, TupleReader2Streams) {
  size_t num_r_tuples = 10;
  size_t num_s_tuples = 20;
  auto r_stream = std::make_unique<stream::SequentialStream>(0, num_r_tuples);
  auto s_stream = std::make_unique<stream::SequentialStream>(0, num_s_tuples);

  stream::TupleReader<int64_t, int64_t> tuple_reader(std::move(r_stream), std::move(s_stream));
  stream::TsType ts = 0;
  size_t cnt = 0;

  std::optional<stream::TupleType<int64_t, int64_t>> tuple_opt;
  while ((tuple_opt = tuple_reader.GetNextTuple()).has_value()) {
    ASSERT_GE(tuple_opt->timestamp_, ts);
    ts = tuple_opt->timestamp_;
    cnt++;
  }
  ASSERT_EQ(num_r_tuples + num_s_tuples, cnt);

  int64_t r_step = 1;
  int64_t s_step = 5;
  int64_t r_end = 10;
  int64_t s_end = 20;
  num_r_tuples = r_end / r_step;
  num_s_tuples = s_end / s_step;

  r_stream = std::make_unique<stream::SequentialStream>(0, r_end, r_step);
  s_stream = std::make_unique<stream::SequentialStream>(0, s_end, s_step);  // the generation rate of S is less than R
  auto tuple_reader2 = stream::TupleReader<int64_t, int64_t>(std::move(r_stream), std::move(s_stream));
  ts = 0;
  cnt = 0;
  while ((tuple_opt = tuple_reader2.GetNextTuple()).has_value()) {
    ASSERT_GE(tuple_opt->timestamp_, ts);
    ts = tuple_opt->timestamp_;
    cnt++;
  }
  ASSERT_EQ(num_r_tuples + num_s_tuples, cnt);
}

TEST(StreamTest, TupleReaderSingleStream) {
  // Test with only R stream
  size_t num_tuples = 10;
  std::unique_ptr<stream::SequentialStream> r_stream = std::make_unique<stream::SequentialStream>(0, num_tuples);
  std::unique_ptr<stream::SequentialStream> s_stream = nullptr;

  stream::TupleReader<int64_t, int64_t> tuple_reader(std::move(r_stream), std::move(s_stream));
  stream::TsType ts = 0;
  size_t cnt = 0;

  std::optional<stream::TupleType<int64_t, int64_t>> tuple_opt;
  while ((tuple_opt = tuple_reader.GetNextTuple()).has_value()) {
    ASSERT_GE(tuple_opt->timestamp_, ts);
    ts = tuple_opt->timestamp_;
    cnt++;
  }
  ASSERT_EQ(num_tuples, cnt);

  // Test with only S stream
  num_tuples = 20;
  r_stream = nullptr;
  s_stream = std::make_unique<stream::SequentialStream>(0, num_tuples);
  auto tuple_reader2 = stream::TupleReader<int64_t, int64_t>(std::move(r_stream), std::move(s_stream));
  ts = 0;
  cnt = 0;
  while ((tuple_opt = tuple_reader2.GetNextTuple()).has_value()) {
    ASSERT_GE(tuple_opt->timestamp_, ts);
    ts = tuple_opt->timestamp_;
    cnt++;
  }
  ASSERT_EQ(num_tuples, cnt);
}

TEST(StreamTest, SOSDStreamBasic) {
  const std::string file_path = "../data/books_200M_uint64";
  size_t size = 100;
  auto data = stream::load_sosd<uint64_t>(file_path, true);

  stream::SOSDStream<uint64_t> stream(data, size);
  stream::TupleType<uint64_t, uint64_t> tuple;

  for (size_t i = 0; i < size; ++i) {
    ASSERT_TRUE(stream.Available());
    stream >> tuple;
    ASSERT_EQ(tuple.timestamp_, i);
    ASSERT_EQ(tuple.value_, tuple.key_);
    std::cout << tuple << std::endl;
  }

  ASSERT_FALSE(stream.Available());
  ASSERT_TRUE(stream.Eof());
  ASSERT_ANY_THROW(stream >> tuple);  // Attempt to read after EOF should throw an exception
}