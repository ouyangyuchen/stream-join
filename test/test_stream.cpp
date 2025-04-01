#include <gtest/gtest.h>
#include "random_stream.hpp"
#include "tpc_stream.hpp"
#include "utils.hpp"

TEST(StreamTest, RandomStreamBasic) {
  stream::RandomStream stream(100, {0, 10});
  stream::TupleType<int64_t, int64_t> tuple;

  // Check if the stream is not at EOF
  ASSERT_FALSE(stream.Eof());
  ASSERT_TRUE(stream.Available());

  // Read a tuple from the stream
  stream >> tuple;

  // Check if the tuple is valid
  ASSERT_EQ(tuple.timestamp_, 0);
  ASSERT_GE(tuple.key_, 0);
  ASSERT_LE(tuple.key_, 10);

  // Check if the stream is not at EOF after reading one tuple
  ASSERT_FALSE(stream.Eof());

  // Read until EOF
  for (int i = 1; i < 100; ++i) {
    ASSERT_TRUE(stream.Available());
    stream >> tuple;
    ASSERT_EQ(tuple.timestamp_, i);
    ASSERT_GE(tuple.key_, 0);
    ASSERT_LE(tuple.key_, 10);
  }

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