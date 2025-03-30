#include <gtest/gtest.h>
#include "random_stream.hpp"
#include "tpc_stream.hpp"
#include "utils.hpp"

TEST(StreamTest, RandomStreamBasic) {
  stream::RandomStream stream(100, {0, 10});
  stream::TupleType<int32_t, int32_t> tuple;

  // Check if the stream is not at EOF
  ASSERT_FALSE(stream.eof());
  ASSERT_TRUE(stream.available());

  // Read a tuple from the stream
  stream >> tuple;

  // Check if the tuple is valid
  ASSERT_EQ(stream::get_timestamp(tuple), 0);
  ASSERT_GE(stream::get_key(tuple), 0);
  ASSERT_LE(stream::get_key(tuple), 10);

  // Check if the stream is not at EOF after reading one tuple
  ASSERT_FALSE(stream.eof());

  // Read until EOF
  for (int i = 1; i < 100; ++i) {
    ASSERT_TRUE(stream.available());
    stream >> tuple;
    ASSERT_EQ(stream::get_timestamp(tuple), i);
    ASSERT_GE(stream::get_key(tuple), 0);
    ASSERT_LE(stream::get_key(tuple), 10);
  }

  // Check if the stream is at EOF after reading all tuples
  ASSERT_FALSE(stream.available());
  ASSERT_TRUE(stream.eof());
}

TEST(StreamTest, TPCStreamBasic) {
  const std::string file_path = "../data/tpc-h/nation.tbl";
  const int key_column = 3;
  const int value_column = 2;

  stream::TPCStream stream(file_path, key_column, value_column);
  stream::TupleType<int64_t, std::string> tuple;

  // Check if the stream is not at EOF
  ASSERT_FALSE(stream.eof());
  ASSERT_TRUE(stream.available());

  // Read a tuple from the stream
  stream >> tuple;
  ASSERT_EQ(stream::get_timestamp(tuple), 0);
  ASSERT_EQ(stream::get_key(tuple), 0);
  ASSERT_EQ(stream::get_value(tuple), "ALGERIA");

  stream >> tuple;
  ASSERT_EQ(stream::get_timestamp(tuple), 1);
  ASSERT_EQ(stream::get_key(tuple), 1);
  ASSERT_EQ(stream::get_value(tuple), "ARGENTINA");

  stream >> tuple;
  ASSERT_EQ(stream::get_timestamp(tuple), 2);
  ASSERT_EQ(stream::get_key(tuple), 1);
  ASSERT_EQ(stream::get_value(tuple), "BRAZIL");

  // Check if the stream is not at EOF after reading one tuple
  ASSERT_FALSE(stream.eof());

  // read until eof
  for (int i = 3; i < 25; i++) {
    ASSERT_TRUE(stream.available());
    stream >> tuple;
    ASSERT_EQ(stream::get_timestamp(tuple), i);
    // ASSERT_EQ(stream::get_key(tuple), i);
  }

  ASSERT_FALSE(stream.available());
  ASSERT_TRUE(stream.eof());
}