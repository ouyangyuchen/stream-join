/*
test_stream.cpp

Unit tests for the stream interface and its implementations.
*/

#include <gtest/gtest.h>

#include "random_stream.hpp"
#include "tpc_stream.hpp"

TEST(StreamTest, RandomStream) {
  stream::RandomStream stream(10, std::make_pair(0, 9));
  stream::TsType timestamp;
  int32_t key, value;
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(stream.available());
    ASSERT_FALSE(stream.eof());
    ASSERT_TRUE(stream.read(timestamp, key, value));
    ASSERT_EQ(timestamp, i);
    ASSERT_GE(key, 0);
    ASSERT_LE(key, 9);
    ASSERT_EQ(key, value);
  }
  ASSERT_FALSE(stream.available());
  ASSERT_TRUE(stream.eof());
  ASSERT_FALSE(stream.read(timestamp, key, value));
}

TEST(StreamTest, RandomStreamLong) {
  stream::RandomStream stream(1000000, std::make_pair(1, 2560));
  stream::TsType timestamp;
  int32_t key, value;
  for (int i = 0; i < 1000000; ++i) {
    ASSERT_TRUE(stream.available());
    ASSERT_TRUE(stream.read(timestamp, key, value));
    ASSERT_EQ(timestamp, i);
    ASSERT_GE(key, 1);
    ASSERT_LE(key, 2560);
    ASSERT_EQ(key, value);
  }
  ASSERT_FALSE(stream.available());
  ASSERT_TRUE(stream.eof());
  ASSERT_FALSE(stream.read(timestamp, key, value));
}

TEST(StreamTest, RandomStreamIllegalRange) {
  ASSERT_THROW(stream::RandomStream stream(10, std::make_pair(9, 0)), std::invalid_argument);
}

TEST(StreamTest, TPCStream) {
  stream::TPCStream stream("../data/tpc-h/nation.tbl", 1, 2);
  stream::TsType timestamp;
  int32_t key;
  std::string value;

  ASSERT_TRUE(stream.available());
  ASSERT_FALSE(stream.eof());
  ASSERT_TRUE(stream.read(timestamp, key, value));
  ASSERT_EQ(timestamp, 0);
  ASSERT_EQ(key, 0);
  ASSERT_EQ(value, "ALGERIA");

  ASSERT_TRUE(stream.available());
  ASSERT_FALSE(stream.eof());
  ASSERT_TRUE(stream.read(timestamp, key, value));
  ASSERT_EQ(timestamp, 1);
  ASSERT_EQ(key, 1);
  ASSERT_EQ(value, "ARGENTINA");

  ASSERT_TRUE(stream.available());
  ASSERT_FALSE(stream.eof());
  ASSERT_TRUE(stream.read(timestamp, key, value));
  ASSERT_EQ(timestamp, 2);
  ASSERT_EQ(key, 2);
  ASSERT_EQ(value, "BRAZIL");

  for (int i = 3; i < 25; ++i) {
    ASSERT_TRUE(stream.available());
    ASSERT_FALSE(stream.eof());
    ASSERT_TRUE(stream.read(timestamp, key, value));
    ASSERT_EQ(timestamp, i);
  }
  ASSERT_FALSE(stream.available());
  ASSERT_TRUE(stream.eof());
  ASSERT_FALSE(stream.read(timestamp, key, value));

  stream::TPCStream stream2("../data/tpc-h/nation.tbl", 3, 2);
  ASSERT_TRUE(stream2.read(timestamp, key, value));
  ASSERT_EQ(timestamp, 0);
  ASSERT_EQ(key, 0);
  ASSERT_EQ(value, "ALGERIA");

  ASSERT_TRUE(stream2.read(timestamp, key, value));
  ASSERT_EQ(timestamp, 1);
  ASSERT_EQ(key, 1);
  ASSERT_EQ(value, "ARGENTINA");

  ASSERT_TRUE(stream2.read(timestamp, key, value));
  ASSERT_EQ(timestamp, 2);
  ASSERT_EQ(key, 1);
  ASSERT_EQ(value, "BRAZIL");

  ASSERT_FALSE(stream2.eof());
}

TEST(StreamTest, TPCStreamIllegal) {
  ASSERT_THROW(stream::TPCStream stream("../data/tpc-h/nation.tbl", -1, 2), std::invalid_argument);
  ASSERT_THROW(stream::TPCStream stream("../data/tpc-h/nation.tbl", 2, -1), std::invalid_argument);

  // column out of range
  stream::TPCStream stream("../data/tpc-h/nation.tbl", 5, 2);
  stream::TsType timestamp;
  int32_t key;
  std::string value;
  ASSERT_THROW(stream.read(timestamp, key, value), std::runtime_error);

  stream::TPCStream stream2("../data/tpc-h/nation.tbl", 2, 5);
  ASSERT_THROW(stream2.read(timestamp, key, value), std::runtime_error);
}
