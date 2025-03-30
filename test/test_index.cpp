#include <gtest/gtest.h>
#include "index_list.hpp"
#include "utils.hpp"

using stream::GetKey;
using stream::GetTimestamp;
using stream::GetValue;
using stream::MakeTuple;
using stream::TupleType;

TEST(IndexTest, ListIndexInsertPop) {
  using stream::ListIndex;
  ListIndex<int, int> index;
  ASSERT_TRUE(index.Empty());
  ASSERT_EQ(index.Size(), 0);

  TupleType<int, int> tuple1 = MakeTuple(0, 1, 1);
  index.Insert(tuple1);
  ASSERT_FALSE(index.Empty());
  ASSERT_EQ(index.Size(), 1);

  TupleType<int, int> tuple2 = MakeTuple(1, 2, 2);
  index.Insert(tuple2);
  ASSERT_EQ(index.Size(), 2);

  auto tuple = index.PopOldest();
  ASSERT_EQ(GetTimestamp(tuple), 0);
  ASSERT_EQ(GetKey(tuple), 1);
  ASSERT_EQ(GetValue(tuple), 1);
  ASSERT_EQ(index.Size(), 1);

  tuple = index.PopOldest();
  ASSERT_EQ(GetTimestamp(tuple), 1);
  ASSERT_EQ(GetKey(tuple), 2);
  ASSERT_EQ(GetValue(tuple), 2);
  ASSERT_TRUE(index.Empty());
  ASSERT_EQ(index.Size(), 0);
}

TEST(IndexTest, ListIndexRangeSearch) {
  using stream::ListIndex;
  ListIndex<int, int> index;

  index.Insert(MakeTuple(0, 1, 1));
  index.Insert(MakeTuple(1, 2, 2));
  index.Insert(MakeTuple(2, 3, 3));
  index.Insert(MakeTuple(3, 4, 4));

  // search inside
  auto result = index.RangeSearch({2, 3});
  ASSERT_EQ(result.size(), 2);
  ASSERT_EQ(GetKey(result[0]), 2);
  ASSERT_EQ(GetTimestamp(result[0]), 1);
  ASSERT_EQ(GetKey(result[1]), 3);
  ASSERT_EQ(GetTimestamp(result[1]), 2);

  // search left outside
  result = index.RangeSearch({0, 1});
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(GetKey(result[0]), 1);
  ASSERT_EQ(GetTimestamp(result[0]), 0);

  // search right outside
  result = index.RangeSearch({4, 7});
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(GetKey(result[0]), 4);
  ASSERT_EQ(GetTimestamp(result[0]), 3);

  // pop the oldest tuple then range search should not include it
  index.PopOldest();
  result = index.RangeSearch({0, 3});
  ASSERT_EQ(result.size(), 2);
  ASSERT_EQ(GetKey(result[0]), 2);
  ASSERT_EQ(GetTimestamp(result[0]), 1);
  ASSERT_EQ(GetKey(result[1]), 3);
  ASSERT_EQ(GetTimestamp(result[1]), 2);
}