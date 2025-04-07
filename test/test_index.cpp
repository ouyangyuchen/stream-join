#include <gtest/gtest.h>
#include "index/list.hpp"
#include "types/types.hpp"

using stream::TupleType;

TEST(IndexTest, ListIndexInsertPop) {
  using stream::ListIndex;
  ListIndex<int, int> index;
  ASSERT_TRUE(index.Empty());
  ASSERT_EQ(index.Size(), 0);

  TupleType<int, int> tuple1 = {0, 1, 1};
  index.Insert(tuple1);
  ASSERT_FALSE(index.Empty());
  ASSERT_EQ(index.Size(), 1);

  TupleType<int, int> tuple2 = {1, 2, 2};
  index.Insert(tuple2);
  ASSERT_EQ(index.Size(), 2);

  auto tuple = index.PopOldest();
  ASSERT_EQ(tuple.timestamp_, 0);
  ASSERT_EQ(tuple.key_, 1);
  ASSERT_EQ(tuple.value_, 1);
  ASSERT_EQ(index.Size(), 1);

  tuple = index.PopOldest();
  ASSERT_EQ(tuple.timestamp_, 1);
  ASSERT_EQ(tuple.key_, 2);
  ASSERT_EQ(tuple.value_, 2);
  ASSERT_TRUE(index.Empty());
  ASSERT_EQ(index.Size(), 0);
}

TEST(IndexTest, ListIndexRangeSearch) {
  using stream::ListIndex;
  ListIndex<int, int> index;

  index.Insert({0, 1, 1});
  index.Insert({1, 2, 2});
  index.Insert({2, 3, 3});
  index.Insert({3, 4, 4});

  // search inside
  auto result = index.RangeSearch({2, 3});
  ASSERT_EQ(result.size(), 2);
  ASSERT_EQ(result[0].key_, 2);
  ASSERT_EQ(result[0].timestamp_, 1);
  ASSERT_EQ(result[1].key_, 3);
  ASSERT_EQ(result[1].timestamp_, 2);

  // search left outside
  result = index.RangeSearch({0, 1});
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(result[0].key_, 1);
  ASSERT_EQ(result[0].timestamp_, 0);

  // search right outside
  result = index.RangeSearch({4, 7});
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(result[0].key_, 4);
  ASSERT_EQ(result[0].timestamp_, 3);

  // pop the oldest tuple then range search should not include it
  index.PopOldest();
  result = index.RangeSearch({0, 3});
  ASSERT_EQ(result.size(), 2);
  ASSERT_EQ(result[0].key_, 2);
  ASSERT_EQ(result[0].timestamp_, 1);
  ASSERT_EQ(result[1].key_, 3);
  ASSERT_EQ(result[1].timestamp_, 2);
}