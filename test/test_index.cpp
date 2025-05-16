#include <gtest/gtest.h>
#include "index/bplustree.hpp"
#include "index/list.hpp"
#include "types/types.hpp"

using stream::TupleType;

void test_insert_pop(stream::WindowIndex<int, int> &index) {
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

void test_range_search(stream::WindowIndex<int, int> &index) {
  index.Insert({0, 1, 1});
  index.Insert({1, 2, 2});
  index.Insert({2, 3, 3});
  index.Insert({3, 4, 4});
  index.Insert({4, 1, 5});

  // search inside
  auto result = index.RangeSearch({2, 3});
  ASSERT_EQ(result.size(), 2);
  ASSERT_EQ(result[0].key_, 2);
  ASSERT_EQ(result[0].timestamp_, 1);
  ASSERT_EQ(result[1].key_, 3);
  ASSERT_EQ(result[1].timestamp_, 2);

  // search left outside
  result = index.RangeSearch({0, 1});
  ASSERT_EQ(result.size(), 2);

  // search right outside
  result = index.RangeSearch({4, 7});
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(result[0].key_, 4);
  ASSERT_EQ(result[0].timestamp_, 3);

  // pop the oldest tuple then range search should not include it
  auto tuple_del = index.PopOldest();
  result = index.RangeSearch({0, 3});
  ASSERT_EQ(result.size(), 3);
  ASSERT_EQ(tuple_del.key_, 1);
  ASSERT_EQ(tuple_del.timestamp_, 0);
  ASSERT_EQ(tuple_del.value_, 1);

  tuple_del = index.PopOldest();
  ASSERT_EQ(tuple_del.key_, 2);
  ASSERT_EQ(tuple_del.timestamp_, 1);
  ASSERT_EQ(tuple_del.value_, 2);

  tuple_del = index.PopOldest();
  ASSERT_EQ(tuple_del.key_, 3);
  ASSERT_EQ(tuple_del.timestamp_, 2);
  ASSERT_EQ(tuple_del.value_, 3);

  result = index.RangeSearch({0, 3});
  ASSERT_EQ(result.size(), 1);
  result = index.RangeSearch({0, 4});
  ASSERT_EQ(result.size(), 2);

  tuple_del = index.PopOldest();
  ASSERT_EQ(tuple_del.key_, 4);
  ASSERT_EQ(tuple_del.timestamp_, 3);
  ASSERT_EQ(tuple_del.value_, 4);

  result = index.RangeSearch({0, 3});
  ASSERT_EQ(result.size(), 1);

  auto &tuple_oldest = index.GetOldestRef();
  ASSERT_EQ(tuple_oldest.key_, 1);
  ASSERT_EQ(tuple_oldest.timestamp_, 4);
  ASSERT_EQ(tuple_oldest.value_, 5);

  tuple_oldest.key_ = 2;

  tuple_del = index.PopOldest();
  ASSERT_EQ(tuple_del.key_, 2);
  ASSERT_EQ(tuple_del.timestamp_, 4);
  ASSERT_EQ(tuple_del.value_, 5);

  ASSERT_EQ(index.Size(), 0);
  ASSERT_TRUE(index.Empty());
}

TEST(IndexTest, ListIndexInsertPop) {
  using stream::ListIndex;
  ListIndex<int, int> list;
  test_insert_pop(list);
}

TEST(IndexTest, ListIndexRangeSearch) {
  using stream::ListIndex;
  ListIndex<int, int> list;
  test_range_search(list);
}

TEST(IndexTest, BPlusTreeIndexInsertPop) {
  using stream::BPlusTreeIndex;
  BPlusTreeIndex<int, int> bptree;
  test_insert_pop(bptree);
}

TEST(IndexTest, BPlusTreeIndexRangeSearch) {
  using stream::BPlusTreeIndex;
  BPlusTreeIndex<int, int> bptree;
  test_range_search(bptree);
}