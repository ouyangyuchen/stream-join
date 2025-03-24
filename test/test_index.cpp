#include <tuple>
#include "gtest/gtest.h"

#include "bplustree.hpp"
#include "list_index.hpp"

auto basic_test(stream::Index<int, int> &index) -> void {
  index.insert(std::make_tuple(1, 11, 1));
  index.insert(std::make_tuple(2, 12, 2));
  index.insert(std::make_tuple(3, 13, 3));

  auto result = index.range_search(11, 13);
  ASSERT_EQ(result.size(), 3);
  ASSERT_EQ(result[0], std::make_tuple(1, 11, 1));
  ASSERT_EQ(result[1], std::make_tuple(2, 12, 2));
  ASSERT_EQ(result[2], std::make_tuple(3, 13, 3));
  result = index.range_search(11, 12);
  ASSERT_EQ(result.size(), 2);
  ASSERT_EQ(result[0], std::make_tuple(1, 11, 1));
  ASSERT_EQ(result[1], std::make_tuple(2, 12, 2));

  index.insert(std::make_tuple(4, 14, 4));
  result = index.range_search(11, 14);
  ASSERT_EQ(result.size(), 4);
  ASSERT_EQ(result[0], std::make_tuple(1, 11, 1));
  ASSERT_EQ(result[1], std::make_tuple(2, 12, 2));
  ASSERT_EQ(result[2], std::make_tuple(3, 13, 3));
  ASSERT_EQ(result[3], std::make_tuple(4, 14, 4));

  result = index.range_search(12, 14);
  ASSERT_EQ(result.size(), 3);
  ASSERT_EQ(result[0], std::make_tuple(2, 12, 2));
  ASSERT_EQ(result[1], std::make_tuple(3, 13, 3));
  ASSERT_EQ(result[2], std::make_tuple(4, 14, 4));

  // insert a duplicate key which should be ignored
  index.insert(std::make_tuple(5, 13, 5));
  ASSERT_EQ(index.size(), 4);
  result = index.range_search(11, 14);
  ASSERT_EQ(result.size(), 4);
  ASSERT_EQ(result[0], std::make_tuple(1, 11, 1));
  ASSERT_EQ(result[1], std::make_tuple(2, 12, 2));
  ASSERT_EQ(result[2], std::make_tuple(3, 13, 3));
  ASSERT_EQ(result[3], std::make_tuple(4, 14, 4));
}

auto window_overflow_test(stream::Index<int, int> &index) -> void {
  auto window_length = index.get_window_length();
  ASSERT_EQ(window_length, 2);

  // [1, 2]
  index.insert(std::make_tuple(1, 1, 1));
  index.insert(std::make_tuple(2, 2, 2));
  auto result = index.range_search(1, 1);
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(result[0], std::make_tuple(1, 1, 1));

  // [2, 3], pop 1
  index.insert(std::make_tuple(3, 3, 3));
  ASSERT_EQ(index.size(), 2);
  result = index.range_search(1, 3);
  ASSERT_EQ(result.size(), 2);
  ASSERT_EQ(result[0], std::make_tuple(2, 2, 2));
  ASSERT_EQ(result[1], std::make_tuple(3, 3, 3));

  // [3, 4], pop 2
  index.insert(std::make_tuple(4, 4, 4));
  ASSERT_EQ(index.size(), 2);
  result = index.range_search(4, 4);
  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(result[0], std::make_tuple(4, 4, 4));
  result = index.range_search(2, 2);
  ASSERT_EQ(result.size(), 0);
}

auto range_search_test(stream::Index<int, int> &index) -> void {
  // insert 100 tuples with timestamp from 0 to 99, key from 0 to 99, value from 0 to 99
  auto timestamp = 0;
  auto key = 0;
  auto value = 0;
  while (timestamp < 100) {
    index.insert(std::make_tuple(timestamp, key, value));
    timestamp++;
    key++;
    value++;
  }

  auto result = index.range_search(90, 110);
  ASSERT_EQ(result.size(), 10);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(result[i], std::make_tuple(90 + i, 90 + i, 90 + i));
  }

  auto result2 = index.range_search(95, 98);
  ASSERT_EQ(result2.size(), 4);
  for (int i = 0; i < 4; i++) {
    ASSERT_EQ(result2[i], std::make_tuple(95 + i, 95 + i, 95 + i));
  }

  auto result3 = index.range_search(100, 105);
  ASSERT_EQ(result3.size(), 0);
}

TEST(ListIndexTest, Basic) {
  stream::ListIndex<int, int> index(10);
  basic_test(index);
}

TEST(ListIndexTest, WindowOverflow) {
  // [1, 2]
  stream::ListIndex<int, int> index(2);
  window_overflow_test(index);
}

TEST(ListIndexTest, RangeSearch) {
  stream::ListIndex<int, int> index(10);
  range_search_test(index);
}

TEST(BPlusTreeIndexTest, Basic) {
  stream::BPlusTreeIndex<int, int> index(10);
  basic_test(index);
}

TEST(BPlusTreeIndexTest, WindowOverflow) {
  stream::BPlusTreeIndex<int, int> index(2);
  window_overflow_test(index);
}

TEST(BPlusTreeIndexTest, RangeSearch) {
  stream::BPlusTreeIndex<int, int> index(10);
  range_search_test(index);
}
