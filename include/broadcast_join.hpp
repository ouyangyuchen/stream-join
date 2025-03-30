#pragma once

#include "index.hpp"
#include "join.hpp"

namespace sjoin {

template <typename KeyType, typename ValueType>
class BroadcastJoinSingleThread : public Join<KeyType, ValueType> {
 public:
  BroadcastJoinSingleThread(std::unique_ptr<stream::Stream<KeyType, ValueType>> R,
                            std::unique_ptr<stream::Stream<KeyType, ValueType>> S,
                            std::unique_ptr<stream::Index<KeyType, ValueType>> R_index,
                            std::unique_ptr<stream::Index<KeyType, ValueType>> S_index,
                            std::ostream &os = std::cout)
      : Join<KeyType, ValueType>(std::move(R), std::move(S), std::move(R_index), std::move(S_index),
                                 os) {}

  auto execute(const KeyType &diff) -> void override {
    if (diff < 0) {
      throw std::invalid_argument("diff must be non-negative");
    }

    size_t join_count = 0;
    auto print_match_tuples =
        [this, &join_count](
            const typename stream::Index<KeyType, ValueType>::TupleType &r,
            const typename stream::Index<KeyType, ValueType>::TupleType &s) -> void {
      ++join_count;
      this->os_ << "R: " << stream::Index<KeyType, ValueType>::get_timestamp(r) << " "
                << stream::Index<KeyType, ValueType>::get_key(r) << " "
                << stream::Index<KeyType, ValueType>::get_value(r) << "\t"
                << "S: " << stream::Index<KeyType, ValueType>::get_timestamp(s) << " "
                << stream::Index<KeyType, ValueType>::get_key(s) << " "
                << stream::Index<KeyType, ValueType>::get_value(s) << "\n";
    };
    while (!this->R_->eof() && !this->S_->eof()) {
      auto [stream_id, tuple] = this->get_next_tuple();
      if (!tuple) continue;
      const auto &tuple_ref = *tuple;

      // calculte the predicate range of the other stream from key and diff
      const auto &key = stream::Index<KeyType, ValueType>::get_key(tuple_ref);
      KeyType start = key - diff;
      KeyType end = key + diff;

      // indexed-based window broadcast join:
      // 1. broadcast the tuple to all partitions of the other stream, and write to the output
      // stream
      // 2. insert the tuple to the index of the other stream
      // 3. remove expired tuples from the index
      std::vector<typename Join<KeyType, ValueType>::TupleType> result;
      switch (stream_id) {
        case Join<KeyType, ValueType>::StreamID::R:
          result = this->S_index_->range_search(start, end);
          for (const auto &t : result) {
            print_match_tuples(tuple_ref, t);
          }
          this->R_index_->insert(tuple_ref);
          break;
        case Join<KeyType, ValueType>::StreamID::S:
          result = this->R_index_->range_search(start, end);
          for (const auto &t : result) {
            print_match_tuples(t, tuple_ref);
          }
          this->S_index_->insert(tuple_ref);
          break;
      }
    }
    std::cout << "[Total Count]: " << join_count << "\n";
  }
};

}  // namespace sjoin