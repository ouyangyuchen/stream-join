#include <cstdint>
#include <memory>
#include "broadcast_join.hpp"
#include "gtest/gtest.h"
#include "list_index.hpp"
#include "tpc_stream.hpp"

// TEST(Join, GetNextTuple) {
//   auto stream1 = std::make_unique<stream::TPCStream>("../data/tpc-h/nation.tbl", 1, 2);
//   auto stream2 = std::make_unique<stream::TPCStream>("../data/tpc-h/orders.tbl", 2, 3);
//   auto index1 = std::make_unique<stream::ListIndex<std::string, std::string>>(100);
//   auto index2 = std::make_unique<stream::ListIndex<std::string, std::string>>(100);

//   sjoin::Join<std::string, std::string> joiner(std::move(stream1), std::move(stream2),
//                                                std::move(index1), std::move(index2));

//   while (true) {
//     auto [stream_id, tuple] = joiner.get_next_tuple();
//     if (!tuple) {
//       break;
//     }
//     std::cout << (stream_id == sjoin::Join<std::string, std::string>::StreamID::R ? "R: " : "S:
//     ")
//               << std::get<0>(*tuple) << " " << std::get<1>(*tuple) << " " << std::get<2>(*tuple)
//               << std::endl;
//   }
// }

TEST(BroadcastJoin, SingleThread) {
  auto stream1 = std::make_unique<stream::TPCStream>("../data/tpc-h/nation.tbl", 1, 2);
  auto stream2 = std::make_unique<stream::TPCStream>("../data/tpc-h/nation.tbl", 1, 2);
  auto index1 = std::make_unique<stream::ListIndex<int32_t, std::string>>(3);
  auto index2 = std::make_unique<stream::ListIndex<int32_t, std::string>>(3);

  sjoin::BroadcastJoinSingleThread<int32_t, std::string> joiner(
      std::move(stream1), std::move(stream2), std::move(index1), std::move(index2));
  joiner.execute(2);
}
