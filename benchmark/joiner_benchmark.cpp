#include <cstdint>
#include <iomanip>
#include <memory>
#include <ostream>
#include <sstream>
#include <utility>
#include <vector>

#include "index/bplustree.hpp"
#include "index/list.hpp"
#include "join/broadcast_join.hpp"
#include "join/handshake_join.hpp"
#include "stream/random_stream.hpp"
#include "stream/sequential_stream.hpp"
#include "stream/tpc_stream.hpp"
#include "types/types.hpp"
#include "utils/decorator.hpp"

// --- Benchmark Configuration ---
constexpr int64_t TUPLES_R = 200000;         // Number of tuples for stream R
constexpr int64_t TUPLES_S = 200000;         // Number of tuples for stream S
constexpr size_t WINDOW_SIZE = 50000;        // Window size
constexpr int64_t DIFF = 2000;               // Join condition difference |r.key - s.key| <= diff
constexpr size_t CHANNEL_BUFFER_SIZE = 128;  // Buffer size for channels

const std::vector<int64_t> WORKERS = {1,  2,  3,  4,  5,  6,  7,  8, 12,
                                      16, 20, 24, 28, 32, 36, 42, 48};  // Number of workers to test

// --- Stream Configuration ---
using KeyType = int64_t;
using ValueType = int64_t;
using StreamType = stream::RandomStream;

// SEQUENTIAL STREAM
constexpr int64_t SEQ_START = 0;  // Start of the sequential stream
constexpr int64_t SEQ_STEP = 1;   // Step size for the sequential stream

// TPC Stream
const std::string TPC_R_PATH = "../data/tpc-h/orders.tbl";
constexpr int r_key_column = 2;
constexpr int r_value_column = 1;
const std::string TPC_S_PATH = "../data/tpc-h/orders.tbl";
constexpr int s_key_column = 2;
constexpr int s_value_column = 1;

// --- Handshake Joiner Benchmark ---

template <typename IndexType>
static void BM_HandshakeJoiner(size_t num_workers) {
  const int64_t total_tuples = TUPLES_R + TUPLES_S;
  (void)total_tuples;
  std::ostringstream discard_stream;
  std::string label = "HandshakeJoiner " + IndexType::Name + "/" + std::to_string(num_workers) + "w";

  // Create streams for each iteration
  // auto r = std::make_unique<StreamType>(SEQ_START, TUPLES_R, SEQ_STEP);
  // auto s = std::make_unique<StreamType>(SEQ_START, TUPLES_S, SEQ_STEP);
  auto r = std::make_unique<StreamType>(TUPLES_R);
  auto s = std::make_unique<StreamType>(TUPLES_S);

  stream::HandshakeJoiner<KeyType, ValueType, IndexType> joiner(num_workers, WINDOW_SIZE, CHANNEL_BUFFER_SIZE,
                                                                std::move(r), std::move(s), discard_stream);
  // joiner.StartWatcher();

  auto start_time = std::chrono::high_resolution_clock::now();
  joiner.Start(DIFF);
  auto end_time = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
  double throughput = static_cast<double>(total_tuples) / (duration / 1e6);  // tuples per second
  std::cout << std::left << std::setw(30) << label << ": " << std::setw(12) << duration << " us | " << std::setw(20)
            << std::fixed << std::setprecision(2) << throughput << " tuples/s (end to end)" << std::endl;
}

// --- Broadcast Joiner Benchmark ---

template <typename IndexType>
static void BM_BroadcastJoiner(size_t num_workers) {
  const int64_t per_window_total_tuples = TUPLES_R + TUPLES_S / num_workers;
  const size_t end_to_end_total_tuples = TUPLES_R + TUPLES_S;
  std::ostringstream discard_stream;
  std::string label = "BroadcastJoiner " + IndexType::Name + "/" + std::to_string(num_workers) + "w";

  // auto r = std::make_unique<StreamType>(SEQ_START, TUPLES_R, SEQ_STEP);
  // auto s = std::make_unique<StreamType>(SEQ_START, TUPLES_S, SEQ_STEP);
  auto r = std::make_unique<StreamType>(TUPLES_R);
  auto s = std::make_unique<StreamType>(TUPLES_S);

  stream::BroadcastJoiner<KeyType, ValueType, IndexType> joiner(num_workers, WINDOW_SIZE, CHANNEL_BUFFER_SIZE,
                                                                std::move(r), std::move(s), discard_stream);
  auto start_time = std::chrono::high_resolution_clock::now();
  joiner.Start(DIFF);
  auto end_time = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
  double per_window_throughput = static_cast<double>(per_window_total_tuples) / (duration / 1e6);  // tuples per second
  double end_to_end_throughput = static_cast<double>(end_to_end_total_tuples) / (duration / 1e6);  // tuples per second

  std::cout << std::left << std::setw(30) << label << ": " << std::setw(12) << duration << " us | " << std::setw(20)
            << std::fixed << std::setprecision(2) << per_window_throughput << " tuples/s (per window)| "
            << std::setw(20) << std::fixed << std::setprecision(2) << end_to_end_throughput << " tuples/s (end to end)"
            << std::endl;
}

// --- Main Function ---
int main(int argc, char **argv) {
  (void)argc;
  (void)argv;
  spdlog::set_level(spdlog::level::off);

  // print benchmark configuration parameters
  std::cout << "--- Benchmark Configuration ---" << std::endl;
  std::cout << "Benchmark Configuration:\n";
  std::cout << "  Number of tuples (R): " << TUPLES_R << "\n";
  std::cout << "  Number of tuples (S): " << TUPLES_S << "\n";
  std::cout << "  Window size: " << WINDOW_SIZE << "\n";
  std::cout << "  Join condition difference: " << DIFF << "\n";
  std::cout << "  Channel buffer size: " << CHANNEL_BUFFER_SIZE << "\n";
  std::cout << "  Stream type: " << typeid(StreamType).name() << "\n";
  std::cout << "  Random Stream R key range: [" << 0 << ", " << TUPLES_R << "]\n";
  std::cout << "  Random Stream S key range: [" << 0 << ", " << TUPLES_S << "]\n";
  std::cout << std::endl;

  for (const auto &num_workers : WORKERS) {
    BM_BroadcastJoiner<stream::ListIndex<KeyType, ValueType>>(num_workers);
  }

  for (const auto &num_workers : WORKERS) {
    BM_BroadcastJoiner<stream::BPlusTreeIndex<KeyType, ValueType>>(num_workers);
  }

  for (const auto &num_workers : WORKERS) {
    BM_HandshakeJoiner<stream::ListIndex<KeyType, ValueType>>(num_workers);
  }

  for (const auto &num_workers : WORKERS) {
    BM_HandshakeJoiner<stream::BPlusTreeIndex<KeyType, ValueType>>(num_workers);
  }

  decorator::printAllDurations();

  return 0;
}