// filepath: benchmark/joiner_benchmark.cpp
#include <benchmark/benchmark.h>
#include <cstdint>
#include <memory>
#include <ostream>
#include <sstream>  // To discard output during benchmark
#include <utility>
#include <vector>

#include "index/bplustree.hpp"
#include "index/list.hpp"
#include "join/broadcast_window.hpp"
#include "join/handshake_window.hpp"
#include "stream/random_stream.hpp"
#include "stream/sequential_stream.hpp"
#include "stream/tpc_stream.hpp"
#include "types/types.hpp"

// --- Benchmark Configuration ---
constexpr int64_t TUPLES_R = 200000;         // Number of tuples for stream R
constexpr int64_t TUPLES_S = 200000;         // Number of tuples for stream S
constexpr size_t WINDOW_SIZE = 40000;        // Window size
constexpr int64_t DIFF = 20;                 // Join condition difference |r.key - s.key| <= diff
constexpr size_t CHANNEL_BUFFER_SIZE = 128;  // Buffer size for channels

const std::vector<int64_t> WORKERS = {1, 2, 3, 4, 5, 6, 7, 8, 12, 16};  // Number of workers to test

// --- Stream Configuration ---
using KeyType = int64_t;
using ValueType = int64_t;
using StreamType = stream::SequentialStream;

// SEQUENTIAL STREAM
constexpr int64_t SEQ_START = 0;  // Start of the sequential stream
constexpr int64_t SEQ_STEP = 1;   // Step size for the sequential stream

// RANDOM STREAM
constexpr int64_t KEY_LOW = 0;       // Lower bound for key range
constexpr int64_t KEY_HIGH = 10000;  // Upper bound for key range

// TPC Stream
const std::string TPC_R_PATH = "../data/tpc-h/orders.tbl";
constexpr int r_key_column = 2;
constexpr int r_value_column = 1;
const std::string TPC_S_PATH = "../data/tpc-h/orders.tbl";
constexpr int s_key_column = 2;
constexpr int s_value_column = 1;

// --- Handshake Joiner Benchmark ---

template <typename IndexType>
static void BM_HandshakeJoiner(benchmark::State &state) {
  const size_t num_workers = state.range(0);
  const int64_t total_tuples = TUPLES_R + TUPLES_S;
  // Use a stringstream to discard output during benchmark runs
  std::ostringstream discard_stream;
  spdlog::set_level(spdlog::level::off);

  for (auto _ : state) {
    state.PauseTiming();  // Pause while setting up streams and joiner
    // Create streams for each iteration
    auto rubbish_tuples = num_workers + WINDOW_SIZE;
    auto r = std::make_unique<StreamType>(SEQ_START, TUPLES_R, SEQ_STEP);
    auto s = std::make_unique<StreamType>(SEQ_START, TUPLES_S, SEQ_STEP);
    // auto r = std::make_unique<StreamType>(TUPLES_R, std::make_pair(KEY_LOW, KEY_HIGH));
    // auto s = std::make_unique<StreamType>(TUPLES_S, std::make_pair(KEY_LOW, KEY_HIGH));

    stream::HandshakeJoiner<KeyType, ValueType, IndexType> joiner(num_workers, WINDOW_SIZE, CHANNEL_BUFFER_SIZE,
                                                                  std::move(r), std::move(s), discard_stream);
    state.ResumeTiming();  // Resume timing for the actual join operation

    joiner.Start(DIFF);

    state.PauseTiming();  // Pause again after join finishes
    // No explicit teardown needed here as unique_ptrs handle streams
    state.ResumeTiming();  // Resume briefly for loop overhead accounting
  }

  state.SetItemsProcessed(state.iterations() * total_tuples);
  state.SetComplexityN(num_workers);  // Optional: Mark complexity related to workers
  state.SetLabel(IndexType::Name + "/" + std::to_string(num_workers) + "w");
}

// --- Broadcast Joiner Benchmark ---

template <typename IndexType>
static void BM_BroadcastJoiner(benchmark::State &state) {
  const size_t num_workers = state.range(0);
  const int64_t total_tuples = TUPLES_R + TUPLES_S;
  std::ostringstream discard_stream;
  spdlog::set_level(spdlog::level::off);

  for (auto _ : state) {
    state.PauseTiming();
    auto r = std::make_unique<StreamType>(SEQ_START, TUPLES_R, SEQ_STEP);
    auto s = std::make_unique<StreamType>(SEQ_START, TUPLES_S, SEQ_STEP);
    // auto r = std::make_unique<StreamType>(TUPLES_R, std::make_pair(KEY_LOW, KEY_HIGH));
    // auto s = std::make_unique<StreamType>(TUPLES_S, std::make_pair(KEY_LOW, KEY_HIGH));

    // Note: BroadcastJoiner template takes StreamType as well
    stream::BroadcastJoiner<KeyType, ValueType, IndexType> joiner(num_workers, WINDOW_SIZE, CHANNEL_BUFFER_SIZE,
                                                                  std::move(r), std::move(s), discard_stream);
    state.ResumeTiming();

    joiner.Start(DIFF);

    state.PauseTiming();
    state.ResumeTiming();
  }

  state.SetItemsProcessed(state.iterations() * total_tuples);
  state.SetComplexityN(num_workers);
  state.SetLabel(IndexType::Name + "/" + std::to_string(num_workers) + "w");
}

// --- Register Benchmarks ---

// Function to apply worker arguments from the WORKERS vector
static void RegisterWorkerArgs(benchmark::internal::Benchmark *b) {
  b->ArgNames({"workers"});  // Set argument name once
  for (int64_t workers : WORKERS) {
    b->Arg(workers);  // Register a separate run for each worker count
  }
}

// Handshake Joiner Instances
BENCHMARK_TEMPLATE(BM_HandshakeJoiner, stream::ListIndex<KeyType, ValueType>)
    ->Apply(RegisterWorkerArgs)  // Apply the function to register args
    ->UseRealTime();
BENCHMARK_TEMPLATE(BM_HandshakeJoiner, stream::BPlusTreeIndex<KeyType, ValueType>)
    ->Apply(RegisterWorkerArgs)
    ->UseRealTime();

// Broadcast Joiner Instances
BENCHMARK_TEMPLATE(BM_BroadcastJoiner, stream::ListIndex<KeyType, ValueType>)->Apply(RegisterWorkerArgs)->UseRealTime();
BENCHMARK_TEMPLATE(BM_BroadcastJoiner, stream::BPlusTreeIndex<KeyType, ValueType>)
    ->Apply(RegisterWorkerArgs)
    ->UseRealTime();

BENCHMARK_MAIN();