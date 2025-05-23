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

struct Config {
  // --- Join Configuration ---
  const std::vector<int64_t> workers = {4, 8, 12, 16, 20, 24, 28, 32, 36};  // Number of workers to test
  size_t window_size = 1'000000;
  int64_t diff = 3000;
  int64_t tuples_r = window_size * 3;
  int64_t tuples_s = window_size * 3;
  size_t channel_buffer_size = 256;

  size_t iterations = 1;  // Number of iterations for each benchmark

  bool preload = true;           // Preload tuples into the index in the constructor of joiner
  bool watcher_enabled = false;  // Enable or disable watcher for handshake joiner
  std::chrono::milliseconds watcher_interval = std::chrono::milliseconds(10000);

} config;

// --- Stream Configuration ---
using KeyType = int64_t;
using ValueType = int64_t;
using StreamType = stream::RandomStream;

void printHelp() {
  std::cout << "Usage: joiner_benchmark [options]\n";
  std::cout << "Options:\n";
  std::cout << "  --iterations <count>           Set the number of iterations for each benchmark (default: "
            << config.iterations << ")\n";
  std::cout << "  --window_size <size>         Set the window size (default: " << config.window_size << ")\n";
  std::cout << "  --diff <difference>          Set the join condition difference (default: " << config.diff << ")\n";
  std::cout << "  --tuples_r <count>           Set the number of tuples in stream R (default: " << config.tuples_r
            << ")\n";
  std::cout << "  --tuples_s <count>           Set the number of tuples in stream S (default: " << config.tuples_s
            << ")\n";
  std::cout << "  --channel_buffer_size <size> Set the channel buffer size (default: " << config.channel_buffer_size
            << ")\n";
  std::cout << "  --preload <1|0> .            Preload tuples into the index (default: 1)\n";
  std::cout << "  --watcher_enabled <1|0>      Enable watcher (default: 0)\n";
  std::cout << "  --watcher_interval <ms>      Watcher interval in milliseconds (default: 10000)\n";
  std::cout << "  --help                       Display this help message\n";
}

void parseArguments(int argc, char **argv) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      printHelp();
      std::exit(EXIT_SUCCESS);
    } else if (arg == "--iterations" && i + 1 < argc) {
      config.iterations = std::stoul(argv[++i]);
    } else if (arg == "--window_size" && i + 1 < argc) {
      config.window_size = std::stoul(argv[++i]);
    } else if (arg == "--diff" && i + 1 < argc) {
      config.diff = std::stoll(argv[++i]);
    } else if (arg == "--tuples_r" && i + 1 < argc) {
      config.tuples_r = std::stoll(argv[++i]);
    } else if (arg == "--tuples_s" && i + 1 < argc) {
      config.tuples_s = std::stoll(argv[++i]);
    } else if (arg == "--channel_buffer_size" && i + 1 < argc) {
      config.channel_buffer_size = std::stoul(argv[++i]);
    } else if (arg == "--preload" && i + 1 < argc) {
      config.preload = std::stoi(argv[++i]) != 0;
    } else if (arg == "--watcher_enabled" && i + 1 < argc) {
      config.watcher_enabled = std::stoi(argv[++i]) != 0;
    } else if (arg == "--watcher_interval" && i + 1 < argc) {
      config.watcher_interval = std::chrono::milliseconds(std::stoul(argv[++i]));
    } else {
      std::cerr << "Unknown or incomplete argument: " << arg << std::endl;
      std::exit(EXIT_FAILURE);
    }
  }
}

// --- Handshake Joiner Benchmark ---

template <typename IndexType>
static void BM_HandshakeJoiner(size_t num_workers, size_t iteration = 1) {
  double avg_end_to_end_throughput = 0;
  double avg_duration_ms = 0;
  std::string label = "HandshakeJoiner " + IndexType::Name + "/" + std::to_string(num_workers) + "w";

  for (size_t i = 0; i < iteration; ++i) {
    int64_t total_tuples = config.tuples_r + config.tuples_s;
    stream::time_record_t timing_info;  // neglect preloading time and tailpopping time

    // Create streams for each iteration
    auto r = std::make_unique<StreamType>(config.tuples_r);
    auto s = std::make_unique<StreamType>(config.tuples_s);

    stream::HandshakeJoiner<KeyType, ValueType, IndexType> joiner(
        num_workers, config.window_size, config.channel_buffer_size, std::move(r), std::move(s), &timing_info);
    if (config.preload) {
      joiner.Preload();
      total_tuples -= config.window_size + config.window_size;
    }
    if (config.watcher_enabled) {
      joiner.StartWatcher(config.watcher_interval);
    }

    joiner.Start(config.diff);

    double throughput = timing_info.GetThroughput();
    auto duration = timing_info.GetDuration().count();  // duration in microseconds
    avg_end_to_end_throughput += throughput;
    avg_duration_ms += double(duration) / 1e3;  // convert to milliseconds
  }

  avg_end_to_end_throughput /= iteration;
  avg_duration_ms /= iteration;
  std::cout << std::left << std::setw(30) << label << ": " << std::setw(12) << avg_duration_ms << " ms | "
            << std::setw(20) << std::fixed << std::setprecision(2) << avg_end_to_end_throughput
            << " tuples/s (end to end)" << std::endl;
}

// --- Broadcast Joiner Benchmark ---

template <typename IndexType>
static void BM_BroadcastJoiner(size_t num_workers, size_t iteration = 1) {
  double avg_per_window_throughput = 0;
  double avg_end_to_end_throughput = 0;
  double avg_duration_ms = 0;
  std::string label = "BroadcastJoiner " + IndexType::Name + "/" + std::to_string(num_workers) + "w";

  for (size_t i = 0; i < iteration; ++i) {
    int64_t per_window_total_tuples = config.tuples_r + config.tuples_s / num_workers;
    size_t end_to_end_total_tuples = config.tuples_r + config.tuples_s;

    auto r = std::make_unique<StreamType>(config.tuples_r);
    auto s = std::make_unique<StreamType>(config.tuples_s);

    stream::BroadcastJoiner<KeyType, ValueType, IndexType> joiner(
        num_workers, config.window_size, config.channel_buffer_size, std::move(r), std::move(s));
    if (config.preload) {
      joiner.Preload();
      per_window_total_tuples -= config.window_size + config.window_size / num_workers;
      end_to_end_total_tuples -= config.window_size + config.window_size;
    }
    if (config.watcher_enabled) {
      joiner.StartWatcher(config.watcher_interval);
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    joiner.Start(config.diff);
    auto end_time = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    double per_window_throughput =
        static_cast<double>(per_window_total_tuples) / (duration / 1e6);  // tuples per second
    double end_to_end_throughput =
        static_cast<double>(end_to_end_total_tuples) / (duration / 1e6);  // tuples per second
    avg_per_window_throughput += per_window_throughput;
    avg_end_to_end_throughput += end_to_end_throughput;
    avg_duration_ms += double(duration) / 1e3;  // convert to milliseconds
  }

  avg_per_window_throughput /= iteration;
  avg_end_to_end_throughput /= iteration;
  avg_duration_ms /= iteration;
  std::cout << std::left << std::setw(30) << label << ": " << std::setw(12) << avg_duration_ms << " ms | "
            << std::setw(20) << std::fixed << std::setprecision(2) << avg_per_window_throughput
            << " tuples/s (per window)| " << std::setw(20) << std::fixed << std::setprecision(2)
            << avg_end_to_end_throughput << " tuples/s (end to end)" << std::endl;
}

// --- Main Function ---
int main(int argc, char **argv) {
  parseArguments(argc, argv);
  spdlog::set_level(spdlog::level::off);

  // print benchmark configuration parameters
  std::cout << "--- Benchmark Configuration ---" << std::endl;
  std::cout << "Benchmark Configuration:\n";
  std::cout << "  Number of tuples (R): " << config.tuples_r << "\n";
  std::cout << "  Number of tuples (S): " << config.tuples_s << "\n";
  std::cout << "  Window size: " << config.window_size << "\n";
  std::cout << "  Join condition difference: " << config.diff << "\n";
  std::cout << "  Channel buffer size: " << config.channel_buffer_size << "\n";
  std::cout << "  Stream type: " << typeid(StreamType).name() << "\n";
  std::cout << "  Random Stream R key range: [" << 0 << ", " << config.tuples_r << "]\n";
  std::cout << "  Random Stream S key range: [" << 0 << ", " << config.tuples_s << "]\n";
  std::cout << "  Preload tuples: " << (config.preload ? "true" : "false") << "\n";
  std::cout << "  Watcher enabled: " << (config.watcher_enabled ? "true" : "false") << "\n";
  std::cout << "  Watcher interval: " << config.watcher_interval.count() << " ms\n";
  std::cout << "  Iterations per test: " << config.iterations << "\n";
  std::cout << std::endl;

  for (const auto &num_workers : config.workers) {
    BM_BroadcastJoiner<stream::ListIndex<KeyType, ValueType>>(num_workers, config.iterations);
  }

  for (const auto &num_workers : config.workers) {
    BM_BroadcastJoiner<stream::BPlusTreeIndex<KeyType, ValueType>>(num_workers, config.iterations);
  }

  for (const auto &num_workers : config.workers) {
    BM_HandshakeJoiner<stream::ListIndex<KeyType, ValueType>>(num_workers, config.iterations);
  }

  for (const auto &num_workers : config.workers) {
    BM_HandshakeJoiner<stream::BPlusTreeIndex<KeyType, ValueType>>(num_workers, config.iterations);
  }

  decorator::printAllDurations();

  return 0;
}