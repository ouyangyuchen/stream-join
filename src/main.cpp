#include "index/bplustree.hpp"
#include "index/list.hpp"
#include "join/broadcast_join.hpp"
#include "join/handshake_join.hpp"
#include "stream/random_stream.hpp"
#include "stream/sequential_stream.hpp"
#include "types/types.hpp"
#include "utils/decorator.hpp"  // For printAllDurations

#include <spdlog/spdlog.h>  // For spdlog::set_level
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

struct Config {
  using KeyType = int64_t;
  using ValueType = int64_t;

  size_t window_size = 50000;
  int64_t diff = 2000;
  int64_t tuples_r = 200000;
  int64_t tuples_s = 200000;
  size_t channel_buffer_size = 128;
  size_t workers = 4;                     // Default to 4 workers
  std::string joiner_type = "handshake";  // "handshake" or "broadcast"
  std::string index_type = "bplustree";   // "list" or "bplustree"
  std::string stream_type = "random";     // "random" or "sequential"

  bool watcher_enabled = false;  // Enable or disable watcher for handshake joiner

  // Sequential stream params
  KeyType seq_start = 0;
  KeyType seq_step = 1;
};

void print_help(const char *prog_name) {
  std::cerr << "Usage: " << prog_name << " [options]\n\n"
            << "Options:\n"
            << "  --help                       Show this help message and exit\n"
            << "  --window_size <val>          Window size (default: 50000)\n"
            << "  --diff <val>                 Join condition difference |r.key - s.key| <= diff (default: 2000)\n"
            << "  --tuples_r <val>             Number of tuples for stream R (default: 200000)\n"
            << "  --tuples_s <val>             Number of tuples for stream S (default: 200000)\n"
            << "  --channel_buffer_size <val>  Buffer size for channels (default: 128)\n"
            << "  --workers <val>              Number of worker threads (default: 4)\n"
            << "  --joiner_type <type>         Joiner type: 'handshake' or 'broadcast' (default: handshake)\n"
            << "  --index_type <type>          Index type: 'list' or 'bplustree' (default: bplustree)\n"
            << "  --stream_type <type>         Stream type: 'random' or 'sequential' (default: random)\n"
            << "  --watcher_enabled <val>       Enable watcher (default: false)\n"
            << "  --seq_start <val>            For sequential stream: start key (default: 0)\n"
            << "  --seq_step <val>             For sequential stream: step size (default: 1)\n"
            << std::endl;
}

bool parse_arguments(int argc, char *argv[], Config &config) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    try {
      if (arg == "--help") {
        print_help(argv[0]);
        return false;  // Indicate to exit
      } else if (arg == "--window_size") {
        if (++i < argc)
          config.window_size = std::stoul(argv[i]);
        else
          throw std::runtime_error("Missing value for --window_size");
      } else if (arg == "--diff") {
        if (++i < argc)
          config.diff = std::stoll(argv[i]);
        else
          throw std::runtime_error("Missing value for --diff");
      } else if (arg == "--tuples_r") {
        if (++i < argc)
          config.tuples_r = std::stoll(argv[i]);
        else
          throw std::runtime_error("Missing value for --tuples_r");
      } else if (arg == "--tuples_s") {
        if (++i < argc)
          config.tuples_s = std::stoll(argv[i]);
        else
          throw std::runtime_error("Missing value for --tuples_s");
      } else if (arg == "--channel_buffer_size") {
        if (++i < argc)
          config.channel_buffer_size = std::stoul(argv[i]);
        else
          throw std::runtime_error("Missing value for --channel_buffer_size");
      } else if (arg == "--workers") {
        if (++i < argc)
          config.workers = std::stoul(argv[i]);
        else
          throw std::runtime_error("Missing value for --workers");
      } else if (arg == "--joiner_type") {
        if (++i < argc)
          config.joiner_type = argv[i];
        else
          throw std::runtime_error("Missing value for --joiner_type");
      } else if (arg == "--index_type") {
        if (++i < argc)
          config.index_type = argv[i];
        else
          throw std::runtime_error("Missing value for --index_type");
      } else if (arg == "--stream_type") {
        if (++i < argc)
          config.stream_type = argv[i];
        else
          throw std::runtime_error("Missing value for --stream_type");
      } else if (arg == "--watcher_enabled") {
        if (++i < argc)
          config.watcher_enabled = std::stoul(argv[i]);
        else
          throw std::runtime_error("Missing value for --watcher_enabled");
      } else if (arg == "--seq_start") {
        if (++i < argc)
          config.seq_start = std::stoll(argv[i]);
        else
          throw std::runtime_error("Missing value for --seq_start");
      } else if (arg == "--seq_step") {
        if (++i < argc)
          config.seq_step = std::stoll(argv[i]);
        else
          throw std::runtime_error("Missing value for --seq_step");
      } else {
        throw std::runtime_error("Unknown option: " + arg);
      }
    } catch (const std::exception &e) {
      std::cerr << "Argument parsing error: " << e.what() << std::endl;
      print_help(argv[0]);
      return false;
    }
  }

  // Validate choices
  if (config.joiner_type != "handshake" && config.joiner_type != "broadcast") {
    std::cerr << "Invalid joiner_type: " << config.joiner_type << std::endl;
    return false;
  }
  if (config.index_type != "list" && config.index_type != "bplustree") {
    std::cerr << "Invalid index_type: " << config.index_type << std::endl;
    return false;
  }
  if (config.stream_type != "random" && config.stream_type != "sequential") {
    std::cerr << "Invalid stream_type: " << config.stream_type << std::endl;
    return false;
  }
  if (config.workers == 0) {
    std::cerr << "Number of workers must be greater than 0." << std::endl;
    return false;
  }

  return true;
}

int main(int argc, char *argv[]) {
  Config config;
  if (!parse_arguments(argc, argv, config)) {
    return (argc == 2 && std::string(argv[1]) == "--help") ? 0 : 1;
  }

  // Suppress spdlog output, similar to benchmark
  spdlog::set_level(spdlog::level::info);

  std::cout << "--- Configuration ---\n"
            << "Joiner Type: " << config.joiner_type << "\n"
            << "Index Type: " << config.index_type << "\n"
            << "Stream Type: " << config.stream_type << "\n"
            << "Workers: " << config.workers << "\n"
            << "Window Size: " << config.window_size << "\n"
            << "Diff: " << config.diff << "\n"
            << "Tuples R: " << config.tuples_r << ", Tuples S: " << config.tuples_s << "\n"
            << "Channel Buffer Size: " << config.channel_buffer_size << "\n";
  if (config.stream_type == "random") {
    std::cout << "Key Range: R = [" << 0 << ", " << config.tuples_r << "), S = [" << 0 << ", " << config.tuples_s
              << ")\n";
  } else {
    std::cout << "Seq Start: " << config.seq_start << ", Seq Step: " << config.seq_step << "\n";
  }
  std::cout << "---------------------\n" << std::endl;

  std::unique_ptr<stream::Stream<Config::KeyType, Config::ValueType>> stream_r;
  std::unique_ptr<stream::Stream<Config::KeyType, Config::ValueType>> stream_s;

  if (config.stream_type == "random") {
    stream_r = std::make_unique<stream::RandomStream>(config.tuples_r);
    stream_s = std::make_unique<stream::RandomStream>(config.tuples_s);
  } else {  // sequential
    stream_r = std::make_unique<stream::SequentialStream>(config.seq_start, config.tuples_r, config.seq_step);
    stream_s = std::make_unique<stream::SequentialStream>(config.seq_start, config.tuples_s, config.seq_step);
  }

  std::cout << "Initializing joiner..." << std::endl;
  auto start_time = std::chrono::high_resolution_clock::now();

  if (config.joiner_type == "handshake") {
    if (config.index_type == "list") {
      stream::HandshakeJoiner<Config::KeyType, Config::ValueType, stream::ListIndex<Config::KeyType, Config::ValueType>>
          joiner(config.workers, config.window_size, config.channel_buffer_size, std::move(stream_r),
                 std::move(stream_s));
      std::cout << "Starting HandshakeJoiner (ListIndex)..." << std::endl;
      if (config.watcher_enabled) {
        joiner.StartWatcher();
      }
      joiner.Start(config.diff);
    } else {  // bplustree
      stream::HandshakeJoiner<Config::KeyType, Config::ValueType,
                              stream::BPlusTreeIndex<Config::KeyType, Config::ValueType>>
          joiner(config.workers, config.window_size, config.channel_buffer_size, std::move(stream_r),
                 std::move(stream_s));
      std::cout << "Starting HandshakeJoiner (BPlusTreeIndex)..." << std::endl;
      if (config.watcher_enabled) {
        joiner.StartWatcher();
      }
      joiner.Start(config.diff);
    }
  } else {  // broadcast
    if (config.index_type == "list") {
      stream::BroadcastJoiner<Config::KeyType, Config::ValueType, stream::ListIndex<Config::KeyType, Config::ValueType>>
          joiner(config.workers, config.window_size, config.channel_buffer_size, std::move(stream_r),
                 std::move(stream_s));
      std::cout << "Starting BroadcastJoiner (ListIndex)..." << std::endl;
      joiner.Start(config.diff);
    } else {  // bplustree
      stream::BroadcastJoiner<Config::KeyType, Config::ValueType,
                              stream::BPlusTreeIndex<Config::KeyType, Config::ValueType>>
          joiner(config.workers, config.window_size, config.channel_buffer_size, std::move(stream_r),
                 std::move(stream_s));
      std::cout << "Starting BroadcastJoiner (BPlusTreeIndex)..." << std::endl;
      joiner.Start(config.diff);
    }
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  std::cout << "\nJoin operation completed." << std::endl;
  std::cout << "Total execution time: " << duration_ms << " ms" << std::endl;

  // decorator::printAllDurations(); // If you want to print detailed internal durations

  return 0;
}