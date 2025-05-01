#include "utils/decorator.hpp"
#include <chrono>  // Include for duration_cast and milliseconds
#include <iostream>
#include <mutex>          // Include for std::lock_guard
#include <unordered_map>  // Include for std::unordered_map

namespace decorator::detail {

auto getDurationMap() -> std::unordered_map<std::string, Duration> & {
  static std::unordered_map<std::string, Duration> durationMap;
  return durationMap;
}

auto getDurationMutex() -> std::mutex & {
  // Use function-local static for the mutex as well
  static std::mutex durationMutex;
  return durationMutex;
}

}  // namespace decorator::detail

namespace decorator {

#ifdef DEBUGGING
auto getTotalDuration(const std::string &key) -> Duration {
  // Use detail namespace to access internal helpers
  std::lock_guard<std::mutex> lock(detail::getDurationMutex());
  const auto &durationMap = detail::getDurationMap();  // Use const reference
  auto it = durationMap.find(key);
  if (it != durationMap.end()) {
    return it->second;
  }
  return Duration::zero();
}

void printAllDurations() {
  // Use detail namespace to access internal helpers
  std::lock_guard<std::mutex> lock(detail::getDurationMutex());
  const auto &durationMap = detail::getDurationMap();  // Use const reference
  std::cout << "--- Recorded Durations ---" << std::endl;
  if (durationMap.empty()) {
    std::cout << "(No durations recorded yet)" << std::endl;
  } else {
    for (const auto &pair : durationMap) {
      // Convert duration to a more readable unit, e.g., milliseconds
      auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(pair.second);
      std::cout << pair.first << ": " << ms.count() << " ms" << std::endl;
    }
  }
  std::cout << "--------------------------" << std::endl;
}

#else
// Provide non-debugging stubs for the public API functions
auto getTotalDuration(const std::string &key) -> Duration {
  // Return zero duration if debugging is disabled
  (void)key;  // Mark key as unused
  return Duration::zero();
}

void printAllDurations() {
  // Print a message indicating debugging is disabled
  std::cout << "--- Durations Not Recorded (DEBUGGING is off) ---" << std::endl;
}
#endif  // DEBUGGING

}  // namespace decorator
