#ifndef DECORATOR_HPP
#define DECORATOR_HPP

#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <type_traits>  // Include for std::is_void_v, std::invoke_result_t
#include <unordered_map>
#include <utility>

#define DEBUGGING

using Duration = std::chrono::high_resolution_clock::duration;

namespace decorator {

namespace detail {
auto getDurationMap() -> std::unordered_map<std::string, Duration> &;
auto getDurationMutex() -> std::mutex &;
}  // namespace detail

/**
 * @brief Decorator function that adds timing capabilities to a given function.
 *
 * Wraps the input function `func` to measure its execution time. The total
 * execution time for all calls associated with the given `key` is accumulated
 * in a heap-allocated map. Accesses internal map/mutex via detail namespace.
 *
 * @tparam Func Type of the callable object (function pointer, lambda, functor).
 * @param func The callable object to wrap.
 * @param key A string key used to identify and accumulate the duration for this function.
 * @return A new callable object (lambda) that wraps the original function with timing logic.
 */
template <typename Func>
auto decorateWithTimer(Func &&func, const std::string &key) {
#ifdef DEBUGGING
  return [f = std::forward<Func>(func), key](auto &&...args) -> decltype(auto) {
    auto start = std::chrono::high_resolution_clock::now();
    // Use if constexpr to handle void return types
    if constexpr (std::is_void_v<std::invoke_result_t<Func, decltype(args)...>>) {
      std::invoke(f, std::forward<decltype(args)>(args)...);
      auto end = std::chrono::high_resolution_clock::now();
      auto duration = end - start;
      {
        // Use detail namespace to access internal helpers
        std::lock_guard<std::mutex> lock(detail::getDurationMutex());
        auto &durationMap = detail::getDurationMap();
        durationMap[key] += duration;
      }
      // Explicitly return void if the original function returns void
      return;
    } else {
      decltype(auto) result = std::invoke(f, std::forward<decltype(args)>(args)...);
      auto end = std::chrono::high_resolution_clock::now();
      auto duration = end - start;
      {
        // Use detail namespace to access internal helpers
        std::lock_guard<std::mutex> lock(detail::getDurationMutex());
        auto &durationMap = detail::getDurationMap();
        durationMap[key] += duration;
      }
      return result;
    }
  };
#else
  (void)key;
  return std::forward<Func>(func);
#endif
}

/**
 * @brief Retrieves the total accumulated duration for a specific key.
 *
 * @param key The key whose total duration is requested.
 * @return The total duration accumulated for the key, or zero if the key is not found.
 */
Duration getTotalDuration(const std::string &key);

/**
 * @brief Prints all recorded keys and their total accumulated durations.
 *        Durations are printed in milliseconds for readability.
 */
void printAllDurations();

}  // namespace decorator

#endif  // DECORATOR_HPP