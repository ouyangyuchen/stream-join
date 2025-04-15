#include <gtest/gtest.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

auto main(int argc, char **argv) -> int {
  // Set up spdlog to use color output
  spdlog::set_level(spdlog::level::info);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}