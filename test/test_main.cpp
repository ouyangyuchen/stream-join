#include <gtest/gtest.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

auto main(int argc, char **argv) -> int {
  // Set up spdlog to use color output
  auto console_logger = spdlog::stdout_color_mt("console");
  console_logger->set_level(spdlog::level::debug);  // Set the log level to debug

  spdlog::info("Starting tests...");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}