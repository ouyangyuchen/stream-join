# Minimum CMake version required
cmake_minimum_required(VERSION 3.14)

# Project name and version
project(main VERSION 1.0 LANGUAGES CXX)

# Set C++ standard (e.g., C++17, C++20)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable warnings and treat them as errors
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    add_compile_options(-Wall -Wextra -Werror)
endif()

# Option to enable ASan (default: OFF)
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
if(ENABLE_ASAN)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        message(STATUS "AddressSanitizer enabled")
        add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
        add_link_options(-fsanitize=address)
    else()
        message(WARNING "AddressSanitizer not supported for this compiler")
    endif()
endif()

# optimize level
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Add OpenMP flag (for pgm index)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")

# Add executable target
add_executable(${PROJECT_NAME}
src/main.cpp
src/decorator.cpp
)

# Include directories
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/libs/bwtree/src)
include_directories(${PROJECT_SOURCE_DIR}/libs/stx-btree/include)
include_directories(${PROJECT_SOURCE_DIR}/libs/cpp-channel/include)
include_directories(${PROJECT_SOURCE_DIR}/libs/pgm-index/include)

# Link libraries (if needed)
target_link_libraries(${PROJECT_NAME} PRIVATE spdlog::spdlog)

# Install target (optional)
install(TARGETS ${PROJECT_NAME} DESTINATION bin)

# Add FetchContent for Google Test
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        release-1.12.1 # Or another stable tag like release-1.10.0, release-1.11.0
)
# Note: For GTest 1.10.0+ you might need to set(GTEST_HAS_PTHREAD ON) if not detected
FetchContent_MakeAvailable(googletest)

find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)

# Add test executable
add_executable(unit_test
    test/test_main.cpp
    test/test_index.cpp
    test/test_stream.cpp
    test/test_window.cpp
    src/decorator.cpp
)

# Include directories and link libraries
target_link_libraries(unit_test
    PRIVATE
    GTest::gtest
    GTest::gtest_main
    Threads::Threads
    spdlog::spdlog
)

# benchmark
add_executable(benchmark_test
    benchmark/joiner_benchmark.cpp
    src/decorator.cpp
)

target_link_libraries(benchmark_test PRIVATE
    benchmark::benchmark
    benchmark::benchmark_main
    spdlog::spdlog
    Threads::Threads
)

set_target_properties(benchmark_test PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON) # Or your desired standard
if(CMAKE_BUILD_TYPE STREQUAL "" OR CMAKE_BUILD_TYPE STREQUAL "None")
  message(STATUS "No build type specified, defaulting to Release for benchmarks.")
  set_property(TARGET benchmark_test PROPERTY CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
  set_property(TARGET benchmark_test PROPERTY CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -DNDEBUG")
endif()

# Add test target
include(GoogleTest)
gtest_discover_tests(unit_test)

# install google benchmark
include(FetchContent)
FetchContent_Declare(
  googletest_benchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG        v1.8.3 # Use a specific tag or commit hash
)

# Disable benchmark's own tests which require Google Test
# since we are already finding/managing Google Test separately.
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest_benchmark)

# Find clang-format and clang-tidy
find_program(CLANG_FORMAT "clang-format")
find_program(CLANG_TIDY "clang-tidy")

# Add a custom target for clang-format
if (CLANG_FORMAT)
    file(GLOB_RECURSE ALL_SOURCE_FILES
        src/*.cpp
        include/*.h
        include/*.hpp
    )

    add_custom_target(
        format
        COMMAND ${CLANG_FORMAT}
        -i
        ${ALL_SOURCE_FILES}
        COMMENT "Running clang-format on all source files"
    )
else()
    message(WARNING "clang-format not found. Formatting target will not be available.")
endif()

# Add clang-tidy integration
if (CLANG_TIDY)
    set(CMAKE_CXX_CLANG_TIDY ${CLANG_TIDY} -extra-arg=-Wno-unknown-warning-option)
else()
    message(WARNING "clang-tidy not found. Static analysis will not be available.")
endif()