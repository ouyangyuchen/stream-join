# Minimum CMake version required
cmake_minimum_required(VERSION 3.14)

# Project name and version
project(main VERSION 1.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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

# Optimization levels
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Add OpenMP flag
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")

# Dependencies
find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)

# Add FetchContent for Google Test
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        release-1.12.1
)
FetchContent_MakeAvailable(googletest)

# Add FetchContent for Google Benchmark
FetchContent_Declare(
    googletest_benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.8.3
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest_benchmark)

# Include directories
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/libs/bwtree/src)
include_directories(${PROJECT_SOURCE_DIR}/libs/stx-btree/include)
include_directories(${PROJECT_SOURCE_DIR}/libs/cpp-channel/include)
include_directories(${PROJECT_SOURCE_DIR}/libs/pgm-index/include)
include_directories(${PROJECT_SOURCE_DIR}/libs/alex-map/src/core)

# compiler flags
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    add_compile_options(-Wall -Wextra -march=native)
endif()

# Main executable
add_executable(${PROJECT_NAME}
    src/main.cpp
    src/decorator.cpp
)
target_link_libraries(${PROJECT_NAME} PRIVATE spdlog::spdlog)

# Unit tests
add_executable(unit_test
    test/test_main.cpp
    test/test_index.cpp
    test/test_stream.cpp
    test/test_join.cpp
    src/decorator.cpp
)
target_link_libraries(unit_test
    PRIVATE
    GTest::gtest
    GTest::gtest_main
    Threads::Threads
    spdlog::spdlog
)

# Install target
install(TARGETS ${PROJECT_NAME} DESTINATION bin)

# Tools: clang-format
find_program(CLANG_FORMAT "clang-format")
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

# Tools: clang-tidy
find_program(CLANG_TIDY "clang-tidy")
if (CLANG_TIDY)
    set(CMAKE_CXX_CLANG_TIDY ${CLANG_TIDY} -extra-arg=-Wno-unknown-warning-option)
else()
    message(WARNING "clang-tidy not found. Static analysis will not be available.")
endif()