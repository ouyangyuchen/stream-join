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
    add_compile_options(-Wall -Wextra -Wpedantic -Werror)
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

# Generate compile_commands.json for clangd
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Add executable target
add_executable(${PROJECT_NAME}
src/main.cpp
)

# Include directories
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/libs/bwtree/src)
include_directories(${PROJECT_SOURCE_DIR}/libs/stx-btree/include)
include_directories(${PROJECT_SOURCE_DIR}/libs/cpp-channel/include)

# Link libraries (if needed)
# target_link_libraries(${PROJECT_NAME} PRIVATE some_library)

# Install target (optional)
install(TARGETS ${PROJECT_NAME} DESTINATION bin)


# Find Google Test
find_package(GTest REQUIRED)
find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)

# Add test executable
add_executable(unit_test
    test/test_main.cpp
    test/test_index.cpp
    test/test_stream.cpp
    test/test_window.cpp
)

# Include directories and link libraries
target_link_libraries(unit_test
    GTest::GTest
    GTest::Main
    Threads::Threads
    spdlog::spdlog
)
include_directories(${GTEST_INCLUDE_DIRS})

# Add test target
include(GoogleTest)
gtest_discover_tests(unit_test)

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