cmake_minimum_required(VERSION 3.16)
project(mini_kafka LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

option(MINI_KAFKA_BUILD_TESTS "Build unit tests" ON)

add_subdirectory(src)

if(MINI_KAFKA_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
