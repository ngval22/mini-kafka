include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(mini_kafka_tests
    version_test.cpp
    crc32_test.cpp
    record_test.cpp
    topic_test.cpp
    log_test.cpp
    protocol_test.cpp
    broker_client_test.cpp
)

target_link_libraries(mini_kafka_tests PRIVATE
    mini_kafka
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(mini_kafka_tests)
