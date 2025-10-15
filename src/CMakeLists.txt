add_library(mini_kafka
    version.cpp
    crc32.cpp
    record.cpp
    topic.cpp
    log.cpp
    protocol.cpp
    socket_util.cpp
    client.cpp
    broker.cpp
)

target_include_directories(mini_kafka
    PUBLIC ${CMAKE_SOURCE_DIR}/include
)

target_compile_options(mini_kafka PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)

add_executable(mini_kafka_broker
    broker_main.cpp
)
target_link_libraries(mini_kafka_broker PRIVATE
    mini_kafka
)

add_executable(mini_kafka_produce
    produce_main.cpp
)
target_link_libraries(mini_kafka_produce PRIVATE
    mini_kafka
)

add_executable(mini_kafka_consume
    consume_main.cpp
)
target_link_libraries(mini_kafka_consume PRIVATE
    mini_kafka
)
