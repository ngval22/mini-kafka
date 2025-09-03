add_library(mini_kafka
    version.cpp
)

target_include_directories(mini_kafka
    PUBLIC ${CMAKE_SOURCE_DIR}/include
)

target_compile_options(mini_kafka PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)
