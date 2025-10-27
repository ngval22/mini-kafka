#include "mini_kafka/broker.h"
#include "mini_kafka/topic.h"

#include <cstdint>
#include <exception>
#include <iostream>
#include <string>

namespace {

uint16_t parse_port(const char* text) {
    const int value = std::stoi(text);
    if (value < 0 || value > 65535) {
        throw std::runtime_error("port must be between 0 and 65535");
    }
    return static_cast<uint16_t>(value);
}

std::uint32_t parse_partition_count(const char* text) {
    const unsigned long value = std::stoul(text);
    if (value == 0 || value > UINT32_MAX) {
        throw std::runtime_error("partition count must be between 1 and 2^32-1");
    }
    return static_cast<std::uint32_t>(value);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3 && argc != 5) {
        std::cerr << "usage: mini_kafka_broker <data_dir> <port> [<topic> <partitions>]\n";
        return 1;
    }

    try {
        mini_kafka::Broker broker(argv[1], parse_port(argv[2]));
        if (argc == 5) {
            broker.add_topic(
                    mini_kafka::make_topic_metadata(argv[3], parse_partition_count(argv[4])));
        }
        std::cout << "listening on port " << broker.port() << "\n";
        broker.serve_forever();
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
}
