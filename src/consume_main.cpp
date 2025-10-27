#include "mini_kafka/client.h"

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

std::uint32_t parse_partition(const char* text) {
    const unsigned long value = std::stoul(text);
    if (value > UINT32_MAX) {
        throw std::runtime_error("partition must fit in uint32_t");
    }
    return static_cast<std::uint32_t>(value);
}

std::string to_string(const std::vector<uint8_t>& bytes) {
    return std::string(bytes.begin(), bytes.end());
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "usage: mini_kafka_consume <host> <port> <topic> <partition>\n";
        return 1;
    }

    try {
        const std::vector<mini_kafka::Record> records = mini_kafka::consume_all(
                argv[1], parse_port(argv[2]), argv[3], parse_partition(argv[4]));
        for (const mini_kafka::Record& record : records) {
            std::cout << to_string(record.key) << "\t" << to_string(record.value) << "\n";
        }
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
}
