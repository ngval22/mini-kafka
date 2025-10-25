#include "mini_kafka/broker.h"

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

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "usage: mini_kafka_broker <data_dir> <port>\n";
        return 1;
    }

    try {
        mini_kafka::Broker broker(argv[1], parse_port(argv[2]));
        std::cout << "listening on port " << broker.port() << "\n";
        broker.serve_forever();
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
}
