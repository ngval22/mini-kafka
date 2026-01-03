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

void print_usage() {
    std::cerr << "usage: mini_kafka_broker <data_dir> <port> "
                 "[--follower <leader_host> <leader_port> | --promote] [<topic> <partitions>]\n";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        print_usage();
        return 1;
    }

    try {
        mini_kafka::BrokerOptions options;
        options.data_dir = argv[1];
        options.port = parse_port(argv[2]);

        int topic_index = 3;
        if (argc > 3) {
            const std::string flag = argv[3];
            if (flag == "--follower") {
                if (argc != 6 && argc != 8) {
                    print_usage();
                    return 1;
                }
                options.role = mini_kafka::BrokerRole::Follower;
                options.leader_host = argv[4];
                options.leader_port = parse_port(argv[5]);
                topic_index = 6;
            } else if (flag == "--promote") {
                options.promoted = true;
                topic_index = 4;
            }
        }

        const bool has_topic = (argc == topic_index + 2);
        if (argc != topic_index && !has_topic) {
            print_usage();
            return 1;
        }

        mini_kafka::Broker broker(std::move(options));
        if (has_topic) {
            broker.add_topic(
                    mini_kafka::make_topic_metadata(argv[topic_index],
                                                    parse_partition_count(argv[topic_index + 1])));
        }
        std::cout << "listening on port " << broker.port() << "\n";
        broker.serve_forever();
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
}
