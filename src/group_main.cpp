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

std::uint64_t parse_offset(const char* text) {
    const unsigned long long value = std::stoull(text);
    return value;
}

std::string to_string(const std::vector<uint8_t>& bytes) {
    return std::string(bytes.begin(), bytes.end());
}

std::string member_id_arg(const char* text) {
    if (std::string(text) == "-") {
        return "";
    }
    return text;
}

void print_usage() {
    std::cerr << "usage:\n"
              << "  mini_kafka_group join <host> <port> <group> <member|-> <topic>\n"
              << "  mini_kafka_group consume <host> <port> <group> <topic> <partition>\n"
              << "  mini_kafka_group commit <host> <port> <group> <topic> <partition> <offset>\n"
              << "  mini_kafka_group leave <host> <port> <group> <member>\n";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

    const std::string command = argv[1];

    try {
        if (command == "join" && argc == 7) {
            const mini_kafka::JoinGroupResponse response = mini_kafka::join_group(
                    argv[2], parse_port(argv[3]), argv[4], member_id_arg(argv[5]), argv[6]);
            std::cout << response.member_id << "\n";
            for (std::uint32_t partition : response.partitions) {
                std::cout << partition << "\n";
            }
            return 0;
        }
        if (command == "consume" && argc == 7) {
            const std::vector<mini_kafka::Record> records = mini_kafka::group_consume(
                    argv[2], parse_port(argv[3]), argv[4], argv[5], parse_partition(argv[6]));
            for (const mini_kafka::Record& record : records) {
                std::cout << to_string(record.key) << "\t" << to_string(record.value) << "\n";
            }
            return 0;
        }
        if (command == "commit" && argc == 8) {
            mini_kafka::commit_offset(argv[2], parse_port(argv[3]), argv[4], argv[5],
                                      parse_partition(argv[6]), parse_offset(argv[7]));
            return 0;
        }
        if (command == "leave" && argc == 6) {
            mini_kafka::leave_group(argv[2], parse_port(argv[3]), argv[4], argv[5]);
            return 0;
        }
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }

    print_usage();
    return 1;
}
