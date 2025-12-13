#include "mini_kafka/partition_store.h"
#include "mini_kafka/segmented_log.h"

#include <cstdint>
#include <exception>
#include <iostream>
#include <string>

namespace {

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
    if (argc != 4) {
        std::cerr << "usage: mini_kafka_log_dump <data_dir> <topic> <partition>\n";
        return 1;
    }

    try {
        const std::string data_dir = argv[1];
        const std::string topic = argv[2];
        const std::uint32_t partition = parse_partition(argv[3]);

        mini_kafka::PartitionLogStore store(data_dir);
        const std::string dir = store.partition_dir(topic, partition);
        const mini_kafka::SegmentedLog log(dir);
        const std::vector<mini_kafka::Record> records = log.read_all();

        for (const mini_kafka::Record& record : records) {
            std::cout << to_string(record.key) << "\t" << to_string(record.value) << "\n";
        }
        std::cerr << records.size() << " record(s) from " << dir << "\n";
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
}
