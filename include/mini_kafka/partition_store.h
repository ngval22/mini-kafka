#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "mini_kafka/record.h"
#include "mini_kafka/segmented_log.h"
#include "mini_kafka/topic.h"

namespace mini_kafka {

// One segmented log directory per (topic, partition) under base_dir:
//   {base_dir}/{topic}-p{partition}/
class PartitionLogStore {
public:
    explicit PartitionLogStore(std::string base_dir);

    void add_topic(TopicMetadata topic);
    const TopicRegistry& topics() const;

    void append(const std::string& topic, std::uint32_t partition, const Record& record);
    void append_by_key(const std::string& topic, const Record& record);
    std::vector<Record> read_all(const std::string& topic, std::uint32_t partition);
    std::vector<Record> read_from(const std::string& topic, std::uint32_t partition,
                                  std::uint64_t from_offset);

    std::string partition_dir(const std::string& topic, std::uint32_t partition) const;

private:
    struct PartitionEntry {
        std::mutex mu;
        std::unique_ptr<SegmentedLog> log;
    };

    const TopicMetadata& metadata_for(const std::string& topic) const;
    void validate_partition(const TopicMetadata& topic, std::uint32_t partition) const;
    PartitionEntry& entry_for(const std::string& dir);
    SegmentedLog& open_log(PartitionEntry& entry, const std::string& dir);

    std::string base_dir_;
    TopicRegistry topics_;
    std::mutex partitions_map_mutex_;
    std::unordered_map<std::string, PartitionEntry> partitions_;
};

}  // namespace mini_kafka
