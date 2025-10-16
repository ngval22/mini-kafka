#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "mini_kafka/log.h"
#include "mini_kafka/record.h"
#include "mini_kafka/topic.h"

namespace mini_kafka {

// One append-only log file per (topic, partition) under base_dir:
//   {base_dir}/{topic}-p{partition}.bin
class PartitionLogStore {
public:
    explicit PartitionLogStore(std::string base_dir);

    void add_topic(TopicMetadata topic);
    const TopicRegistry& topics() const;

    void append(const std::string& topic, std::uint32_t partition, const Record& record);
    void append_by_key(const std::string& topic, const Record& record);
    std::vector<Record> read_all(const std::string& topic, std::uint32_t partition) const;

    std::string partition_path(const std::string& topic, std::uint32_t partition) const;

private:
    const TopicMetadata& metadata_for(const std::string& topic) const;
    void validate_partition(const TopicMetadata& topic, std::uint32_t partition) const;
    Log& open_log(const std::string& topic, std::uint32_t partition);

    std::string base_dir_;
    TopicRegistry topics_;
    std::unordered_map<std::string, std::unique_ptr<Log>> open_logs_;
};

}  // namespace mini_kafka
