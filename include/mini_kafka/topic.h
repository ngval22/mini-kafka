#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace mini_kafka {

struct TopicMetadata {
    std::string name;
    std::uint32_t partition_count;
};

// Throws std::runtime_error if name is empty or partition_count is zero.
TopicMetadata make_topic_metadata(std::string name, std::uint32_t partition_count);

// Picks a partition in [0, partition_count) from the record key.
// Empty key always maps to partition 0. Same key always maps to the same partition.
std::uint32_t partition_for_key(const std::vector<uint8_t>& key, std::uint32_t partition_count);

class TopicRegistry {
public:
    void add(TopicMetadata topic);
    const TopicMetadata* find(const std::string& name) const;
    std::vector<TopicMetadata> topics() const;

private:
    std::vector<TopicMetadata> topics_;
};

}  // namespace mini_kafka
