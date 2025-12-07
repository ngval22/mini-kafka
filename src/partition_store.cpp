#include "mini_kafka/partition_store.h"

#include <filesystem>
#include <stdexcept>
#include <utility>

namespace mini_kafka {

namespace fs = std::filesystem;

PartitionLogStore::PartitionLogStore(std::string base_dir) : base_dir_(std::move(base_dir)) {
    if (base_dir_.empty()) {
        throw std::runtime_error("partition_store: base_dir must not be empty");
    }
    std::error_code ec;
    fs::create_directories(base_dir_, ec);
    if (ec) {
        throw std::runtime_error("partition_store: failed to create base_dir: " + base_dir_);
    }
}

void PartitionLogStore::add_topic(TopicMetadata topic) {
    topics_.add(std::move(topic));
}

const TopicRegistry& PartitionLogStore::topics() const {
    return topics_;
}

std::string PartitionLogStore::partition_dir(const std::string& topic,
                                             std::uint32_t partition) const {
    return base_dir_ + "/" + topic + "-p" + std::to_string(partition);
}

const TopicMetadata& PartitionLogStore::metadata_for(const std::string& topic) const {
    const TopicMetadata* meta = topics_.find(topic);
    if (meta == nullptr) {
        throw std::runtime_error("partition_store: unknown topic: " + topic);
    }
    return *meta;
}

void PartitionLogStore::validate_partition(const TopicMetadata& topic,
                                           std::uint32_t partition) const {
    if (partition >= topic.partition_count) {
        throw std::runtime_error("partition_store: partition out of range for topic: " +
                                 topic.name);
    }
}

PartitionLogStore::PartitionEntry& PartitionLogStore::entry_for(const std::string& dir) {
    std::lock_guard<std::mutex> map_lock(partitions_map_mutex_);
    const auto inserted = partitions_.try_emplace(dir);
    return inserted.first->second;
}

SegmentedLog& PartitionLogStore::open_log(PartitionEntry& entry, const std::string& dir) {
    if (entry.log == nullptr) {
        entry.log = std::make_unique<SegmentedLog>(dir);
    }
    return *entry.log;
}

void PartitionLogStore::append(const std::string& topic, std::uint32_t partition,
                               const Record& record) {
    const TopicMetadata& meta = metadata_for(topic);
    validate_partition(meta, partition);

    const std::string dir = partition_dir(topic, partition);
    PartitionEntry& entry = entry_for(dir);
    std::lock_guard<std::mutex> partition_lock(entry.mu);
    open_log(entry, dir).append(record);
}

void PartitionLogStore::append_by_key(const std::string& topic, const Record& record) {
    const TopicMetadata& meta = metadata_for(topic);
    const std::uint32_t partition = partition_for_key(record.key, meta.partition_count);
    append(topic, partition, record);
}

std::vector<Record> PartitionLogStore::read_all(const std::string& topic,
                                                std::uint32_t partition) {
    const TopicMetadata& meta = metadata_for(topic);
    validate_partition(meta, partition);

    const std::string dir = partition_dir(topic, partition);
    PartitionEntry& entry = entry_for(dir);
    std::lock_guard<std::mutex> partition_lock(entry.mu);
    return open_log(entry, dir).read_all();
}

}  // namespace mini_kafka
