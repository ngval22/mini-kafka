#include "mini_kafka/topic.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace mini_kafka {

namespace {

void validate_topic_name(const std::string& name) {
    if (name.find('/') != std::string::npos) {
        throw std::runtime_error("topic: name must not contain '/'");
    }
    if (name.find('\\') != std::string::npos) {
        throw std::runtime_error("topic: name must not contain '\\'");
    }
    if (name == "." || name == "..") {
        throw std::runtime_error("topic: invalid topic name");
    }
}

std::uint32_t fnv1a32(const std::vector<uint8_t>& bytes) {
    std::uint32_t hash = 2166136261u;
    for (const uint8_t byte : bytes) {
        hash ^= byte;
        hash *= 16777619u;
    }
    return hash;
}

}  // namespace

TopicMetadata make_topic_metadata(std::string name, std::uint32_t partition_count) {
    if (name.empty()) {
        throw std::runtime_error("topic: name must not be empty");
    }
    validate_topic_name(name);
    if (partition_count == 0) {
        throw std::runtime_error("topic: partition_count must be at least 1");
    }
    return TopicMetadata{std::move(name), partition_count};
}

std::uint32_t partition_for_key(const std::vector<uint8_t>& key, std::uint32_t partition_count) {
    if (partition_count == 0) {
        throw std::runtime_error("topic: partition_count must be at least 1");
    }
    if (key.empty()) {
        return 0;
    }
    return fnv1a32(key) % partition_count;
}

void TopicRegistry::add(TopicMetadata topic) {
    make_topic_metadata(topic.name, topic.partition_count);
    if (find(topic.name) != nullptr) {
        throw std::runtime_error("topic: duplicate topic: " + topic.name);
    }
    topics_.push_back(std::move(topic));
}

const TopicMetadata* TopicRegistry::find(const std::string& name) const {
    for (const TopicMetadata& topic : topics_) {
        if (topic.name == name) {
            return &topic;
        }
    }
    return nullptr;
}

std::vector<TopicMetadata> TopicRegistry::topics() const {
    std::vector<TopicMetadata> copy = topics_;
    std::sort(copy.begin(), copy.end(),
              [](const TopicMetadata& a, const TopicMetadata& b) { return a.name < b.name; });
    return copy;
}

}  // namespace mini_kafka
