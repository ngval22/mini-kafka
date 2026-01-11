#include "mini_kafka/consumer_group.h"

#include <algorithm>
#include <stdexcept>

namespace mini_kafka {

namespace {

void validate_group_id(const std::string& group_id) {
    if (group_id.empty()) {
        throw std::runtime_error("consumer group: group_id must not be empty");
    }
}

void validate_topic(const std::string& topic) {
    if (topic.empty()) {
        throw std::runtime_error("consumer group: topic must not be empty");
    }
}

}  // namespace

PartitionAssignment assign_partitions_round_robin(const std::vector<std::string>& members,
                                                    const std::uint32_t partition_count) {
    if (partition_count == 0) {
        throw std::runtime_error("consumer group: partition_count must be greater than zero");
    }

    std::vector<std::string> sorted_members = members;
    std::sort(sorted_members.begin(), sorted_members.end());

    PartitionAssignment assignment;
    for (const std::string& member : sorted_members) {
        assignment.emplace(member, std::vector<std::uint32_t>{});
    }
    if (sorted_members.empty()) {
        return assignment;
    }

    const std::size_t member_count = sorted_members.size();
    for (std::uint32_t partition = 0; partition < partition_count; ++partition) {
        const std::string& member = sorted_members[partition % member_count];
        assignment[member].push_back(partition);
    }
    return assignment;
}

std::string ConsumerGroupRegistry::join(const std::string& group_id,
                                        const std::string& member_id) {
    validate_group_id(group_id);

    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::string>& members = groups_[group_id];

    std::string assigned = member_id;
    if (assigned.empty()) {
        assigned = "member-" + std::to_string(next_member_seq_++);
    }

    if (std::find(members.begin(), members.end(), assigned) != members.end()) {
        return assigned;
    }
    members.push_back(assigned);
    return assigned;
}

void ConsumerGroupRegistry::leave(const std::string& group_id, const std::string& member_id) {
    if (group_id.empty() || member_id.empty()) {
        return;
    }

    std::lock_guard<std::mutex> lock(mu_);
    const auto group_it = groups_.find(group_id);
    if (group_it == groups_.end()) {
        return;
    }

    std::vector<std::string>& members = group_it->second;
    members.erase(std::remove(members.begin(), members.end(), member_id), members.end());
    if (members.empty()) {
        groups_.erase(group_it);
    }
}

std::vector<std::string> ConsumerGroupRegistry::members(const std::string& group_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const auto group_it = groups_.find(group_id);
    if (group_it == groups_.end()) {
        return {};
    }
    std::vector<std::string> result = group_it->second;
    std::sort(result.begin(), result.end());
    return result;
}

void CommittedOffsetStore::commit(const std::string& group_id, const std::string& topic,
                                  const std::uint32_t partition, const std::uint64_t offset) {
    validate_group_id(group_id);
    validate_topic(topic);

    std::lock_guard<std::mutex> lock(mu_);
    offsets_[group_id][topic][partition] = offset;
}

std::uint64_t CommittedOffsetStore::committed_offset(const std::string& group_id,
                                                     const std::string& topic,
                                                     const std::uint32_t partition) const {
    if (group_id.empty() || topic.empty()) {
        return 0;
    }

    std::lock_guard<std::mutex> lock(mu_);
    const auto group_it = offsets_.find(group_id);
    if (group_it == offsets_.end()) {
        return 0;
    }
    const auto topic_it = group_it->second.find(topic);
    if (topic_it == group_it->second.end()) {
        return 0;
    }
    const auto partition_it = topic_it->second.find(partition);
    if (partition_it == topic_it->second.end()) {
        return 0;
    }
    return partition_it->second;
}

}  // namespace mini_kafka
