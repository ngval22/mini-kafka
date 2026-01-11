#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mini_kafka {

using PartitionAssignment = std::unordered_map<std::string, std::vector<std::uint32_t>>;

// Round-robin: partition p goes to member (p % member_count). Members are sorted first.
// Every member appears in the result (possibly with an empty partition list).
PartitionAssignment assign_partitions_round_robin(const std::vector<std::string>& members,
                                                    std::uint32_t partition_count);

// In-memory consumer group membership (join / leave). Thread-safe.
class ConsumerGroupRegistry {
public:
    // Adds member_id to group_id. If member_id is empty, assigns a unique id and returns it.
    // Re-joining the same member_id is idempotent.
    std::string join(const std::string& group_id, const std::string& member_id);

    // Removes a member. No-op if the group or member is unknown.
    void leave(const std::string& group_id, const std::string& member_id);

    // Sorted member ids for the group (empty if unknown).
    std::vector<std::string> members(const std::string& group_id) const;

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, std::vector<std::string>> groups_;
    std::uint64_t next_member_seq_ = 0;
};

// In-memory committed offsets per (group, topic, partition). Thread-safe.
// Offset is the next record index to read (0 = start of log).
class CommittedOffsetStore {
public:
    void commit(const std::string& group_id, const std::string& topic, std::uint32_t partition,
                std::uint64_t offset);

    // Returns 0 when nothing has been committed for this key.
    std::uint64_t committed_offset(const std::string& group_id, const std::string& topic,
                                   std::uint32_t partition) const;

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string,
                       std::unordered_map<std::string, std::unordered_map<std::uint32_t, std::uint64_t>>>
            offsets_;
};

}  // namespace mini_kafka
