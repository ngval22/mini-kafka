#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mini_kafka {

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

}  // namespace mini_kafka
