#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mini_kafka/record.h"

namespace mini_kafka {

enum class RequestType : uint8_t {
    Produce = 1,
    Consume = 2,
    ReplicaFetch = 3,
    JoinGroup = 4,
    LeaveGroup = 5,
    OffsetCommit = 6,
    GroupConsume = 7,
};

enum class ResponseType : uint8_t {
    ProduceOk = 1,
    ConsumeRecords = 2,
    Error = 3,
    JoinGroupOk = 4,
    LeaveGroupOk = 5,
    OffsetCommitOk = 6,
};

struct ProduceRequest {
    std::string topic;
    Record record;
};

struct ConsumeRequest {
    std::string topic;
    std::uint32_t partition;
};

struct ReplicaFetchRequest {
    std::string topic;
    std::uint32_t partition;
    std::uint64_t from_offset;
};

struct JoinGroupRequest {
    std::string group_id;
    std::string member_id;
    std::string topic;
};

struct JoinGroupResponse {
    std::string member_id;
    std::vector<std::uint32_t> partitions;
};

struct LeaveGroupRequest {
    std::string group_id;
    std::string member_id;
};

struct OffsetCommitRequest {
    std::string group_id;
    std::string topic;
    std::uint32_t partition;
    std::uint64_t offset;
};

struct GroupConsumeRequest {
    std::string group_id;
    std::string topic;
    std::uint32_t partition;
};

std::vector<uint8_t> encode_frame(const std::vector<uint8_t>& payload);

// Produce payload: [type][u32 topic_len][topic bytes][record bytes]
// Partition is chosen by the broker from the record key.
std::vector<uint8_t> encode_produce_request(const std::string& topic, const Record& record);
ProduceRequest decode_produce_request(const std::vector<uint8_t>& payload);

// Consume payload: [type][u32 topic_len][topic bytes][u32 partition]
std::vector<uint8_t> encode_consume_request(const std::string& topic, std::uint32_t partition);
ConsumeRequest decode_consume_request(const std::vector<uint8_t>& payload);

// Replica fetch payload: [type][u32 topic_len][topic bytes][u32 partition][u64 from_offset]
// Response reuses consume records encoding.
std::vector<uint8_t> encode_replica_fetch_request(const std::string& topic,
                                                    std::uint32_t partition,
                                                    std::uint64_t from_offset);
ReplicaFetchRequest decode_replica_fetch_request(const std::vector<uint8_t>& payload);

// Join group: [type][group_id][member_id][topic]. Empty member_id means broker assigns one.
// Response: [type][member_id][u32 partition_count][u32 partition]...
std::vector<uint8_t> encode_join_group_request(const JoinGroupRequest& request);
JoinGroupRequest decode_join_group_request(const std::vector<uint8_t>& payload);
std::vector<uint8_t> encode_join_group_response(const JoinGroupResponse& response);
JoinGroupResponse decode_join_group_response(const std::vector<uint8_t>& payload);

// Leave group: [type][group_id][member_id]
std::vector<uint8_t> encode_leave_group_request(const LeaveGroupRequest& request);
LeaveGroupRequest decode_leave_group_request(const std::vector<uint8_t>& payload);
std::vector<uint8_t> encode_leave_group_ok_response();
void decode_leave_group_ok_response(const std::vector<uint8_t>& payload);

// Offset commit: [type][group_id][topic][u32 partition][u64 offset]
std::vector<uint8_t> encode_offset_commit_request(const OffsetCommitRequest& request);
OffsetCommitRequest decode_offset_commit_request(const std::vector<uint8_t>& payload);
std::vector<uint8_t> encode_offset_commit_ok_response();
void decode_offset_commit_ok_response(const std::vector<uint8_t>& payload);

// Group consume: [type][group_id][topic][u32 partition]. Response reuses consume records.
std::vector<uint8_t> encode_group_consume_request(const GroupConsumeRequest& request);
GroupConsumeRequest decode_group_consume_request(const std::vector<uint8_t>& payload);

std::vector<uint8_t> encode_produce_ok_response();
void decode_produce_ok_response(const std::vector<uint8_t>& payload);

std::vector<uint8_t> encode_consume_response(const std::vector<Record>& records);
std::vector<Record> decode_consume_response(const std::vector<uint8_t>& payload);

std::vector<uint8_t> encode_error_response(const std::string& message);
std::string decode_error_response(const std::vector<uint8_t>& payload);

}  // namespace mini_kafka
