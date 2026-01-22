#include "mini_kafka/protocol.h"

#include <limits>
#include <stdexcept>

namespace mini_kafka {

namespace {

void append_u32_le(std::vector<uint8_t>& buf, uint32_t value) {
    buf.push_back(static_cast<uint8_t>(value & 0xFFu));
    buf.push_back(static_cast<uint8_t>((value >> 8) & 0xFFu));
    buf.push_back(static_cast<uint8_t>((value >> 16) & 0xFFu));
    buf.push_back(static_cast<uint8_t>((value >> 24) & 0xFFu));
}

uint32_t read_u32_le(const std::vector<uint8_t>& payload, std::size_t offset) {
    if (offset + 4 > payload.size()) {
        throw std::runtime_error("protocol: truncated u32");
    }
    const uint8_t* p = payload.data() + offset;
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

void append_u64_le(std::vector<uint8_t>& buf, uint64_t value) {
    append_u32_le(buf, static_cast<uint32_t>(value & 0xFFFFFFFFu));
    append_u32_le(buf, static_cast<uint32_t>((value >> 32) & 0xFFFFFFFFu));
}

uint64_t read_u64_le(const std::vector<uint8_t>& payload, std::size_t offset) {
    const uint64_t low = read_u32_le(payload, offset);
    const uint64_t high = read_u32_le(payload, offset + 4);
    return low | (high << 32);
}

uint8_t first_byte_or_throw(const std::vector<uint8_t>& payload, const char* what) {
    if (payload.empty()) {
        throw std::runtime_error(std::string("protocol: empty ") + what);
    }
    return payload[0];
}

void append_string(std::vector<uint8_t>& buf, const std::string& text) {
    if (text.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("protocol: string too large");
    }
    append_u32_le(buf, static_cast<uint32_t>(text.size()));
    buf.insert(buf.end(), text.begin(), text.end());
}

std::size_t decode_string_at(const std::vector<uint8_t>& payload, std::size_t offset,
                               std::string& text) {
    const uint32_t length = read_u32_le(payload, offset);
    offset += 4;
    if (offset + length > payload.size()) {
        throw std::runtime_error("protocol: truncated string");
    }
    text.assign(payload.begin() + static_cast<std::ptrdiff_t>(offset),
                payload.begin() + static_cast<std::ptrdiff_t>(offset + length));
    return offset + length;
}

Record decode_single_record_payload(const std::vector<uint8_t>& payload, std::size_t offset) {
    Record record;
    const std::size_t next_offset = decode_record_at(payload, offset, record);
    if (next_offset != payload.size()) {
        throw std::runtime_error("protocol: extra record data");
    }
    return record;
}

void append_partitions(std::vector<uint8_t>& buf, const std::vector<std::uint32_t>& partitions) {
    if (partitions.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("protocol: partition list too large");
    }
    append_u32_le(buf, static_cast<uint32_t>(partitions.size()));
    for (const std::uint32_t partition : partitions) {
        append_u32_le(buf, partition);
    }
}

std::size_t decode_partitions_at(const std::vector<uint8_t>& payload, std::size_t offset,
                                 std::vector<std::uint32_t>& partitions) {
    const uint32_t count = read_u32_le(payload, offset);
    offset += 4;
    partitions.clear();
    partitions.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        if (offset + 4 > payload.size()) {
            throw std::runtime_error("protocol: truncated partition list");
        }
        partitions.push_back(read_u32_le(payload, offset));
        offset += 4;
    }
    return offset;
}

void decode_empty_ok_response(const std::vector<uint8_t>& payload, ResponseType expected,
                              const char* what) {
    if (first_byte_or_throw(payload, what) != static_cast<uint8_t>(expected)) {
        throw std::runtime_error(std::string("protocol: wrong response type for ") + what);
    }
    if (payload.size() != 1) {
        throw std::runtime_error(std::string("protocol: ") + what + " should not include a body");
    }
}

}  // namespace

std::vector<uint8_t> encode_frame(const std::vector<uint8_t>& payload) {
    if (payload.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("protocol: frame too large");
    }

    std::vector<uint8_t> frame;
    frame.reserve(4 + payload.size());
    append_u32_le(frame, static_cast<uint32_t>(payload.size()));
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

std::vector<uint8_t> encode_produce_request(const std::string& topic, const Record& record) {
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(RequestType::Produce));
    append_string(payload, topic);
    const std::vector<uint8_t> record_bytes = encode_record(record);
    payload.insert(payload.end(), record_bytes.begin(), record_bytes.end());
    return payload;
}

ProduceRequest decode_produce_request(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "produce request") !=
        static_cast<uint8_t>(RequestType::Produce)) {
        throw std::runtime_error("protocol: wrong request type for produce");
    }

    ProduceRequest request;
    const std::size_t record_offset = decode_string_at(payload, 1, request.topic);
    request.record = decode_single_record_payload(payload, record_offset);
    return request;
}

std::vector<uint8_t> encode_consume_request(const std::string& topic, std::uint32_t partition) {
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(RequestType::Consume));
    append_string(payload, topic);
    append_u32_le(payload, partition);
    return payload;
}

ConsumeRequest decode_consume_request(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "consume request") !=
        static_cast<uint8_t>(RequestType::Consume)) {
        throw std::runtime_error("protocol: wrong request type for consume");
    }

    ConsumeRequest request;
    std::size_t offset = decode_string_at(payload, 1, request.topic);
    if (offset + 4 != payload.size()) {
        throw std::runtime_error("protocol: consume request has unexpected trailing data");
    }
    request.partition = read_u32_le(payload, offset);
    return request;
}

std::vector<uint8_t> encode_replica_fetch_request(const std::string& topic,
                                                    const std::uint32_t partition,
                                                    const std::uint64_t from_offset) {
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(RequestType::ReplicaFetch));
    append_string(payload, topic);
    append_u32_le(payload, partition);
    append_u64_le(payload, from_offset);
    return payload;
}

ReplicaFetchRequest decode_replica_fetch_request(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "replica fetch request") !=
        static_cast<uint8_t>(RequestType::ReplicaFetch)) {
        throw std::runtime_error("protocol: wrong request type for replica fetch");
    }

    ReplicaFetchRequest request;
    std::size_t offset = decode_string_at(payload, 1, request.topic);
    if (offset + 12 > payload.size()) {
        throw std::runtime_error("protocol: replica fetch request truncated");
    }
    request.partition = read_u32_le(payload, offset);
    offset += 4;
    request.from_offset = read_u64_le(payload, offset);
    if (offset + 8 != payload.size()) {
        throw std::runtime_error("protocol: replica fetch request has unexpected trailing data");
    }
    return request;
}

std::vector<uint8_t> encode_join_group_request(const JoinGroupRequest& request) {
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(RequestType::JoinGroup));
    append_string(payload, request.group_id);
    append_string(payload, request.member_id);
    append_string(payload, request.topic);
    return payload;
}

JoinGroupRequest decode_join_group_request(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "join group request") !=
        static_cast<uint8_t>(RequestType::JoinGroup)) {
        throw std::runtime_error("protocol: wrong request type for join group");
    }

    JoinGroupRequest request;
    std::size_t offset = decode_string_at(payload, 1, request.group_id);
    offset = decode_string_at(payload, offset, request.member_id);
    offset = decode_string_at(payload, offset, request.topic);
    if (offset != payload.size()) {
        throw std::runtime_error("protocol: join group request has unexpected trailing data");
    }
    return request;
}

std::vector<uint8_t> encode_join_group_response(const JoinGroupResponse& response) {
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(ResponseType::JoinGroupOk));
    append_string(payload, response.member_id);
    append_partitions(payload, response.partitions);
    return payload;
}

JoinGroupResponse decode_join_group_response(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "join group response") !=
        static_cast<uint8_t>(ResponseType::JoinGroupOk)) {
        throw std::runtime_error("protocol: wrong response type for join group");
    }

    JoinGroupResponse response;
    std::size_t offset = decode_string_at(payload, 1, response.member_id);
    offset = decode_partitions_at(payload, offset, response.partitions);
    if (offset != payload.size()) {
        throw std::runtime_error("protocol: join group response has unexpected trailing data");
    }
    return response;
}

std::vector<uint8_t> encode_leave_group_request(const LeaveGroupRequest& request) {
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(RequestType::LeaveGroup));
    append_string(payload, request.group_id);
    append_string(payload, request.member_id);
    return payload;
}

LeaveGroupRequest decode_leave_group_request(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "leave group request") !=
        static_cast<uint8_t>(RequestType::LeaveGroup)) {
        throw std::runtime_error("protocol: wrong request type for leave group");
    }

    LeaveGroupRequest request;
    std::size_t offset = decode_string_at(payload, 1, request.group_id);
    offset = decode_string_at(payload, offset, request.member_id);
    if (offset != payload.size()) {
        throw std::runtime_error("protocol: leave group request has unexpected trailing data");
    }
    return request;
}

std::vector<uint8_t> encode_leave_group_ok_response() {
    return {static_cast<uint8_t>(ResponseType::LeaveGroupOk)};
}

void decode_leave_group_ok_response(const std::vector<uint8_t>& payload) {
    decode_empty_ok_response(payload, ResponseType::LeaveGroupOk, "leave group");
}

std::vector<uint8_t> encode_offset_commit_request(const OffsetCommitRequest& request) {
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(RequestType::OffsetCommit));
    append_string(payload, request.group_id);
    append_string(payload, request.topic);
    append_u32_le(payload, request.partition);
    append_u64_le(payload, request.offset);
    return payload;
}

OffsetCommitRequest decode_offset_commit_request(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "offset commit request") !=
        static_cast<uint8_t>(RequestType::OffsetCommit)) {
        throw std::runtime_error("protocol: wrong request type for offset commit");
    }

    OffsetCommitRequest request;
    std::size_t offset = decode_string_at(payload, 1, request.group_id);
    offset = decode_string_at(payload, offset, request.topic);
    if (offset + 12 > payload.size()) {
        throw std::runtime_error("protocol: offset commit request truncated");
    }
    request.partition = read_u32_le(payload, offset);
    offset += 4;
    request.offset = read_u64_le(payload, offset);
    if (offset + 8 != payload.size()) {
        throw std::runtime_error("protocol: offset commit request has unexpected trailing data");
    }
    return request;
}

std::vector<uint8_t> encode_offset_commit_ok_response() {
    return {static_cast<uint8_t>(ResponseType::OffsetCommitOk)};
}

void decode_offset_commit_ok_response(const std::vector<uint8_t>& payload) {
    decode_empty_ok_response(payload, ResponseType::OffsetCommitOk, "offset commit");
}

std::vector<uint8_t> encode_group_consume_request(const GroupConsumeRequest& request) {
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(RequestType::GroupConsume));
    append_string(payload, request.group_id);
    append_string(payload, request.topic);
    append_u32_le(payload, request.partition);
    return payload;
}

GroupConsumeRequest decode_group_consume_request(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "group consume request") !=
        static_cast<uint8_t>(RequestType::GroupConsume)) {
        throw std::runtime_error("protocol: wrong request type for group consume");
    }

    GroupConsumeRequest request;
    std::size_t offset = decode_string_at(payload, 1, request.group_id);
    offset = decode_string_at(payload, offset, request.topic);
    if (offset + 4 != payload.size()) {
        throw std::runtime_error("protocol: group consume request has unexpected trailing data");
    }
    request.partition = read_u32_le(payload, offset);
    return request;
}

std::vector<uint8_t> encode_produce_ok_response() {
    return {static_cast<uint8_t>(ResponseType::ProduceOk)};
}

void decode_produce_ok_response(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "produce response") !=
        static_cast<uint8_t>(ResponseType::ProduceOk)) {
        throw std::runtime_error("protocol: wrong response type for produce");
    }
    if (payload.size() != 1) {
        throw std::runtime_error("protocol: produce response should not include a body");
    }
}

std::vector<uint8_t> encode_consume_response(const std::vector<Record>& records) {
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(ResponseType::ConsumeRecords));
    for (const Record& record : records) {
        const std::vector<uint8_t> record_bytes = encode_record(record);
        payload.insert(payload.end(), record_bytes.begin(), record_bytes.end());
    }
    return payload;
}

std::vector<Record> decode_consume_response(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "consume response") !=
        static_cast<uint8_t>(ResponseType::ConsumeRecords)) {
        throw std::runtime_error("protocol: wrong response type for consume");
    }

    std::vector<Record> records;
    std::size_t offset = 1;
    while (offset < payload.size()) {
        Record record;
        offset = decode_record_at(payload, offset, record);
        records.push_back(std::move(record));
    }
    return records;
}

std::vector<uint8_t> encode_error_response(const std::string& message) {
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(ResponseType::Error));
    payload.insert(payload.end(), message.begin(), message.end());
    return payload;
}

std::string decode_error_response(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "error response") != static_cast<uint8_t>(ResponseType::Error)) {
        throw std::runtime_error("protocol: wrong response type for error");
    }
    return std::string(payload.begin() + 1, payload.end());
}

}  // namespace mini_kafka
