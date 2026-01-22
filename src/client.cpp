#include "mini_kafka/client.h"

#include <stdexcept>

#include "mini_kafka/protocol.h"
#include "socket_util.h"

namespace mini_kafka {

namespace {

void throw_if_error_response(const std::vector<uint8_t>& payload) {
    if (!payload.empty() &&
        payload[0] == static_cast<uint8_t>(ResponseType::Error)) {
        throw std::runtime_error(decode_error_response(payload));
    }
}

}  // namespace

void produce(const std::string& host, uint16_t port, const std::string& topic,
             const Record& record) {
    SocketHandle socket(connect_to_server(host, port));

    write_frame(socket.get(), encode_produce_request(topic, record));
    const std::vector<uint8_t> response = read_frame(socket.get());
    throw_if_error_response(response);
    decode_produce_ok_response(response);
}

std::vector<Record> consume_all(const std::string& host, uint16_t port, const std::string& topic,
                                std::uint32_t partition) {
    SocketHandle socket(connect_to_server(host, port));

    write_frame(socket.get(), encode_consume_request(topic, partition));
    const std::vector<uint8_t> response = read_frame(socket.get());
    throw_if_error_response(response);
    return decode_consume_response(response);
}

std::vector<Record> replica_fetch(const std::string& host, const uint16_t port,
                                  const std::string& topic, const std::uint32_t partition,
                                  const std::uint64_t from_offset) {
    SocketHandle socket(connect_to_server(host, port));

    write_frame(socket.get(), encode_replica_fetch_request(topic, partition, from_offset));
    const std::vector<uint8_t> response = read_frame(socket.get());
    throw_if_error_response(response);
    return decode_consume_response(response);
}

JoinGroupResponse join_group(const std::string& host, const uint16_t port,
                             const std::string& group_id, const std::string& member_id,
                             const std::string& topic) {
    SocketHandle socket(connect_to_server(host, port));

    JoinGroupRequest request;
    request.group_id = group_id;
    request.member_id = member_id;
    request.topic = topic;
    write_frame(socket.get(), encode_join_group_request(request));
    const std::vector<uint8_t> response = read_frame(socket.get());
    throw_if_error_response(response);
    return decode_join_group_response(response);
}

void leave_group(const std::string& host, const uint16_t port, const std::string& group_id,
                 const std::string& member_id) {
    SocketHandle socket(connect_to_server(host, port));

    LeaveGroupRequest request;
    request.group_id = group_id;
    request.member_id = member_id;
    write_frame(socket.get(), encode_leave_group_request(request));
    const std::vector<uint8_t> response = read_frame(socket.get());
    throw_if_error_response(response);
    decode_leave_group_ok_response(response);
}

void commit_offset(const std::string& host, const uint16_t port, const std::string& group_id,
                   const std::string& topic, const std::uint32_t partition,
                   const std::uint64_t offset) {
    SocketHandle socket(connect_to_server(host, port));

    OffsetCommitRequest request;
    request.group_id = group_id;
    request.topic = topic;
    request.partition = partition;
    request.offset = offset;
    write_frame(socket.get(), encode_offset_commit_request(request));
    const std::vector<uint8_t> response = read_frame(socket.get());
    throw_if_error_response(response);
    decode_offset_commit_ok_response(response);
}

std::vector<Record> group_consume(const std::string& host, const uint16_t port,
                                  const std::string& group_id, const std::string& topic,
                                  const std::uint32_t partition) {
    SocketHandle socket(connect_to_server(host, port));

    GroupConsumeRequest request;
    request.group_id = group_id;
    request.topic = topic;
    request.partition = partition;
    write_frame(socket.get(), encode_group_consume_request(request));
    const std::vector<uint8_t> response = read_frame(socket.get());
    throw_if_error_response(response);
    return decode_consume_response(response);
}

}  // namespace mini_kafka
