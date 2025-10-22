#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mini_kafka/record.h"

namespace mini_kafka {

enum class RequestType : uint8_t {
    Produce = 1,
    Consume = 2,
};

enum class ResponseType : uint8_t {
    ProduceOk = 1,
    ConsumeRecords = 2,
    Error = 3,
};

struct ProduceRequest {
    std::string topic;
    Record record;
};

struct ConsumeRequest {
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

std::vector<uint8_t> encode_produce_ok_response();
void decode_produce_ok_response(const std::vector<uint8_t>& payload);

std::vector<uint8_t> encode_consume_response(const std::vector<Record>& records);
std::vector<Record> decode_consume_response(const std::vector<uint8_t>& payload);

std::vector<uint8_t> encode_error_response(const std::string& message);
std::string decode_error_response(const std::vector<uint8_t>& payload);

}  // namespace mini_kafka
