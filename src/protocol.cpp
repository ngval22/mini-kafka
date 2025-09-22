#include "mini_kafka/protocol.h"

#include <limits>
#include <sstream>
#include <stdexcept>

namespace mini_kafka {

namespace {

void append_u32_le(std::vector<uint8_t>& buf, uint32_t value) {
    buf.push_back(static_cast<uint8_t>(value & 0xFFu));
    buf.push_back(static_cast<uint8_t>((value >> 8) & 0xFFu));
    buf.push_back(static_cast<uint8_t>((value >> 16) & 0xFFu));
    buf.push_back(static_cast<uint8_t>((value >> 24) & 0xFFu));
}

Record decode_single_record_payload(const std::vector<uint8_t>& payload, std::size_t offset) {
    const auto begin = payload.begin() + static_cast<std::vector<uint8_t>::difference_type>(offset);
    std::string bytes(begin, payload.end());
    std::stringstream in(bytes, std::ios::in | std::ios::out | std::ios::binary);

    auto record = read_record(in);
    if (!record.has_value()) {
        throw std::runtime_error("protocol: missing record");
    }
    if (read_record(in).has_value()) {
        throw std::runtime_error("protocol: extra record data");
    }
    return *record;
}

void append_record_bytes(std::vector<uint8_t>& payload, const Record& record) {
    std::stringstream out(std::ios::in | std::ios::out | std::ios::binary);
    write_record(out, record);
    const std::string bytes = out.str();
    payload.insert(payload.end(), bytes.begin(), bytes.end());
}

uint8_t first_byte_or_throw(const std::vector<uint8_t>& payload, const char* what) {
    if (payload.empty()) {
        throw std::runtime_error(std::string("protocol: empty ") + what);
    }
    return payload[0];
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

std::vector<uint8_t> encode_produce_request(const Record& record) {
    std::vector<uint8_t> payload;
    payload.push_back(static_cast<uint8_t>(RequestType::Produce));
    append_record_bytes(payload, record);
    return payload;
}

Record decode_produce_request(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "produce request") !=
        static_cast<uint8_t>(RequestType::Produce)) {
        throw std::runtime_error("protocol: wrong request type for produce");
    }
    return decode_single_record_payload(payload, 1);
}

std::vector<uint8_t> encode_consume_request() {
    return {static_cast<uint8_t>(RequestType::Consume)};
}

void decode_consume_request(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "consume request") !=
        static_cast<uint8_t>(RequestType::Consume)) {
        throw std::runtime_error("protocol: wrong request type for consume");
    }
    if (payload.size() != 1) {
        throw std::runtime_error("protocol: consume request should not include a body");
    }
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
        append_record_bytes(payload, record);
    }
    return payload;
}

std::vector<Record> decode_consume_response(const std::vector<uint8_t>& payload) {
    if (first_byte_or_throw(payload, "consume response") !=
        static_cast<uint8_t>(ResponseType::ConsumeRecords)) {
        throw std::runtime_error("protocol: wrong response type for consume");
    }

    std::string bytes(payload.begin() + 1, payload.end());
    std::stringstream in(bytes, std::ios::in | std::ios::out | std::ios::binary);
    std::vector<Record> records;
    while (auto record = read_record(in)) {
        records.push_back(*record);
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
