#include "mini_kafka/record.h"

#include <istream>
#include <limits>
#include <ostream>
#include <stdexcept>

#include "mini_kafka/crc32.h"

namespace mini_kafka {

bool operator==(const Record& a, const Record& b) {
    return a.key == b.key && a.value == b.value;
}

bool operator!=(const Record& a, const Record& b) {
    return !(a == b);
}

namespace {

constexpr uint32_t kMinRecordLength = 12u;
constexpr uint32_t kMaxRecordLength = 16u * 1024u * 1024u;

void append_u32_le(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFFu));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFFu));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFFu));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFFu));
}

uint32_t read_u32_le(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

void validate_record_sizes(const Record& record) {
    if (record.key.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("encode_record: key too large");
    }
    if (record.value.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("encode_record: value too large");
    }

    const uint32_t key_size = static_cast<uint32_t>(record.key.size());
    const uint32_t value_size = static_cast<uint32_t>(record.value.size());
    const std::size_t payload_size = 8u + static_cast<std::size_t>(key_size) +
                                     static_cast<std::size_t>(value_size);
    if (payload_size > kMaxRecordLength - 4u) {
        throw std::runtime_error("encode_record: record too large");
    }
}

Record decode_record_payload(const uint8_t* payload, std::size_t payload_size) {
    if (payload_size < 8u) {
        throw std::runtime_error("decode_record: payload too small");
    }

    const uint32_t key_size = read_u32_le(payload);
    const uint32_t value_size = read_u32_le(payload + 4);
    if (8u + static_cast<std::size_t>(key_size) + static_cast<std::size_t>(value_size) !=
        payload_size) {
        throw std::runtime_error("decode_record: key/value sizes inconsistent with payload");
    }

    Record record;
    record.key.assign(payload + 8, payload + 8 + key_size);
    record.value.assign(payload + 8 + key_size, payload + 8 + key_size + value_size);
    return record;
}

}  // namespace

std::vector<uint8_t> encode_record(const Record& record) {
    validate_record_sizes(record);

    const uint32_t key_size = static_cast<uint32_t>(record.key.size());
    const uint32_t value_size = static_cast<uint32_t>(record.value.size());

    std::vector<uint8_t> payload;
    payload.reserve(8u + key_size + value_size);
    append_u32_le(payload, key_size);
    append_u32_le(payload, value_size);
    payload.insert(payload.end(), record.key.begin(), record.key.end());
    payload.insert(payload.end(), record.value.begin(), record.value.end());

    const uint32_t crc = crc32(payload.data(), payload.size());
    const uint32_t length = 4u + static_cast<uint32_t>(payload.size());

    std::vector<uint8_t> bytes;
    bytes.reserve(8u + payload.size());
    append_u32_le(bytes, length);
    append_u32_le(bytes, crc);
    bytes.insert(bytes.end(), payload.begin(), payload.end());
    return bytes;
}

std::size_t decode_record_at(const std::vector<uint8_t>& bytes, std::size_t offset, Record& record) {
    if (offset + 4 > bytes.size()) {
        throw std::runtime_error("decode_record: truncated length field");
    }

    const uint32_t length = read_u32_le(bytes.data() + offset);
    if (length < kMinRecordLength) {
        throw std::runtime_error("decode_record: length too small");
    }
    if (length > kMaxRecordLength) {
        throw std::runtime_error("decode_record: length too large");
    }

    const std::size_t record_size = 4u + static_cast<std::size_t>(length);
    if (offset + record_size > bytes.size()) {
        throw std::runtime_error("decode_record: truncated record");
    }

    const uint32_t expected_crc = read_u32_le(bytes.data() + offset + 4);
    const uint8_t* payload = bytes.data() + offset + 8;
    const std::size_t payload_size = static_cast<std::size_t>(length) - 4u;
    if (crc32(payload, payload_size) != expected_crc) {
        throw std::runtime_error("decode_record: crc mismatch");
    }

    record = decode_record_payload(payload, payload_size);
    return offset + record_size;
}

void write_record(std::ostream& out, const Record& record) {
    const std::vector<uint8_t> bytes = encode_record(record);
    out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    if (!out) {
        throw std::runtime_error("write_record: stream write failed");
    }
}

std::optional<Record> read_record(std::istream& in) {
    uint8_t length_bytes[4];
    in.read(reinterpret_cast<char*>(length_bytes), 4);
    const std::streamsize got = in.gcount();
    if (got == 0 && in.eof()) {
        return std::nullopt;
    }
    if (got != 4) {
        throw std::runtime_error("read_record: truncated length field");
    }

    std::vector<uint8_t> bytes(length_bytes, length_bytes + 4);
    const uint32_t length = read_u32_le(length_bytes);
    if (length < kMinRecordLength) {
        throw std::runtime_error("read_record: length too small");
    }
    if (length > kMaxRecordLength) {
        throw std::runtime_error("read_record: length too large");
    }

    bytes.resize(4u + length);
    in.read(reinterpret_cast<char*>(bytes.data() + 4), length);
    if (static_cast<std::size_t>(in.gcount()) != length) {
        throw std::runtime_error("read_record: truncated record");
    }

    Record record;
    decode_record_at(bytes, 0, record);
    return record;
}

}  // namespace mini_kafka
