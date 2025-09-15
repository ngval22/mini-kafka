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

}  // namespace

void write_record(std::ostream& out, const Record& record) {
    if (record.key.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("write_record: key too large");
    }
    if (record.value.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("write_record: value too large");
    }

    const uint32_t key_size = static_cast<uint32_t>(record.key.size());
    const uint32_t value_size = static_cast<uint32_t>(record.value.size());
    const std::size_t payload_size = 8u + static_cast<std::size_t>(key_size) +
                                     static_cast<std::size_t>(value_size);
    if (payload_size > kMaxRecordLength - 4u) {
        throw std::runtime_error("write_record: record too large");
    }

    // build the CRC-covered payload in memory so we can checksum it before writing.
    std::vector<uint8_t> payload;
    payload.reserve(payload_size);
    append_u32_le(payload, key_size);
    append_u32_le(payload, value_size);
    payload.insert(payload.end(), record.key.begin(), record.key.end());
    payload.insert(payload.end(), record.value.begin(), record.value.end());

    const uint32_t crc = crc32(payload.data(), payload.size());
    const uint32_t length = 4u + static_cast<uint32_t>(payload.size());  // crc + payload

    std::vector<uint8_t> header;
    header.reserve(8);
    append_u32_le(header, length);
    append_u32_le(header, crc);

    out.write(reinterpret_cast<const char*>(header.data()), header.size());
    out.write(reinterpret_cast<const char*>(payload.data()), payload.size());
    if (!out) {
        throw std::runtime_error("write_record: stream write failed");
    }
}

std::optional<Record> read_record(std::istream& in) {
    uint8_t length_bytes[4];
    in.read(reinterpret_cast<char*>(length_bytes), 4);
    std::streamsize got = in.gcount();
    if (got == 0 && in.eof()) {
        return std::nullopt;  // clean EOF: no more records
    }
    if (got != 4) {
        throw std::runtime_error("read_record: truncated length field");
    }
    const uint32_t length = read_u32_le(length_bytes);
    if (length < kMinRecordLength) {
        // must contain at least: crc(4) + key_size(4) + value_size(4)
        throw std::runtime_error("read_record: length too small");
    }
    if (length > kMaxRecordLength) {
        throw std::runtime_error("read_record: length too large");
    }

    uint8_t crc_bytes[4];
    in.read(reinterpret_cast<char*>(crc_bytes), 4);
    if (in.gcount() != 4) {
        throw std::runtime_error("read_record: truncated crc field");
    }
    const uint32_t expected_crc = read_u32_le(crc_bytes);

    std::vector<uint8_t> payload(length - 4u);
    if (!payload.empty()) {
        in.read(reinterpret_cast<char*>(payload.data()), payload.size());
        if (static_cast<std::size_t>(in.gcount()) != payload.size()) {
            throw std::runtime_error("read_record: truncated payload");
        }
    }

    if (crc32(payload.data(), payload.size()) != expected_crc) {
        throw std::runtime_error("read_record: crc mismatch");
    }

    const uint32_t key_size = read_u32_le(payload.data());
    const uint32_t value_size = read_u32_le(payload.data() + 4);
    if (8u + static_cast<std::size_t>(key_size) + static_cast<std::size_t>(value_size) !=
        payload.size()) {
        throw std::runtime_error("read_record: key/value sizes inconsistent with payload");
    }

    Record r;
    r.key.assign(payload.begin() + 8, payload.begin() + 8 + key_size);
    r.value.assign(payload.begin() + 8 + key_size, payload.end());
    return r;
}

}  // namespace mini_kafka
