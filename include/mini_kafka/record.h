#pragma once

#include <cstdint>
#include <iosfwd>
#include <optional>
#include <vector>

namespace mini_kafka {

struct Record {
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
};

bool operator==(const Record& a, const Record& b);
bool operator!=(const Record& a, const Record& b);

// On-disk framing (all integers little-endian):
//   uint32 length   = bytes that follow (crc + key_size + value_size + key + value)
//   uint32 crc      = crc32 of everything after this field
//   uint32 key_size
//   uint32 value_size
//   bytes  key      (key_size bytes)
//   bytes  value    (value_size bytes)
void write_record(std::ostream& out, const Record& record);

// Returns std::nullopt at clean end-of-file.
// Throws std::runtime_error on truncation or CRC mismatch.
std::optional<Record> read_record(std::istream& in);

}  // namespace mini_kafka
