#pragma once

#include <cstddef>
#include <string>

namespace mini_kafka {

// Byte length of the longest valid record prefix in an append-only log file.
// Stops at EOF, a partial record, or the first CRC/format error.
std::size_t valid_record_byte_length(const std::string& path);

void truncate_log_file(const std::string& path, std::size_t byte_length);

}  // namespace mini_kafka
