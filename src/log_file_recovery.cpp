#include "mini_kafka/log_file_recovery.h"

#include "mini_kafka/record.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>

namespace mini_kafka {

namespace fs = std::filesystem;

std::size_t valid_record_byte_length(const std::string& path) {
    std::ifstream in(path, std::ios::in | std::ios::binary);
    if (!in) {
        throw std::runtime_error("log_file_recovery: failed to open for scan: " + path);
    }

    std::size_t valid_end = 0;
    while (in) {
        const std::size_t record_start = static_cast<std::size_t>(in.tellg());
        try {
            const std::optional<Record> record = read_record(in);
            if (!record) {
                valid_end = record_start;
                break;
            }
            valid_end = static_cast<std::size_t>(in.tellg());
        } catch (const std::exception&) {
            valid_end = record_start;
            break;
        }
    }
    return valid_end;
}

void truncate_log_file(const std::string& path, const std::size_t byte_length) {
    std::error_code ec;
    fs::resize_file(path, byte_length, ec);
    if (ec) {
        throw std::runtime_error("log_file_recovery: failed to truncate: " + path);
    }
}

}  // namespace mini_kafka
