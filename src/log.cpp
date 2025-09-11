#include "mini_kafka/log.h"

#include <stdexcept>
#include <utility>

namespace mini_kafka {

Log::Log(std::string path) : path_(std::move(path)) {
    out_.open(path_, std::ios::out | std::ios::app | std::ios::binary);
    if (!out_) {
        throw std::runtime_error("Log: failed to open file for append: " + path_);
    }
}

void Log::append(const Record& record) {
    write_record(out_, record);
    out_.flush();
    if (!out_) {
        throw std::runtime_error("Log: write failed for: " + path_);
    }
}

std::vector<Record> Log::read_all() const {
    std::ifstream in(path_, std::ios::in | std::ios::binary);
    if (!in) {
        throw std::runtime_error("Log: failed to open file for read: " + path_);
    }
    std::vector<Record> records;
    while (auto r = read_record(in)) {
        records.push_back(std::move(*r));
    }
    return records;
}

}  // namespace mini_kafka
