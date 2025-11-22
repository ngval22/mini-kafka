#include "mini_kafka/log.h"

#include <stdexcept>
#include <utility>

namespace mini_kafka {

Log::Log(std::string path, const FlushPolicy flush_policy)
        : path_(std::move(path)), flush_policy_(flush_policy) {
    out_.open(path_, std::ios::out | std::ios::app | std::ios::binary);
    if (!out_) {
        throw std::runtime_error("Log: failed to open file for append: " + path_);
    }
}

FlushPolicy Log::flush_policy() const {
    return flush_policy_;
}

void Log::append(const Record& record) {
    write_record(out_, record);
    if (!out_) {
        throw std::runtime_error("Log: write failed for: " + path_);
    }
    apply_flush_policy(out_, path_, flush_policy_);
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
