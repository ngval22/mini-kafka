#pragma once

#include <fstream>
#include <string>
#include <vector>

#include "mini_kafka/record.h"

namespace mini_kafka {

// Single append-only log backed by one file on disk.
// Restart-safe: after this Log is destroyed (flushes on close), constructing a
// new Log on the same path and calling read_all() yields the same records.
class Log {
public:
    explicit Log(std::string path);

    Log(const Log&) = delete;
    Log& operator=(const Log&) = delete;
    Log(Log&&) = default;
    Log& operator=(Log&&) = default;

    void append(const Record& record);
    std::vector<Record> read_all() const;

private:
    std::string path_;
    std::ofstream out_;
};

}  // namespace mini_kafka
