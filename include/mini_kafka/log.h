#pragma once

#include <fstream>
#include <string>
#include <vector>

#include "mini_kafka/flush_policy.h"
#include "mini_kafka/record.h"

namespace mini_kafka {

// Single append-only log backed by one file on disk.
// On open, truncates any partial or corrupt record tail left by a crash.
// Clean-restart-safe: constructing a new Log on the same path and calling
// read_all() yields the same complete records that were fully written.
class Log {
public:
    explicit Log(std::string path, FlushPolicy flush_policy = FlushPolicy::Flush);

    Log(const Log&) = delete;
    Log& operator=(const Log&) = delete;
    Log(Log&&) = default;
    Log& operator=(Log&&) = default;

    void append(const Record& record);
    std::vector<Record> read_all() const;

    FlushPolicy flush_policy() const;

private:
    std::string path_;
    FlushPolicy flush_policy_;
    std::ofstream out_;
};

}  // namespace mini_kafka
