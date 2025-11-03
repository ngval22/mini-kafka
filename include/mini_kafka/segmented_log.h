#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

#include "mini_kafka/record.h"

namespace mini_kafka {

// Append-only log split into size-bounded segment files under one directory:
//   {dir}/{base_offset:020d}.seg
// base_offset is the logical record index of the first record in that segment.
class SegmentedLog {
public:
    explicit SegmentedLog(std::string dir_path, std::size_t max_segment_bytes = 1024 * 1024);

    SegmentedLog(const SegmentedLog&) = delete;
    SegmentedLog& operator=(const SegmentedLog&) = delete;
    SegmentedLog(SegmentedLog&&) = delete;
    SegmentedLog& operator=(SegmentedLog&&) = delete;

    void append(const Record& record);
    std::vector<Record> read_all() const;

    const std::string& dir_path() const;
    std::size_t max_segment_bytes() const;
    std::size_t segment_file_count() const;

private:
    std::string segment_path(std::uint64_t base_offset) const;
    std::vector<std::uint64_t> list_segment_base_offsets() const;
    std::uint64_t compute_next_offset() const;
    void open_active_segment(std::uint64_t base_offset);
    void roll_segment();
    std::size_t active_segment_size() const;

    std::string dir_path_;
    std::size_t max_segment_bytes_;
    std::uint64_t next_offset_;
    std::uint64_t active_base_offset_;
    std::ofstream active_out_;
};

}  // namespace mini_kafka
