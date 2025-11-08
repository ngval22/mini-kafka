#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mini_kafka {

struct IndexEntry {
    std::uint64_t offset;
    std::uint64_t segment_base_offset;
    std::uint64_t byte_position;
};

// Sparse index stored at {partition_dir}/sparse.idx
// Each entry is 24 bytes (little-endian): offset, segment_base_offset, byte_position.
class SparseIndex {
public:
    SparseIndex(std::string path, std::uint32_t index_interval);

    SparseIndex(const SparseIndex&) = delete;
    SparseIndex& operator=(const SparseIndex&) = delete;

    std::uint32_t index_interval() const;
    const std::vector<IndexEntry>& entries() const;

    void append_entry(std::uint64_t offset, std::uint64_t segment_base_offset,
                      std::uint64_t byte_position);

    // Largest indexed offset <= query, or nullopt if query is before the first entry.
    std::optional<IndexEntry> lookup(std::uint64_t offset) const;

private:
    void load_from_disk();

    std::string path_;
    std::uint32_t index_interval_;
    std::vector<IndexEntry> entries_;
};

}  // namespace mini_kafka
