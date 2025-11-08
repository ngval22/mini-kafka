#include "mini_kafka/sparse_index.h"

#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <utility>

namespace mini_kafka {

namespace {

constexpr std::size_t kEntrySize = 24;

void append_u64_le(std::vector<uint8_t>& buf, std::uint64_t value) {
    for (int shift = 0; shift < 64; shift += 8) {
        buf.push_back(static_cast<uint8_t>((value >> shift) & 0xFFu));
    }
}

std::uint64_t read_u64_le(const uint8_t* bytes) {
    std::uint64_t value = 0;
    for (int shift = 0; shift < 64; shift += 8) {
        value |= static_cast<std::uint64_t>(bytes[shift / 8]) << shift;
    }
    return value;
}

IndexEntry decode_entry(const uint8_t* bytes) {
    return IndexEntry{read_u64_le(bytes), read_u64_le(bytes + 8), read_u64_le(bytes + 16)};
}

std::vector<uint8_t> encode_entry(const IndexEntry& entry) {
    std::vector<uint8_t> bytes;
    bytes.reserve(kEntrySize);
    append_u64_le(bytes, entry.offset);
    append_u64_le(bytes, entry.segment_base_offset);
    append_u64_le(bytes, entry.byte_position);
    return bytes;
}

}  // namespace

SparseIndex::SparseIndex(std::string path, const std::uint32_t index_interval)
        : path_(std::move(path)), index_interval_(index_interval) {
    if (index_interval_ == 0) {
        throw std::runtime_error("sparse_index: index_interval must be at least 1");
    }
    load_from_disk();
}

std::uint32_t SparseIndex::index_interval() const {
    return index_interval_;
}

const std::vector<IndexEntry>& SparseIndex::entries() const {
    return entries_;
}

void SparseIndex::load_from_disk() {
    entries_.clear();

    std::ifstream in(path_, std::ios::in | std::ios::binary);
    if (!in) {
        return;
    }

    in.seekg(0, std::ios::end);
    const std::streamoff file_size = in.tellg();
    if (file_size < 0) {
        throw std::runtime_error("sparse_index: failed to stat index: " + path_);
    }
    if (file_size % static_cast<std::streamoff>(kEntrySize) != 0) {
        throw std::runtime_error("sparse_index: corrupt index file size: " + path_);
    }

    in.seekg(0, std::ios::beg);
    std::vector<uint8_t> bytes(static_cast<std::size_t>(file_size));
    if (!bytes.empty()) {
        in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        if (!in) {
            throw std::runtime_error("sparse_index: failed to read index: " + path_);
        }
    }

    entries_.reserve(bytes.size() / kEntrySize);
    for (std::size_t offset = 0; offset < bytes.size(); offset += kEntrySize) {
        entries_.push_back(decode_entry(bytes.data() + offset));
    }

    for (std::size_t i = 1; i < entries_.size(); ++i) {
        if (entries_[i].offset <= entries_[i - 1].offset) {
            throw std::runtime_error("sparse_index: entries must be strictly increasing");
        }
    }
}

void SparseIndex::append_entry(const std::uint64_t offset, const std::uint64_t segment_base_offset,
                               const std::uint64_t byte_position) {
    if (!entries_.empty() && entries_.back().offset >= offset) {
        return;
    }

    const IndexEntry entry{offset, segment_base_offset, byte_position};
    entries_.push_back(entry);

    const std::vector<uint8_t> encoded = encode_entry(entry);
    std::ofstream out(path_, std::ios::out | std::ios::app | std::ios::binary);
    if (!out) {
        throw std::runtime_error("sparse_index: failed to open index for append: " + path_);
    }
    out.write(reinterpret_cast<const char*>(encoded.data()),
              static_cast<std::streamsize>(encoded.size()));
    out.flush();
    if (!out) {
        throw std::runtime_error("sparse_index: failed to append index entry: " + path_);
    }
}

std::optional<IndexEntry> SparseIndex::lookup(const std::uint64_t offset) const {
    if (entries_.empty()) {
        return std::nullopt;
    }

    auto it = std::upper_bound(
            entries_.begin(), entries_.end(), offset,
            [](const std::uint64_t value, const IndexEntry& entry) { return value < entry.offset; });
    if (it == entries_.begin()) {
        return std::nullopt;
    }
    return *(--it);
}

}  // namespace mini_kafka
