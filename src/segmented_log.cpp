#include "mini_kafka/segmented_log.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace mini_kafka {

namespace fs = std::filesystem;

namespace {

constexpr char kSegmentSuffix[] = ".seg";
constexpr char kIndexFileName[] = "sparse.idx";

std::vector<Record> read_segment_file(const std::string& path) {
    std::ifstream in(path, std::ios::in | std::ios::binary);
    if (!in) {
        throw std::runtime_error("segmented_log: failed to open segment for read: " + path);
    }
    std::vector<Record> records;
    while (auto record = read_record(in)) {
        records.push_back(std::move(*record));
    }
    return records;
}

std::uint64_t parse_base_offset(const fs::path& path) {
    const std::string stem = path.stem().string();
    if (stem.size() != 20) {
        throw std::runtime_error("segmented_log: invalid segment filename: " + path.string());
    }
    return std::stoull(stem);
}

std::uint64_t count_records_from_position(const std::string& segment_path,
                                         const std::uint64_t byte_position,
                                         const std::uint64_t start_offset) {
    std::ifstream in(segment_path, std::ios::in | std::ios::binary);
    if (!in) {
        throw std::runtime_error("segmented_log: failed to open segment for scan: " + segment_path);
    }
    in.seekg(static_cast<std::streamoff>(byte_position), std::ios::beg);

    std::uint64_t offset = start_offset;
    while (read_record(in)) {
        ++offset;
    }
    return offset;
}

}  // namespace

SegmentedLog::SegmentedLog(std::string dir_path, const std::size_t max_segment_bytes,
                           const std::uint32_t index_interval)
        : dir_path_(std::move(dir_path)),
          max_segment_bytes_(max_segment_bytes),
          index_(index_path(), index_interval),
          next_offset_(0),
          active_base_offset_(0) {
    if (dir_path_.empty()) {
        throw std::runtime_error("segmented_log: dir_path must not be empty");
    }
    if (max_segment_bytes_ == 0) {
        throw std::runtime_error("segmented_log: max_segment_bytes must be at least 1");
    }

    std::error_code ec;
    fs::create_directories(dir_path_, ec);
    if (ec) {
        throw std::runtime_error("segmented_log: failed to create directory: " + dir_path_);
    }

    const std::vector<std::uint64_t> bases = list_segment_base_offsets();
    if (bases.empty()) {
        open_active_segment(0);
        return;
    }

    next_offset_ = compute_next_offset();
    active_base_offset_ = bases.back();
    open_active_segment(active_base_offset_);

    if (active_segment_size() >= max_segment_bytes_) {
        roll_segment();
    }
}

const std::string& SegmentedLog::dir_path() const {
    return dir_path_;
}

std::size_t SegmentedLog::max_segment_bytes() const {
    return max_segment_bytes_;
}

std::uint32_t SegmentedLog::index_interval() const {
    return index_.index_interval();
}

std::size_t SegmentedLog::segment_file_count() const {
    return list_segment_base_offsets().size();
}

std::optional<IndexEntry> SegmentedLog::index_lookup(const std::uint64_t offset) const {
    return index_.lookup(offset);
}

std::string SegmentedLog::index_path() const {
    return dir_path_ + "/" + kIndexFileName;
}

std::string SegmentedLog::segment_path(const std::uint64_t base_offset) const {
    std::ostringstream name;
    name.width(20);
    name.fill('0');
    name << std::dec << base_offset << kSegmentSuffix;
    return dir_path_ + "/" + name.str();
}

std::vector<std::uint64_t> SegmentedLog::list_segment_base_offsets() const {
    std::vector<std::uint64_t> bases;
    if (!fs::exists(dir_path_)) {
        return bases;
    }

    for (const fs::directory_entry& entry : fs::directory_iterator(dir_path_)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() != kSegmentSuffix) {
            continue;
        }
        bases.push_back(parse_base_offset(entry.path()));
    }

    std::sort(bases.begin(), bases.end());
    return bases;
}

std::uint64_t SegmentedLog::compute_next_offset_full_scan() const {
    std::uint64_t expected_base = 0;
    for (const std::uint64_t base : list_segment_base_offsets()) {
        if (base != expected_base) {
            throw std::runtime_error("segmented_log: missing segment at offset " +
                                     std::to_string(expected_base));
        }
        const std::vector<Record> records = read_segment_file(segment_path(base));
        expected_base += records.size();
    }
    return expected_base;
}

std::uint64_t SegmentedLog::scan_tail_from_index(const IndexEntry& entry) const {
    std::uint64_t offset = count_records_from_position(segment_path(entry.segment_base_offset),
                                                       entry.byte_position, entry.offset);

    for (const std::uint64_t base : list_segment_base_offsets()) {
        if (base <= entry.segment_base_offset) {
            continue;
        }
        if (base != offset) {
            throw std::runtime_error("segmented_log: missing segment at offset " +
                                     std::to_string(offset));
        }
        const std::vector<Record> records = read_segment_file(segment_path(base));
        offset += records.size();
    }
    return offset;
}

std::uint64_t SegmentedLog::compute_next_offset() const {
    if (index_.entries().empty()) {
        return compute_next_offset_full_scan();
    }
    return scan_tail_from_index(index_.entries().back());
}

void SegmentedLog::maybe_index(const std::uint64_t offset, const std::uint64_t segment_base_offset,
                               const std::uint64_t byte_position) {
    index_.append_entry(offset, segment_base_offset, byte_position);
}

void SegmentedLog::open_active_segment(const std::uint64_t base_offset) {
    if (active_out_.is_open()) {
        active_out_.close();
    }

    active_base_offset_ = base_offset;
    const std::string path = segment_path(base_offset);
    active_out_.open(path, std::ios::out | std::ios::app | std::ios::binary);
    if (!active_out_) {
        throw std::runtime_error("segmented_log: failed to open segment for append: " + path);
    }
}

void SegmentedLog::roll_segment() {
    if (active_out_.is_open()) {
        active_out_.close();
    }
    open_active_segment(next_offset_);
    maybe_index(next_offset_, active_base_offset_, 0);
}

std::size_t SegmentedLog::active_segment_size() const {
    if (!active_out_.is_open()) {
        return 0;
    }
    const std::string path = segment_path(active_base_offset_);
    std::error_code ec;
    if (!fs::exists(path, ec)) {
        return 0;
    }
    return static_cast<std::size_t>(fs::file_size(path, ec));
}

void SegmentedLog::append(const Record& record) {
    const std::vector<uint8_t> encoded = encode_record(record);
    if (active_segment_size() > 0 &&
        active_segment_size() + encoded.size() > max_segment_bytes_) {
        roll_segment();
    }

    const std::uint64_t offset = next_offset_;
    const std::uint64_t byte_position = active_segment_size();
    if (offset % index_.index_interval() == 0) {
        maybe_index(offset, active_base_offset_, byte_position);
    }

    write_record(active_out_, record);
    active_out_.flush();
    if (!active_out_) {
        throw std::runtime_error("segmented_log: write failed for: " +
                                 segment_path(active_base_offset_));
    }
    ++next_offset_;
}

std::vector<Record> SegmentedLog::read_all() const {
    std::vector<Record> records;
    for (const std::uint64_t base : list_segment_base_offsets()) {
        const std::vector<Record> segment_records = read_segment_file(segment_path(base));
        records.insert(records.end(), segment_records.begin(), segment_records.end());
    }
    return records;
}

}  // namespace mini_kafka
