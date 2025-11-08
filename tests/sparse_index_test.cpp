#include "mini_kafka/sparse_index.h"

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

namespace fs = std::filesystem;

namespace {

class TempIndexFile {
public:
    TempIndexFile() {
        path_ = fs::temp_directory_path() /
                ("mini_kafka_sparse_index_" + std::to_string(reinterpret_cast<uintptr_t>(this)) +
                 ".idx");
        fs::remove(path_);
    }

    ~TempIndexFile() {
        fs::remove(path_);
    }

    std::string path() const {
        return path_.string();
    }

private:
    fs::path path_;
};

}  // namespace

TEST(SparseIndexTest, AppendAndLookupFloorEntry) {
    TempIndexFile tmp;
    mini_kafka::SparseIndex index(tmp.path(), 4);

    index.append_entry(0, 0, 0);
    index.append_entry(4, 0, 120);
    index.append_entry(8, 0, 240);

    ASSERT_EQ(index.entries().size(), 3u);

    const std::optional<mini_kafka::IndexEntry> at_four = index.lookup(4);
    ASSERT_TRUE(at_four.has_value());
    EXPECT_EQ(at_four->offset, 4u);
    EXPECT_EQ(at_four->segment_base_offset, 0u);
    EXPECT_EQ(at_four->byte_position, 120u);

    const std::optional<mini_kafka::IndexEntry> at_seven = index.lookup(7);
    ASSERT_TRUE(at_seven.has_value());
    EXPECT_EQ(at_seven->offset, 4u);

    const std::optional<mini_kafka::IndexEntry> at_zero = index.lookup(0);
    ASSERT_TRUE(at_zero.has_value());
    EXPECT_EQ(at_zero->offset, 0u);
}

TEST(SparseIndexTest, ReloadsFromDisk) {
    TempIndexFile tmp;
    {
        mini_kafka::SparseIndex index(tmp.path(), 2);
        index.append_entry(0, 0, 0);
        index.append_entry(2, 0, 50);
    }

    mini_kafka::SparseIndex index(tmp.path(), 2);
    ASSERT_EQ(index.entries().size(), 2u);
    EXPECT_EQ(index.lookup(3)->offset, 2u);
}
