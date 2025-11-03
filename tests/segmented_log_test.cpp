#include "mini_kafka/segmented_log.h"

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

namespace fs = std::filesystem;

namespace {

class TempLogDir {
public:
    TempLogDir() {
        path_ = fs::temp_directory_path() /
                ("mini_kafka_segmented_log_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        fs::remove_all(path_);
    }

    ~TempLogDir() {
        fs::remove_all(path_);
    }

    std::string path() const {
        return path_.string();
    }

private:
    fs::path path_;
};

mini_kafka::Record make_record(const std::string& key, const std::string& value) {
    mini_kafka::Record record;
    record.key.assign(key.begin(), key.end());
    record.value.assign(value.begin(), value.end());
    return record;
}

}  // namespace

TEST(SegmentedLogTest, EmptyLogReadsEmpty) {
    TempLogDir tmp;
    mini_kafka::SegmentedLog log(tmp.path(), 256);
    EXPECT_TRUE(log.read_all().empty());
    EXPECT_EQ(log.segment_file_count(), 1u);
}

TEST(SegmentedLogTest, RollsWhenSegmentReachesMaxSize) {
    TempLogDir tmp;
    constexpr std::size_t kMaxSegmentBytes = 64;

    {
        mini_kafka::SegmentedLog log(tmp.path(), kMaxSegmentBytes);
        log.append(make_record("k1", std::string(40, 'a')));
        log.append(make_record("k2", std::string(40, 'b')));
        EXPECT_GE(log.segment_file_count(), 2u);
    }

    mini_kafka::SegmentedLog log(tmp.path(), kMaxSegmentBytes);
    const std::vector<mini_kafka::Record> records = log.read_all();
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0], make_record("k1", std::string(40, 'a')));
    EXPECT_EQ(records[1], make_record("k2", std::string(40, 'b')));
}

TEST(SegmentedLogTest, CleanRestartSafeReadsAcrossSegments) {
    TempLogDir tmp;
    constexpr std::size_t kMaxSegmentBytes = 64;

    {
        mini_kafka::SegmentedLog log(tmp.path(), kMaxSegmentBytes);
        log.append(make_record("a", "1"));
        log.append(make_record("b", "2"));
        log.append(make_record("c", "3"));
    }

    mini_kafka::SegmentedLog log(tmp.path(), kMaxSegmentBytes);
    const std::vector<mini_kafka::Record> records = log.read_all();
    ASSERT_EQ(records.size(), 3u);
    EXPECT_EQ(records[0], make_record("a", "1"));
    EXPECT_EQ(records[1], make_record("b", "2"));
    EXPECT_EQ(records[2], make_record("c", "3"));
}
