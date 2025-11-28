#include "mini_kafka/log.h"

#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

namespace fs = std::filesystem;

namespace {

class TempLogFile {
public:
    TempLogFile() {
        path_ = fs::temp_directory_path() /
                ("mini_kafka_log_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)) +
                 ".bin");
        fs::remove(path_);
    }
    ~TempLogFile() { fs::remove(path_); }

    const std::string path() const { return path_.string(); }

private:
    fs::path path_;
};

mini_kafka::Record make_record(const std::string& key, const std::string& value) {
    mini_kafka::Record r;
    r.key.assign(key.begin(), key.end());
    r.value.assign(value.begin(), value.end());
    return r;
}

}  // namespace

TEST(LogTest, EmptyLogReadsEmpty) {
    TempLogFile tmp;
    mini_kafka::Log log(tmp.path());
    EXPECT_TRUE(log.read_all().empty());
}

TEST(LogTest, AppendThenReadInSameSession) {
    TempLogFile tmp;
    mini_kafka::Log log(tmp.path());
    log.append(make_record("k1", "v1"));
    log.append(make_record("k2", "v2"));

    auto records = log.read_all();
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0], make_record("k1", "v1"));
    EXPECT_EQ(records[1], make_record("k2", "v2"));
}

TEST(LogTest, CleanRestartSafeReads) {
    TempLogFile tmp;
    {
        mini_kafka::Log log(tmp.path());
        log.append(make_record("a", "1"));
        log.append(make_record("b", "2"));
        log.append(make_record("c", "3"));
    }
    {
        mini_kafka::Log log(tmp.path());
        auto records = log.read_all();
        ASSERT_EQ(records.size(), 3u);
        EXPECT_EQ(records[0], make_record("a", "1"));
        EXPECT_EQ(records[1], make_record("b", "2"));
        EXPECT_EQ(records[2], make_record("c", "3"));
    }
}

TEST(LogTest, ReopeningAndAppendingMore) {
    TempLogFile tmp;
    {
        mini_kafka::Log log(tmp.path());
        log.append(make_record("a", "1"));
    }
    {
        mini_kafka::Log log(tmp.path());
        log.append(make_record("b", "2"));
    }
    {
        mini_kafka::Log log(tmp.path());
        auto records = log.read_all();
        ASSERT_EQ(records.size(), 2u);
        EXPECT_EQ(records[0], make_record("a", "1"));
        EXPECT_EQ(records[1], make_record("b", "2"));
    }
}

TEST(LogTest, TruncatesPartialTailOnStartup) {
    TempLogFile tmp;
    {
        mini_kafka::Log log(tmp.path());
        log.append(make_record("a", "1"));
        log.append(make_record("b", "2"));
        log.append(make_record("c", "3"));
    }

    {
        std::ofstream out(tmp.path(), std::ios::out | std::ios::app | std::ios::binary);
        ASSERT_TRUE(out);
        const std::string garbage = "partial-write-garbage";
        out.write(garbage.data(), static_cast<std::streamsize>(garbage.size()));
    }

    mini_kafka::Log log(tmp.path());
    const std::vector<mini_kafka::Record> recovered = log.read_all();
    ASSERT_EQ(recovered.size(), 3u);
    EXPECT_EQ(recovered[2], make_record("c", "3"));

    log.append(make_record("d", "4"));
    const std::vector<mini_kafka::Record> all = log.read_all();
    ASSERT_EQ(all.size(), 4u);
    EXPECT_EQ(all[3], make_record("d", "4"));
}

TEST(LogTest, HandlesVaryingRecordSizes) {
    TempLogFile tmp;
    mini_kafka::Log log(tmp.path());

    log.append(make_record("", ""));
    log.append(make_record("short", "value"));
    log.append(make_record("k", std::string(1024, 'x')));

    auto records = log.read_all();
    ASSERT_EQ(records.size(), 3u);
    EXPECT_TRUE(records[0].key.empty());
    EXPECT_TRUE(records[0].value.empty());
    EXPECT_EQ(records[2].value.size(), 1024u);
    EXPECT_EQ(records[2].value[0], 'x');
}
