#include "mini_kafka/flush_policy.h"
#include "mini_kafka/log.h"
#include "mini_kafka/segmented_log.h"

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

namespace fs = std::filesystem;

namespace {

mini_kafka::Record make_record(const std::string& key, const std::string& value) {
    mini_kafka::Record record;
    record.key.assign(key.begin(), key.end());
    record.value.assign(value.begin(), value.end());
    return record;
}

}  // namespace

TEST(FlushPolicyTest, LogStoresPolicy) {
    const fs::path path =
            fs::temp_directory_path() /
            ("mini_kafka_flush_policy_" + std::to_string(reinterpret_cast<uintptr_t>(&path)) +
             ".bin");
    fs::remove(path);

    mini_kafka::Log buffered(path.string(), mini_kafka::FlushPolicy::Buffered);
    EXPECT_EQ(buffered.flush_policy(), mini_kafka::FlushPolicy::Buffered);

    mini_kafka::Log fsync(path.string(), mini_kafka::FlushPolicy::Fsync);
    EXPECT_EQ(fsync.flush_policy(), mini_kafka::FlushPolicy::Fsync);

    fs::remove(path);
}

TEST(FlushPolicyTest, SegmentedLogFsyncAppendSucceeds) {
    const fs::path dir =
            fs::temp_directory_path() /
            ("mini_kafka_flush_policy_seg_" + std::to_string(reinterpret_cast<uintptr_t>(&dir)));
    fs::remove_all(dir);

    mini_kafka::SegmentedLog log(dir.string(), 1024, 4, mini_kafka::FlushPolicy::Fsync);
    log.append(make_record("k", "v"));
    ASSERT_EQ(log.read_all().size(), 1u);
    EXPECT_EQ(log.flush_policy(), mini_kafka::FlushPolicy::Fsync);

    fs::remove_all(dir);
}
