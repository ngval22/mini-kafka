#include "mini_kafka/partition_store.h"

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

namespace fs = std::filesystem;

namespace {

class TempLogDir {
public:
    TempLogDir() {
        path_ = fs::temp_directory_path() /
                ("mini_kafka_partition_store_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
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

TEST(PartitionStoreTest, PartitionsAreIsolated) {
    TempLogDir tmp;
    mini_kafka::PartitionLogStore store(tmp.path());
    store.add_topic(mini_kafka::make_topic_metadata("events", 2));

    store.append("events", 0, make_record("k0", "v0"));
    store.append("events", 1, make_record("k1", "v1"));

    EXPECT_EQ(store.read_all("events", 0), std::vector<mini_kafka::Record>{make_record("k0", "v0")});
    EXPECT_EQ(store.read_all("events", 1), std::vector<mini_kafka::Record>{make_record("k1", "v1")});
}

TEST(PartitionStoreTest, AppendByKeyRoutesToPartition) {
    TempLogDir tmp;
    mini_kafka::PartitionLogStore store(tmp.path());
    store.add_topic(mini_kafka::make_topic_metadata("events", 4));

    const mini_kafka::Record record = make_record("user-42", "payload");
    store.append_by_key("events", record);

    const std::uint32_t partition =
            mini_kafka::partition_for_key(record.key, 4);
    EXPECT_EQ(store.read_all("events", partition),
              std::vector<mini_kafka::Record>{record});
}

TEST(PartitionStoreTest, CleanRestartSafeReads) {
    TempLogDir tmp;
    const std::string base = tmp.path();

    {
        mini_kafka::PartitionLogStore store(base);
        store.add_topic(mini_kafka::make_topic_metadata("events", 2));
        store.append("events", 0, make_record("a", "1"));
        store.append("events", 1, make_record("b", "2"));
    }

    mini_kafka::PartitionLogStore store(base);
    store.add_topic(mini_kafka::make_topic_metadata("events", 2));

    EXPECT_EQ(store.read_all("events", 0), std::vector<mini_kafka::Record>{make_record("a", "1")});
    EXPECT_EQ(store.read_all("events", 1), std::vector<mini_kafka::Record>{make_record("b", "2")});
}

TEST(PartitionStoreTest, RejectsUnknownTopicOrPartition) {
    TempLogDir tmp;
    mini_kafka::PartitionLogStore store(tmp.path());
    store.add_topic(mini_kafka::make_topic_metadata("events", 2));

    EXPECT_THROW(store.append("missing", 0, make_record("k", "v")), std::runtime_error);
    EXPECT_THROW(store.append("events", 2, make_record("k", "v")), std::runtime_error);
    EXPECT_THROW(store.read_all("events", 2), std::runtime_error);
}

TEST(PartitionStoreTest, PartitionPathsFollowConvention) {
    TempLogDir tmp;
    mini_kafka::PartitionLogStore store(tmp.path());
    EXPECT_EQ(store.partition_path("events", 3), tmp.path() + "/events-p3.bin");
}
