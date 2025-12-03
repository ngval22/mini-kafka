#include "mini_kafka/broker.h"
#include "mini_kafka/client.h"
#include "mini_kafka/topic.h"

#include <filesystem>
#include <string>
#include <thread>

#include <gtest/gtest.h>

namespace fs = std::filesystem;

namespace {

class TempDataDir {
public:
    TempDataDir() {
        path_ = fs::temp_directory_path() /
                ("mini_kafka_broker_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        fs::remove_all(path_);
    }

    ~TempDataDir() {
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

TEST(BrokerClientTest, ProduceThenConsumeOverTcp) {
    TempDataDir tmp;
    mini_kafka::Broker broker(tmp.path(), 0);
    broker.add_topic(mini_kafka::make_topic_metadata("events", 1));

    std::exception_ptr server_error;
    std::thread server([&]() {
        try {
            broker.serve_n(3);
        } catch (...) {
            server_error = std::current_exception();
        }
    });

    mini_kafka::produce("127.0.0.1", broker.port(), "events", make_record("k1", "v1"));
    mini_kafka::produce("127.0.0.1", broker.port(), "events", make_record("k2", "v2"));
    const std::vector<mini_kafka::Record> records =
            mini_kafka::consume_all("127.0.0.1", broker.port(), "events", 0);

    server.join();
    if (server_error) {
        std::rethrow_exception(server_error);
    }

    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0], make_record("k1", "v1"));
    EXPECT_EQ(records[1], make_record("k2", "v2"));
}

TEST(BrokerClientTest, PartitionRoutingIsolatesRecords) {
    TempDataDir tmp;
    mini_kafka::Broker broker(tmp.path(), 0);
    broker.add_topic(mini_kafka::make_topic_metadata("events", 4));

    std::exception_ptr server_error;
    std::thread server([&]() {
        try {
            broker.serve_n(4);
        } catch (...) {
            server_error = std::current_exception();
        }
    });

    const mini_kafka::Record on_p0 = make_record("", "only-on-zero");
    const mini_kafka::Record on_p2 = make_record("user-42", "on-two");

    mini_kafka::produce("127.0.0.1", broker.port(), "events", on_p0);
    mini_kafka::produce("127.0.0.1", broker.port(), "events", on_p2);

    const std::uint32_t partition_for_user =
            mini_kafka::partition_for_key(on_p2.key, 4);

    const std::vector<mini_kafka::Record> p0_records =
            mini_kafka::consume_all("127.0.0.1", broker.port(), "events", 0);
    const std::vector<mini_kafka::Record> routed_records = mini_kafka::consume_all(
            "127.0.0.1", broker.port(), "events", partition_for_user);

    server.join();
    if (server_error) {
        std::rethrow_exception(server_error);
    }

    ASSERT_EQ(p0_records.size(), 1u);
    EXPECT_EQ(p0_records[0], on_p0);
    ASSERT_EQ(routed_records.size(), 1u);
    EXPECT_EQ(routed_records[0], on_p2);
}
