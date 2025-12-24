#include "mini_kafka/broker.h"
#include "mini_kafka/broker_metrics.h"
#include "mini_kafka/client.h"
#include "mini_kafka/topic.h"

#include <atomic>
#include <filesystem>
#include <set>
#include <string>
#include <thread>
#include <vector>

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

    const mini_kafka::BrokerMetricsSnapshot metrics = broker.metrics();
    EXPECT_EQ(metrics.produce_count, 2u);
    EXPECT_EQ(metrics.consume_count, 1u);
    EXPECT_EQ(metrics.error_count, 0u);
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

TEST(BrokerClientTest, ConcurrentProducesOverTcp) {
    TempDataDir tmp;
    mini_kafka::Broker broker(tmp.path(), 0);
    broker.add_topic(mini_kafka::make_topic_metadata("events", 1));

    constexpr int k_client_threads = 10;
    constexpr int k_connections = k_client_threads + 1;  // produces + one consume

    std::exception_ptr server_error;
    std::thread server([&]() {
        try {
            broker.serve_n(static_cast<std::size_t>(k_connections));
        } catch (...) {
            server_error = std::current_exception();
        }
    });

    std::atomic<int> produce_failures{0};
    std::vector<std::thread> clients;
    clients.reserve(k_client_threads);
    for (int i = 0; i < k_client_threads; ++i) {
        clients.emplace_back([&broker, &produce_failures, i]() {
            try {
                mini_kafka::produce("127.0.0.1", broker.port(), "events",
                                    make_record("", "msg-" + std::to_string(i)));
            } catch (...) {
                produce_failures.fetch_add(1);
            }
        });
    }
    for (std::thread& client : clients) {
        client.join();
    }
    EXPECT_EQ(produce_failures.load(), 0);

    const std::vector<mini_kafka::Record> records =
            mini_kafka::consume_all("127.0.0.1", broker.port(), "events", 0);

    server.join();
    if (server_error) {
        std::rethrow_exception(server_error);
    }

    ASSERT_EQ(records.size(), static_cast<std::size_t>(k_client_threads));

    std::set<std::string> values;
    for (const mini_kafka::Record& record : records) {
        ASSERT_TRUE(record.key.empty());
        values.insert(std::string(record.value.begin(), record.value.end()));
    }
    for (int i = 0; i < k_client_threads; ++i) {
        EXPECT_NE(values.find("msg-" + std::to_string(i)), values.end());
    }
}

TEST(BrokerClientTest, LeaderReplicaFetchReturnsRecordsFromOffset) {
    TempDataDir tmp;
    mini_kafka::Broker broker(tmp.path(), 0);
    broker.add_topic(mini_kafka::make_topic_metadata("events", 1));

    std::exception_ptr server_error;
    std::thread server([&]() {
        try {
            broker.serve_n(5);
        } catch (...) {
            server_error = std::current_exception();
        }
    });

    mini_kafka::produce("127.0.0.1", broker.port(), "events", make_record("k0", "v0"));
    mini_kafka::produce("127.0.0.1", broker.port(), "events", make_record("k1", "v1"));
    mini_kafka::produce("127.0.0.1", broker.port(), "events", make_record("k2", "v2"));

    const std::vector<mini_kafka::Record> from_zero =
            mini_kafka::replica_fetch("127.0.0.1", broker.port(), "events", 0, 0);
    const std::vector<mini_kafka::Record> from_two =
            mini_kafka::replica_fetch("127.0.0.1", broker.port(), "events", 0, 2);

    server.join();
    if (server_error) {
        std::rethrow_exception(server_error);
    }

    ASSERT_EQ(from_zero.size(), 3u);
    EXPECT_EQ(from_zero[0], make_record("k0", "v0"));
    EXPECT_EQ(from_zero[2], make_record("k2", "v2"));

    ASSERT_EQ(from_two.size(), 1u);
    EXPECT_EQ(from_two[0], make_record("k2", "v2"));
}

TEST(BrokerClientTest, FollowerRequiresLeaderEndpoint) {
    TempDataDir tmp;
    mini_kafka::BrokerOptions bad_host;
    bad_host.data_dir = tmp.path();
    bad_host.port = 0;
    bad_host.role = mini_kafka::BrokerRole::Follower;
    bad_host.leader_host = "";
    bad_host.leader_port = 9092;
    EXPECT_THROW(mini_kafka::Broker(std::move(bad_host)), std::runtime_error);

    mini_kafka::BrokerOptions bad_port;
    bad_port.data_dir = tmp.path();
    bad_port.port = 0;
    bad_port.role = mini_kafka::BrokerRole::Follower;
    bad_port.leader_host = "127.0.0.1";
    bad_port.leader_port = 0;
    EXPECT_THROW(mini_kafka::Broker(std::move(bad_port)), std::runtime_error);
}

TEST(BrokerClientTest, FollowerOptionsRecordsUpstream) {
    TempDataDir tmp;
    mini_kafka::BrokerOptions opts;
    opts.data_dir = tmp.path();
    opts.port = 0;
    opts.role = mini_kafka::BrokerRole::Follower;
    opts.leader_host = "127.0.0.1";
    opts.leader_port = 9001;
    mini_kafka::Broker broker(std::move(opts));
    EXPECT_EQ(broker.role(), mini_kafka::BrokerRole::Follower);
    EXPECT_EQ(broker.leader_host(), "127.0.0.1");
    EXPECT_EQ(broker.leader_port(), static_cast<uint16_t>(9001));
}
