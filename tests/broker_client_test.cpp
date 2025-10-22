#include "mini_kafka/broker.h"
#include "mini_kafka/client.h"

#include <filesystem>
#include <string>
#include <thread>

#include <gtest/gtest.h>

namespace fs = std::filesystem;

namespace {

class TempLogFile {
public:
    TempLogFile() {
        path_ = fs::temp_directory_path() /
                ("mini_kafka_broker_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)) +
                 ".bin");
        fs::remove(path_);
    }

    ~TempLogFile() {
        fs::remove(path_);
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
    TempLogFile tmp;
    mini_kafka::Broker broker(tmp.path(), 0);

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
