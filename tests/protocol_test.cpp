#include "mini_kafka/protocol.h"

#include <string>

#include <gtest/gtest.h>

namespace {

mini_kafka::Record make_record(const std::string& key, const std::string& value) {
    mini_kafka::Record record;
    record.key.assign(key.begin(), key.end());
    record.value.assign(value.begin(), value.end());
    return record;
}

}  // namespace

TEST(ProtocolTest, ProduceRequestRoundTripsRecord) {
    const mini_kafka::Record original = make_record("key", "value");

    const std::vector<uint8_t> payload = mini_kafka::encode_produce_request(original);
    EXPECT_EQ(mini_kafka::decode_produce_request(payload), original);
}

TEST(ProtocolTest, ConsumeResponseRoundTripsRecords) {
    const std::vector<mini_kafka::Record> original = {
            make_record("a", "1"),
            make_record("b", "2"),
            make_record("", "3"),
    };

    const std::vector<uint8_t> payload = mini_kafka::encode_consume_response(original);
    EXPECT_EQ(mini_kafka::decode_consume_response(payload), original);
}

TEST(ProtocolTest, ErrorResponseRoundTripsMessage) {
    const std::string original = "something went wrong";

    const std::vector<uint8_t> payload = mini_kafka::encode_error_response(original);
    EXPECT_EQ(mini_kafka::decode_error_response(payload), original);
}
