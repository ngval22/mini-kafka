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

TEST(ProtocolTest, ProduceRequestRoundTripsTopicAndRecord) {
    const mini_kafka::Record record = make_record("key", "value");

    const std::vector<uint8_t> payload = mini_kafka::encode_produce_request("events", record);
    const mini_kafka::ProduceRequest decoded = mini_kafka::decode_produce_request(payload);
    EXPECT_EQ(decoded.topic, "events");
    EXPECT_EQ(decoded.record, record);
}

TEST(ProtocolTest, ConsumeRequestRoundTripsTopicAndPartition) {
    const std::vector<uint8_t> payload = mini_kafka::encode_consume_request("events", 2);
    const mini_kafka::ConsumeRequest decoded = mini_kafka::decode_consume_request(payload);
    EXPECT_EQ(decoded.topic, "events");
    EXPECT_EQ(decoded.partition, 2u);
}

TEST(ProtocolTest, ReplicaFetchRequestRoundTripsTopicPartitionAndOffset) {
    const std::vector<uint8_t> payload =
            mini_kafka::encode_replica_fetch_request("events", 1, 42);
    const mini_kafka::ReplicaFetchRequest decoded =
            mini_kafka::decode_replica_fetch_request(payload);
    EXPECT_EQ(decoded.topic, "events");
    EXPECT_EQ(decoded.partition, 1u);
    EXPECT_EQ(decoded.from_offset, 42u);
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
