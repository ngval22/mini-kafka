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

TEST(ProtocolTest, JoinGroupRequestRoundTrips) {
    const mini_kafka::JoinGroupRequest original{"my-group", "alice", "events"};
    const std::vector<uint8_t> payload = mini_kafka::encode_join_group_request(original);
    const mini_kafka::JoinGroupRequest decoded = mini_kafka::decode_join_group_request(payload);
    EXPECT_EQ(decoded.group_id, original.group_id);
    EXPECT_EQ(decoded.member_id, original.member_id);
    EXPECT_EQ(decoded.topic, original.topic);
}

TEST(ProtocolTest, JoinGroupResponseRoundTrips) {
    const mini_kafka::JoinGroupResponse original{"alice", {0, 3}};
    const std::vector<uint8_t> payload = mini_kafka::encode_join_group_response(original);
    const mini_kafka::JoinGroupResponse decoded = mini_kafka::decode_join_group_response(payload);
    EXPECT_EQ(decoded.member_id, original.member_id);
    EXPECT_EQ(decoded.partitions, original.partitions);
}

TEST(ProtocolTest, LeaveGroupRequestRoundTrips) {
    const mini_kafka::LeaveGroupRequest original{"my-group", "alice"};
    const std::vector<uint8_t> payload = mini_kafka::encode_leave_group_request(original);
    const mini_kafka::LeaveGroupRequest decoded = mini_kafka::decode_leave_group_request(payload);
    EXPECT_EQ(decoded.group_id, original.group_id);
    EXPECT_EQ(decoded.member_id, original.member_id);
}

TEST(ProtocolTest, LeaveGroupOkResponseIsEmptyBody) {
    const std::vector<uint8_t> payload = mini_kafka::encode_leave_group_ok_response();
    EXPECT_NO_THROW(mini_kafka::decode_leave_group_ok_response(payload));
}

TEST(ProtocolTest, OffsetCommitRequestRoundTrips) {
    const mini_kafka::OffsetCommitRequest original{"g", "events", 2, 99};
    const std::vector<uint8_t> payload = mini_kafka::encode_offset_commit_request(original);
    const mini_kafka::OffsetCommitRequest decoded =
            mini_kafka::decode_offset_commit_request(payload);
    EXPECT_EQ(decoded.group_id, original.group_id);
    EXPECT_EQ(decoded.topic, original.topic);
    EXPECT_EQ(decoded.partition, original.partition);
    EXPECT_EQ(decoded.offset, original.offset);
}

TEST(ProtocolTest, OffsetCommitOkResponseIsEmptyBody) {
    const std::vector<uint8_t> payload = mini_kafka::encode_offset_commit_ok_response();
    EXPECT_NO_THROW(mini_kafka::decode_offset_commit_ok_response(payload));
}

TEST(ProtocolTest, GroupConsumeRequestRoundTrips) {
    const mini_kafka::GroupConsumeRequest original{"g", "events", 1};
    const std::vector<uint8_t> payload = mini_kafka::encode_group_consume_request(original);
    const mini_kafka::GroupConsumeRequest decoded =
            mini_kafka::decode_group_consume_request(payload);
    EXPECT_EQ(decoded.group_id, original.group_id);
    EXPECT_EQ(decoded.topic, original.topic);
    EXPECT_EQ(decoded.partition, original.partition);
}
