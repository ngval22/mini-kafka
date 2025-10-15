#include "mini_kafka/topic.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace {

std::vector<uint8_t> to_bytes(const std::string& text) {
    return std::vector<uint8_t>(text.begin(), text.end());
}

}  // namespace

TEST(TopicTest, MakeTopicMetadataRejectsInvalidInput) {
    EXPECT_THROW(mini_kafka::make_topic_metadata("", 1), std::runtime_error);
    EXPECT_THROW(mini_kafka::make_topic_metadata("events", 0), std::runtime_error);
}

TEST(TopicTest, PartitionForKeyIsDeterministic) {
    const std::vector<uint8_t> key = to_bytes("user-42");
    const std::uint32_t first = mini_kafka::partition_for_key(key, 4);
    const std::uint32_t second = mini_kafka::partition_for_key(key, 4);
    EXPECT_EQ(first, second);
    EXPECT_LT(first, 4u);
}

TEST(TopicTest, EmptyKeyUsesPartitionZero) {
    EXPECT_EQ(mini_kafka::partition_for_key({}, 8), 0u);
}

TEST(TopicTest, DifferentKeysCanMapToDifferentPartitions) {
    const std::uint32_t a = mini_kafka::partition_for_key(to_bytes("alpha"), 16);
    const std::uint32_t b = mini_kafka::partition_for_key(to_bytes("beta"), 16);
    EXPECT_NE(a, b);
}

TEST(TopicTest, RegistryStoresAndListsTopics) {
    mini_kafka::TopicRegistry registry;
    registry.add(mini_kafka::make_topic_metadata("zebra", 2));
    registry.add(mini_kafka::make_topic_metadata("alpha", 3));

    const mini_kafka::TopicMetadata* found = registry.find("alpha");
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->partition_count, 3u);

    const std::vector<mini_kafka::TopicMetadata> topics = registry.topics();
    ASSERT_EQ(topics.size(), 2u);
    EXPECT_EQ(topics[0].name, "alpha");
    EXPECT_EQ(topics[1].name, "zebra");
}

TEST(TopicTest, RegistryRejectsDuplicateTopic) {
    mini_kafka::TopicRegistry registry;
    registry.add(mini_kafka::make_topic_metadata("events", 2));
    EXPECT_THROW(registry.add(mini_kafka::make_topic_metadata("events", 4)), std::runtime_error);
}
