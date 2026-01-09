#include "mini_kafka/consumer_group.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

TEST(ConsumerGroupTest, JoinAssignsMemberIdWhenEmpty) {
    mini_kafka::ConsumerGroupRegistry registry;
    const std::string member = registry.join("my-group", "");
    EXPECT_FALSE(member.empty());
    EXPECT_EQ(registry.members("my-group"), std::vector<std::string>{member});
}

TEST(ConsumerGroupTest, JoinWithExplicitMemberId) {
    mini_kafka::ConsumerGroupRegistry registry;
    EXPECT_EQ(registry.join("g", "alice"), "alice");
    EXPECT_EQ(registry.members("g"), std::vector<std::string>{"alice"});
}

TEST(ConsumerGroupTest, RejoinIsIdempotent) {
    mini_kafka::ConsumerGroupRegistry registry;
    registry.join("g", "alice");
    registry.join("g", "alice");
    EXPECT_EQ(registry.members("g"), std::vector<std::string>{"alice"});
}

TEST(ConsumerGroupTest, MultipleMembersSorted) {
    mini_kafka::ConsumerGroupRegistry registry;
    registry.join("g", "bob");
    registry.join("g", "alice");
    EXPECT_EQ(registry.members("g"), (std::vector<std::string>{"alice", "bob"}));
}

TEST(ConsumerGroupTest, LeaveRemovesMemberAndDropsEmptyGroup) {
    mini_kafka::ConsumerGroupRegistry registry;
    registry.join("g", "alice");
    registry.join("g", "bob");
    registry.leave("g", "alice");
    EXPECT_EQ(registry.members("g"), std::vector<std::string>{"bob"});
    registry.leave("g", "bob");
    EXPECT_TRUE(registry.members("g").empty());
}

TEST(ConsumerGroupTest, LeaveUnknownMemberIsNoOp) {
    mini_kafka::ConsumerGroupRegistry registry;
    registry.join("g", "alice");
    registry.leave("g", "missing");
    EXPECT_EQ(registry.members("g"), std::vector<std::string>{"alice"});
}

TEST(ConsumerGroupTest, RejectsEmptyGroupId) {
    mini_kafka::ConsumerGroupRegistry registry;
    EXPECT_THROW(registry.join("", "alice"), std::runtime_error);
}

TEST(ConsumerGroupTest, RoundRobinAssignsPartitions) {
    const mini_kafka::PartitionAssignment assignment = mini_kafka::assign_partitions_round_robin(
            {"bob", "alice", "charlie"}, 4);

    ASSERT_EQ(assignment.at("alice"), (std::vector<std::uint32_t>{0, 3}));
    ASSERT_EQ(assignment.at("bob"), (std::vector<std::uint32_t>{1}));
    ASSERT_EQ(assignment.at("charlie"), (std::vector<std::uint32_t>{2}));
}

TEST(ConsumerGroupTest, RoundRobinSingleMemberGetsAllPartitions) {
    const mini_kafka::PartitionAssignment assignment =
            mini_kafka::assign_partitions_round_robin({"only"}, 3);
    ASSERT_EQ(assignment.size(), 1u);
    EXPECT_EQ(assignment.at("only"), (std::vector<std::uint32_t>{0, 1, 2}));
}

TEST(ConsumerGroupTest, RoundRobinMoreMembersThanPartitions) {
    const mini_kafka::PartitionAssignment assignment =
            mini_kafka::assign_partitions_round_robin({"a", "b", "c"}, 2);
    EXPECT_EQ(assignment.at("a"), (std::vector<std::uint32_t>{0}));
    EXPECT_EQ(assignment.at("b"), (std::vector<std::uint32_t>{1}));
    EXPECT_TRUE(assignment.at("c").empty());
}

TEST(ConsumerGroupTest, RoundRobinNoMembersReturnsEmpty) {
    const mini_kafka::PartitionAssignment assignment =
            mini_kafka::assign_partitions_round_robin({}, 4);
    EXPECT_TRUE(assignment.empty());
}

TEST(ConsumerGroupTest, RoundRobinRejectsZeroPartitionCount) {
    EXPECT_THROW(mini_kafka::assign_partitions_round_robin({"alice"}, 0), std::runtime_error);
}
