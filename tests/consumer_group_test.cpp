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
