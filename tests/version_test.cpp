#include "mini_kafka/version.h"

#include <gtest/gtest.h>

TEST(VersionTest, ReturnsNonEmptyString) {
    EXPECT_FALSE(mini_kafka::version().empty());
}

TEST(VersionTest, MatchesExpectedValue) {
    EXPECT_EQ(mini_kafka::version(), "0.1.0");
}
