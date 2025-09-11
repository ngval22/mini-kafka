#include "mini_kafka/crc32.h"

#include <cstring>
#include <string>

#include <gtest/gtest.h>

namespace {

uint32_t crc_of_string(const std::string& s) {
    return mini_kafka::crc32(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

}  // namespace

TEST(Crc32Test, EmptyInputIsZero) {
    EXPECT_EQ(mini_kafka::crc32(nullptr, 0), 0u);
}

TEST(Crc32Test, KnownVectorCheck) {
    // Standard IEEE CRC-32 check value for "123456789".
    EXPECT_EQ(crc_of_string("123456789"), 0xCBF43926u);
}

TEST(Crc32Test, DetectsSingleBitFlip) {
    const std::string a = "hello world";
    std::string b = a;
    b[0] ^= 0x01;
    EXPECT_NE(crc_of_string(a), crc_of_string(b));
}
