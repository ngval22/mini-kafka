#include "mini_kafka/record.h"

#include <sstream>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

namespace {

mini_kafka::Record make_record(const std::string& key, const std::string& value) {
    mini_kafka::Record r;
    r.key.assign(key.begin(), key.end());
    r.value.assign(value.begin(), value.end());
    return r;
}

}  // namespace

TEST(RecordTest, RoundTripBasic) {
    mini_kafka::Record original = make_record("hello", "world");

    std::stringstream buf(std::ios::in | std::ios::out | std::ios::binary);
    mini_kafka::write_record(buf, original);

    auto roundtripped = mini_kafka::read_record(buf);
    ASSERT_TRUE(roundtripped.has_value());
    EXPECT_EQ(*roundtripped, original);
}

TEST(RecordTest, RoundTripEmptyKey) {
    mini_kafka::Record original = make_record("", "value-only");

    std::stringstream buf(std::ios::in | std::ios::out | std::ios::binary);
    mini_kafka::write_record(buf, original);

    auto roundtripped = mini_kafka::read_record(buf);
    ASSERT_TRUE(roundtripped.has_value());
    EXPECT_EQ(*roundtripped, original);
}

TEST(RecordTest, RoundTripEmptyValue) {
    mini_kafka::Record original = make_record("key-only", "");

    std::stringstream buf(std::ios::in | std::ios::out | std::ios::binary);
    mini_kafka::write_record(buf, original);

    auto roundtripped = mini_kafka::read_record(buf);
    ASSERT_TRUE(roundtripped.has_value());
    EXPECT_EQ(*roundtripped, original);
}

TEST(RecordTest, ReadingEmptyStreamReturnsNullopt) {
    std::stringstream buf(std::ios::in | std::ios::out | std::ios::binary);
    auto r = mini_kafka::read_record(buf);
    EXPECT_FALSE(r.has_value());
}

TEST(RecordTest, MultipleRecordsInSequence) {
    std::stringstream buf(std::ios::in | std::ios::out | std::ios::binary);
    mini_kafka::Record a = make_record("k1", "v1");
    mini_kafka::Record b = make_record("k2", "v2-longer");
    mini_kafka::Record c = make_record("", "");
    mini_kafka::write_record(buf, a);
    mini_kafka::write_record(buf, b);
    mini_kafka::write_record(buf, c);

    auto ra = mini_kafka::read_record(buf);
    auto rb = mini_kafka::read_record(buf);
    auto rc = mini_kafka::read_record(buf);
    auto rd = mini_kafka::read_record(buf);

    ASSERT_TRUE(ra.has_value());
    ASSERT_TRUE(rb.has_value());
    ASSERT_TRUE(rc.has_value());
    EXPECT_FALSE(rd.has_value());
    EXPECT_EQ(*ra, a);
    EXPECT_EQ(*rb, b);
    EXPECT_EQ(*rc, c);
}

TEST(RecordTest, CorruptedPayloadDetectedByCrc) {
    mini_kafka::Record original = make_record("hello", "world");
    std::stringstream buf(std::ios::in | std::ios::out | std::ios::binary);
    mini_kafka::write_record(buf, original);

    std::string bytes = buf.str();
    // Flip a byte inside the value region (well past the header).
    // Header is 16 bytes: length(4) + crc(4) + key_size(4) + value_size(4),
    // then 5 bytes of key ("hello"), then 5 bytes of value ("world").
    // Position 20 is inside "hello"; flip one bit there.
    bytes[20] ^= 0x01;

    std::stringstream corrupted(bytes, std::ios::in | std::ios::out | std::ios::binary);
    EXPECT_THROW(mini_kafka::read_record(corrupted), std::runtime_error);
}

TEST(RecordTest, TruncatedHeaderThrows) {
    std::string only_two_bytes("\x05\x00", 2);
    std::stringstream buf(only_two_bytes, std::ios::in | std::ios::out | std::ios::binary);
    EXPECT_THROW(mini_kafka::read_record(buf), std::runtime_error);
}
