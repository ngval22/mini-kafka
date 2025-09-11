#pragma once

#include <cstddef>
#include <cstdint>

namespace mini_kafka {

// IEEE 802.3 / zlib CRC-32 of `len` bytes starting at `data`.
uint32_t crc32(const uint8_t* data, std::size_t len);

}  // namespace mini_kafka
