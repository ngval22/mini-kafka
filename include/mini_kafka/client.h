#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mini_kafka/record.h"

namespace mini_kafka {

void produce(const std::string& host, uint16_t port, const std::string& topic,
             const Record& record);
std::vector<Record> consume_all(const std::string& host, uint16_t port, const std::string& topic,
                                std::uint32_t partition);
std::vector<Record> replica_fetch(const std::string& host, uint16_t port, const std::string& topic,
                                  std::uint32_t partition, std::uint64_t from_offset);

}  // namespace mini_kafka
