#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mini_kafka/record.h"

namespace mini_kafka {

void produce(const std::string& host, uint16_t port, const Record& record);
std::vector<Record> consume_all(const std::string& host, uint16_t port);

}  // namespace mini_kafka
