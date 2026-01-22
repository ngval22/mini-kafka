#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mini_kafka/protocol.h"
#include "mini_kafka/record.h"

namespace mini_kafka {

void produce(const std::string& host, uint16_t port, const std::string& topic,
             const Record& record);
std::vector<Record> consume_all(const std::string& host, uint16_t port, const std::string& topic,
                                std::uint32_t partition);
std::vector<Record> replica_fetch(const std::string& host, uint16_t port, const std::string& topic,
                                  std::uint32_t partition, std::uint64_t from_offset);

JoinGroupResponse join_group(const std::string& host, uint16_t port, const std::string& group_id,
                             const std::string& member_id, const std::string& topic);
void leave_group(const std::string& host, uint16_t port, const std::string& group_id,
                 const std::string& member_id);
void commit_offset(const std::string& host, uint16_t port, const std::string& group_id,
                   const std::string& topic, std::uint32_t partition, std::uint64_t offset);
std::vector<Record> group_consume(const std::string& host, uint16_t port,
                                  const std::string& group_id, const std::string& topic,
                                  std::uint32_t partition);

}  // namespace mini_kafka
