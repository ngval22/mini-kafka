#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "mini_kafka/partition_store.h"
#include "mini_kafka/topic.h"

namespace mini_kafka {

class Broker {
public:
    Broker(std::string data_dir, uint16_t port);
    ~Broker();

    Broker(const Broker&) = delete;
    Broker& operator=(const Broker&) = delete;
    Broker(Broker&&) = delete;
    Broker& operator=(Broker&&) = delete;

    uint16_t port() const;
    void add_topic(TopicMetadata topic);

    void serve_forever();
    void serve_n(std::size_t max_connections);

private:
    void serve_one();

    PartitionLogStore store_;
    int listen_fd_;
    uint16_t port_;
};

}  // namespace mini_kafka
