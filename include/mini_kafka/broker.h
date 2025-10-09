#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "mini_kafka/log.h"

namespace mini_kafka {

class Broker {
public:
    Broker(std::string log_path, uint16_t port);
    ~Broker();

    Broker(const Broker&) = delete;
    Broker& operator=(const Broker&) = delete;
    Broker(Broker&&) = delete;
    Broker& operator=(Broker&&) = delete;

    uint16_t port() const;

    void serve_forever();
    void serve_n(std::size_t max_connections);

private:
    void serve_one();

    Log log_;
    int listen_fd_;
    uint16_t port_;
};

}  // namespace mini_kafka
