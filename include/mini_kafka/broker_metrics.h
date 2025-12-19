#pragma once

#include <atomic>
#include <cstdint>

namespace mini_kafka {

struct BrokerMetricsSnapshot {
    std::uint64_t produce_count = 0;
    std::uint64_t consume_count = 0;
    std::uint64_t error_count = 0;
};

class BrokerMetrics {
public:
    void on_produce();
    void on_consume();
    void on_error();

    BrokerMetricsSnapshot snapshot() const;

private:
    std::atomic<std::uint64_t> produce_count_{0};
    std::atomic<std::uint64_t> consume_count_{0};
    std::atomic<std::uint64_t> error_count_{0};
};

}  // namespace mini_kafka
