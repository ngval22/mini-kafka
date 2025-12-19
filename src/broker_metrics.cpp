#include "mini_kafka/broker_metrics.h"

namespace mini_kafka {

void BrokerMetrics::on_produce() {
    produce_count_.fetch_add(1, std::memory_order_relaxed);
}

void BrokerMetrics::on_consume() {
    consume_count_.fetch_add(1, std::memory_order_relaxed);
}

void BrokerMetrics::on_error() {
    error_count_.fetch_add(1, std::memory_order_relaxed);
}

BrokerMetricsSnapshot BrokerMetrics::snapshot() const {
    BrokerMetricsSnapshot out;
    out.produce_count = produce_count_.load(std::memory_order_relaxed);
    out.consume_count = consume_count_.load(std::memory_order_relaxed);
    out.error_count = error_count_.load(std::memory_order_relaxed);
    return out;
}

}  // namespace mini_kafka
