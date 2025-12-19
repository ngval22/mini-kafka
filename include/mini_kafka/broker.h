#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "mini_kafka/broker_metrics.h"
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
    BrokerMetricsSnapshot metrics() const;
    void add_topic(TopicMetadata topic);

    void serve_forever();
    void serve_n(std::size_t max_connections);

private:
    static constexpr std::size_t k_worker_count = 4;

    void handle_client(int client_fd);
    void worker_loop();
    void start_workers();
    void stop_workers();
    void enqueue_client(int client_fd);
    void on_client_handled();

    PartitionLogStore store_;
    BrokerMetrics metrics_;
    int listen_fd_;
    uint16_t port_;

    std::vector<std::thread> workers_;
    std::queue<int> client_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    bool workers_running_ = false;
    bool stopping_ = false;

    std::mutex completion_mutex_;
    std::condition_variable completion_cv_;
    bool track_job_completion_ = false;
    std::size_t jobs_remaining_ = 0;
};

}  // namespace mini_kafka
