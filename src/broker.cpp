#include "mini_kafka/broker.h"

#include <unistd.h>

#include <stdexcept>
#include <string>
#include <utility>

#include "mini_kafka/protocol.h"
#include "socket_util.h"

namespace mini_kafka {

namespace {

void close_fd(int fd) {
    if (fd >= 0) {
        ::close(fd);
    }
}

std::vector<uint8_t> handle_request(PartitionLogStore& store, const std::vector<uint8_t>& request) {
    if (request.empty()) {
        throw std::runtime_error("broker: empty request");
    }

    const uint8_t request_type = request[0];
    if (request_type == static_cast<uint8_t>(RequestType::Produce)) {
        const ProduceRequest produce = decode_produce_request(request);
        store.append_by_key(produce.topic, produce.record);
        return encode_produce_ok_response();
    }
    if (request_type == static_cast<uint8_t>(RequestType::Consume)) {
        const ConsumeRequest consume = decode_consume_request(request);
        return encode_consume_response(
                store.read_all(consume.topic, consume.partition));
    }
    throw std::runtime_error("broker: unknown request type");
}

}  // namespace

Broker::Broker(std::string data_dir, uint16_t port)
        : store_(std::move(data_dir)), listen_fd_(-1), port_(0) {
    add_topic(make_topic_metadata("default", 1));
    listen_fd_ = create_listen_socket(port, &port_);
}

Broker::~Broker() {
    stop_workers();
    close_fd(listen_fd_);
}

uint16_t Broker::port() const {
    return port_;
}

void Broker::add_topic(TopicMetadata topic) {
    store_.add_topic(std::move(topic));
}

void Broker::start_workers() {
    if (workers_running_) {
        return;
    }
    stopping_ = false;
    workers_running_ = true;
    workers_.reserve(k_worker_count);
    for (std::size_t i = 0; i < k_worker_count; ++i) {
        workers_.emplace_back(&Broker::worker_loop, this);
    }
}

void Broker::stop_workers() {
    if (!workers_running_) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stopping_ = true;
    }
    queue_cv_.notify_all();
    for (std::thread& worker : workers_) {
        worker.join();
    }
    workers_.clear();
    workers_running_ = false;
    stopping_ = false;
}

void Broker::enqueue_client(int client_fd) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        client_queue_.push(client_fd);
    }
    queue_cv_.notify_one();
}

void Broker::on_client_handled() {
    if (!track_job_completion_) {
        return;
    }
    std::lock_guard<std::mutex> lock(completion_mutex_);
    if (jobs_remaining_ == 0) {
        return;
    }
    --jobs_remaining_;
    if (jobs_remaining_ == 0) {
        completion_cv_.notify_one();
    }
}

void Broker::worker_loop() {
    while (true) {
        int client_fd = -1;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [&] { return stopping_ || !client_queue_.empty(); });
            if (stopping_ && client_queue_.empty()) {
                return;
            }
            client_fd = client_queue_.front();
            client_queue_.pop();
        }
        handle_client(client_fd);
        on_client_handled();
    }
}

void Broker::handle_client(int client_fd) {
    SocketHandle client(client_fd);
    try {
        const std::vector<uint8_t> request = read_frame(client.get());
        const std::vector<uint8_t> response = handle_request(store_, request);
        write_frame(client.get(), response);
    } catch (const std::exception& ex) {
        write_frame(client.get(), encode_error_response(ex.what()));
    }
}

void Broker::serve_forever() {
    start_workers();
    while (true) {
        const int client_fd = accept_client(listen_fd_);
        enqueue_client(client_fd);
    }
}

void Broker::serve_n(std::size_t max_connections) {
    start_workers();
    {
        std::lock_guard<std::mutex> lock(completion_mutex_);
        track_job_completion_ = true;
        jobs_remaining_ = max_connections;
    }

    for (std::size_t i = 0; i < max_connections; ++i) {
        const int client_fd = accept_client(listen_fd_);
        enqueue_client(client_fd);
    }

    {
        std::unique_lock<std::mutex> lock(completion_mutex_);
        completion_cv_.wait(lock, [&] { return jobs_remaining_ == 0; });
        track_job_completion_ = false;
    }
    stop_workers();
}

}  // namespace mini_kafka
