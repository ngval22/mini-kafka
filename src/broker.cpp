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
    close_fd(listen_fd_);
}

uint16_t Broker::port() const {
    return port_;
}

void Broker::add_topic(TopicMetadata topic) {
    store_.add_topic(std::move(topic));
}

void Broker::serve_forever() {
    while (true) {
        serve_one();
    }
}

void Broker::serve_n(std::size_t max_connections) {
    for (std::size_t i = 0; i < max_connections; ++i) {
        serve_one();
    }
}

void Broker::serve_one() {
    SocketHandle client(accept_client(listen_fd_));

    try {
        const std::vector<uint8_t> request = read_frame(client.get());
        const std::vector<uint8_t> response = handle_request(store_, request);
        write_frame(client.get(), response);
    } catch (const std::exception& ex) {
        write_frame(client.get(), encode_error_response(ex.what()));
    }
}

}  // namespace mini_kafka
