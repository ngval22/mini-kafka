#include "mini_kafka/broker.h"

#include <stdexcept>
#include <string>
#include <utility>

#include "mini_kafka/protocol.h"
#include "socket_util.h"

namespace mini_kafka {

namespace {

std::vector<uint8_t> handle_request(Log& log, const std::vector<uint8_t>& request) {
    if (request.empty()) {
        throw std::runtime_error("broker: empty request");
    }

    const uint8_t request_type = request[0];
    if (request_type == static_cast<uint8_t>(RequestType::Produce)) {
        log.append(decode_produce_request(request));
        return encode_produce_ok_response();
    }
    if (request_type == static_cast<uint8_t>(RequestType::Consume)) {
        decode_consume_request(request);
        return encode_consume_response(log.read_all());
    }
    throw std::runtime_error("broker: unknown request type");
}

}  // namespace

struct Broker::Impl {
    SocketHandle listen_fd;
};

Broker::Broker(std::string log_path, uint16_t port)
        : log_(std::move(log_path)), impl_(std::make_unique<Impl>()), port_(0) {
    impl_->listen_fd = SocketHandle(create_listen_socket(port, &port_));
}

Broker::~Broker() = default;

uint16_t Broker::port() const {
    return port_;
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
    SocketHandle client(accept_client(impl_->listen_fd.get()));

    try {
        const std::vector<uint8_t> request = read_frame(client.get());
        const std::vector<uint8_t> response = handle_request(log_, request);
        write_frame(client.get(), response);
    } catch (const std::exception& ex) {
        write_frame(client.get(), encode_error_response(ex.what()));
    }
}

}  // namespace mini_kafka
