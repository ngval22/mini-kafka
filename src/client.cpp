#include "mini_kafka/client.h"

#include <stdexcept>

#include "mini_kafka/protocol.h"
#include "socket_util.h"

namespace mini_kafka {

namespace {

void throw_if_error_response(const std::vector<uint8_t>& payload) {
    if (!payload.empty() &&
        payload[0] == static_cast<uint8_t>(ResponseType::Error)) {
        throw std::runtime_error(decode_error_response(payload));
    }
}

}  // namespace

void produce(const std::string& host, uint16_t port, const Record& record) {
    SocketHandle socket(connect_to_server(host, port));

    write_frame(socket.get(), encode_produce_request(record));
    const std::vector<uint8_t> response = read_frame(socket.get());
    throw_if_error_response(response);
    decode_produce_ok_response(response);
}

std::vector<Record> consume_all(const std::string& host, uint16_t port) {
    SocketHandle socket(connect_to_server(host, port));

    write_frame(socket.get(), encode_consume_request());
    const std::vector<uint8_t> response = read_frame(socket.get());
    throw_if_error_response(response);
    return decode_consume_response(response);
}

}  // namespace mini_kafka
