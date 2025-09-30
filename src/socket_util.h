#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace mini_kafka {

class SocketHandle {
public:
    explicit SocketHandle(int fd = -1);
    ~SocketHandle();

    SocketHandle(const SocketHandle&) = delete;
    SocketHandle& operator=(const SocketHandle&) = delete;
    SocketHandle(SocketHandle&& other) noexcept;
    SocketHandle& operator=(SocketHandle&& other) noexcept;

    int get() const;
    int release();

private:
    int fd_;
};

int create_listen_socket(uint16_t requested_port, uint16_t* actual_port);
int accept_client(int listen_fd);
int connect_to_server(const std::string& host, uint16_t port);

void write_all(int fd, const uint8_t* data, std::size_t size);
void read_exact(int fd, uint8_t* data, std::size_t size);

void write_frame(int fd, const std::vector<uint8_t>& payload);
std::vector<uint8_t> read_frame(int fd);

}  // namespace mini_kafka
