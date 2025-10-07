#include "socket_util.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <utility>

#include "mini_kafka/protocol.h"

namespace mini_kafka {

namespace {

constexpr uint32_t kMaxFrameSize = 64u * 1024u * 1024u;

uint32_t read_u32_le(const uint8_t* bytes) {
    return static_cast<uint32_t>(bytes[0]) | (static_cast<uint32_t>(bytes[1]) << 8) |
           (static_cast<uint32_t>(bytes[2]) << 16) | (static_cast<uint32_t>(bytes[3]) << 24);
}

std::runtime_error socket_error(const std::string& message) {
    return std::runtime_error(message + ": " + std::strerror(errno));
}

}  // namespace

SocketHandle::SocketHandle(int fd) : fd_(fd) {
}

SocketHandle::~SocketHandle() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

SocketHandle::SocketHandle(SocketHandle&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

SocketHandle& SocketHandle::operator=(SocketHandle&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    if (fd_ >= 0) {
        ::close(fd_);
    }
    fd_ = other.fd_;
    other.fd_ = -1;
    return *this;
}

int SocketHandle::get() const {
    return fd_;
}

int SocketHandle::release() {
    const int fd = fd_;
    fd_ = -1;
    return fd;
}

int create_listen_socket(uint16_t requested_port, uint16_t* actual_port) {
    SocketHandle listen_fd(::socket(AF_INET, SOCK_STREAM, 0));
    if (listen_fd.get() < 0) {
        throw socket_error("socket");
    }

    int reuse_addr = 1;
    if (::setsockopt(listen_fd.get(), SOL_SOCKET, SO_REUSEADDR, &reuse_addr, sizeof(reuse_addr)) !=
        0) {
        throw socket_error("setsockopt");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(requested_port);

    if (::bind(listen_fd.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
        throw socket_error("bind");
    }
    if (::listen(listen_fd.get(), 16) != 0) {
        throw socket_error("listen");
    }

    sockaddr_in bound_addr{};
    socklen_t bound_addr_size = sizeof(bound_addr);
    if (::getsockname(
            listen_fd.get(),
            reinterpret_cast<sockaddr*>(&bound_addr),
            &bound_addr_size) != 0) {
        throw socket_error("getsockname");
    }
    *actual_port = ntohs(bound_addr.sin_port);

    return listen_fd.release();
}

int accept_client(int listen_fd) {
    int client_fd = ::accept(listen_fd, nullptr, nullptr);
    if (client_fd < 0) {
        throw socket_error("accept");
    }
    return client_fd;
}

int connect_to_server(const std::string& host, uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* result = nullptr;
    const std::string service = std::to_string(port);
    const int rc = ::getaddrinfo(host.c_str(), service.c_str(), &hints, &result);
    if (rc != 0) {
        throw std::runtime_error(std::string("getaddrinfo: ") + gai_strerror(rc));
    }

    addrinfo* current = result;
    while (current != nullptr) {
        int fd = ::socket(current->ai_family, current->ai_socktype, current->ai_protocol);
        if (fd >= 0) {
            if (::connect(fd, current->ai_addr, current->ai_addrlen) == 0) {
                ::freeaddrinfo(result);
                return fd;
            }
            ::close(fd);
        }
        current = current->ai_next;
    }

    ::freeaddrinfo(result);
    throw socket_error("connect");
}

void write_all(int fd, const uint8_t* data, std::size_t size) {
    std::size_t written = 0;
    while (written < size) {
        const ssize_t rc = ::send(fd, data + written, size - written, 0);
        if (rc < 0) {
            throw socket_error("send");
        }
        written += static_cast<std::size_t>(rc);
    }
}

void read_exact(int fd, uint8_t* data, std::size_t size) {
    std::size_t read_bytes = 0;
    while (read_bytes < size) {
        const ssize_t rc = ::recv(fd, data + read_bytes, size - read_bytes, 0);
        if (rc == 0) {
            throw std::runtime_error("recv: connection closed");
        }
        if (rc < 0) {
            throw socket_error("recv");
        }
        read_bytes += static_cast<std::size_t>(rc);
    }
}

void write_frame(int fd, const std::vector<uint8_t>& payload) {
    const std::vector<uint8_t> frame = encode_frame(payload);
    write_all(fd, frame.data(), frame.size());
}

std::vector<uint8_t> read_frame(int fd) {
    uint8_t length_bytes[4];
    read_exact(fd, length_bytes, sizeof(length_bytes));

    const uint32_t length = read_u32_le(length_bytes);
    if (length > kMaxFrameSize) {
        throw std::runtime_error("protocol: frame too large");
    }

    std::vector<uint8_t> payload(length);
    if (!payload.empty()) {
        read_exact(fd, payload.data(), payload.size());
    }
    return payload;
}

}  // namespace mini_kafka
