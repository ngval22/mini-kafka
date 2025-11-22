#include "mini_kafka/flush_policy.h"

#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>

namespace mini_kafka {

namespace {

void fsync_path(const std::string& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("flush_policy: failed to open for fsync: " + path);
    }
    if (::fsync(fd) != 0) {
        ::close(fd);
        throw std::runtime_error("flush_policy: fsync failed: " + path);
    }
    ::close(fd);
}

}  // namespace

void apply_flush_policy(std::ofstream& out, const std::string& path, const FlushPolicy policy) {
    switch (policy) {
        case FlushPolicy::Buffered:
            return;
        case FlushPolicy::Flush:
            out.flush();
            if (!out) {
                throw std::runtime_error("flush_policy: flush failed: " + path);
            }
            return;
        case FlushPolicy::Fsync:
            out.flush();
            if (!out) {
                throw std::runtime_error("flush_policy: flush failed: " + path);
            }
            fsync_path(path);
            return;
    }
}

}  // namespace mini_kafka
