#pragma once

#include <fstream>
#include <string>

namespace mini_kafka {

// Controls how much data is pushed to the OS (and disk) after each append.
enum class FlushPolicy {
    Buffered,  // no flush on append; fastest, least durable
    Flush,     // std::ostream::flush after each append (default)
    Fsync,     // flush then fsync the backing file
};

// Apply policy after a successful write. path is the open file's path (used for fsync).
void apply_flush_policy(std::ofstream& out, const std::string& path, FlushPolicy policy);

}  // namespace mini_kafka
