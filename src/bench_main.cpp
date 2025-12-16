#include "mini_kafka/flush_policy.h"
#include "mini_kafka/record.h"
#include "mini_kafka/segmented_log.h"

#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>

namespace fs = std::filesystem;

namespace {

using Clock = std::chrono::steady_clock;

struct TimedRun {
    double seconds = 0.0;
    std::size_t count = 0;
};

mini_kafka::Record make_record(std::size_t value_bytes) {
    mini_kafka::Record record;
    record.key = {'k'};
    record.value.assign(value_bytes, 'x');
    return record;
}

void remove_dir(const fs::path& dir) {
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TimedRun bench_append(const fs::path& dir, mini_kafka::FlushPolicy policy,
                      const mini_kafka::Record& record, std::size_t count) {
    remove_dir(dir);
    mini_kafka::SegmentedLog log(dir.string(), 1024 * 1024, 4, policy);

    const Clock::time_point start = Clock::now();
    for (std::size_t i = 0; i < count; ++i) {
        log.append(record);
    }
    const Clock::time_point end = Clock::now();

    TimedRun result;
    result.count = count;
    result.seconds =
            std::chrono::duration<double>(end - start).count();
    return result;
}

TimedRun bench_read(const fs::path& dir, std::size_t expected_count) {
    const Clock::time_point start = Clock::now();
    const mini_kafka::SegmentedLog log(dir.string());
    const std::vector<mini_kafka::Record> records = log.read_all();
    const Clock::time_point end = Clock::now();

    if (records.size() != expected_count) {
        throw std::runtime_error("bench: read count mismatch");
    }

    TimedRun result;
    result.count = expected_count;
    result.seconds =
            std::chrono::duration<double>(end - start).count();
    return result;
}

void print_rate(const char* label, const TimedRun& run) {
    const double records_per_sec = run.count / run.seconds;
    std::cout << "  " << std::left << std::setw(9) << label << std::right << std::setw(12)
              << std::fixed << std::setprecision(0) << records_per_sec << " records/s"
              << "  (" << std::setprecision(3) << run.seconds << " s)\n";
}

std::size_t parse_record_count(const char* text) {
    const unsigned long value = std::stoul(text);
    if (value == 0) {
        throw std::runtime_error("record count must be > 0");
    }
    return static_cast<std::size_t>(value);
}

}  // namespace

int main(int argc, char** argv) {
    std::size_t record_count = 10'000;
    if (argc > 2) {
        std::cerr << "usage: mini_kafka_bench [record_count]\n";
        return 1;
    }
    if (argc == 2) {
        try {
            record_count = parse_record_count(argv[1]);
        } catch (const std::exception& ex) {
            std::cerr << ex.what() << "\n";
            return 1;
        }
    }

    try {
        constexpr std::size_t k_value_bytes = 64;
        const mini_kafka::Record record = make_record(k_value_bytes);

        const fs::path base =
                fs::temp_directory_path() /
                ("mini_kafka_bench_" + std::to_string(reinterpret_cast<uintptr_t>(&record_count)));

        std::cout << "records=" << record_count << " value_bytes=" << k_value_bytes << "\n\n";

        std::cout << "append_throughput:\n";
        const fs::path buffered_dir = base / "append_buffered";
        const fs::path flush_dir = base / "append_flush";
        const fs::path fsync_dir = base / "append_fsync";

        print_rate("Buffered", bench_append(buffered_dir, mini_kafka::FlushPolicy::Buffered, record,
                                            record_count));
        print_rate("Flush", bench_append(flush_dir, mini_kafka::FlushPolicy::Flush, record,
                                         record_count));
        print_rate("Fsync", bench_append(fsync_dir, mini_kafka::FlushPolicy::Fsync, record,
                                         record_count));

        std::cout << "\nread_throughput (reopen after Buffered append):\n";
        print_rate("read_all", bench_read(buffered_dir, record_count));

        remove_dir(base);
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << "\n";
        return 1;
    }
}
