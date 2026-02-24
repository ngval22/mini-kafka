#include "mini_kafka/broker.h"

#include <iostream>
#include <unistd.h>

#include <stdexcept>
#include <string>
#include <utility>

#include "mini_kafka/client.h"
#include "mini_kafka/consumer_group.h"
#include "mini_kafka/protocol.h"
#include "socket_util.h"

namespace mini_kafka {

namespace {

void close_fd(int fd) {
    if (fd >= 0) {
        ::close(fd);
    }
}

void log_produce(const std::string& topic) {
    std::cerr << "[broker] produce topic=" << topic << "\n";
}

void log_consume(const std::string& topic, std::uint32_t partition, std::size_t record_count) {
    std::cerr << "[broker] consume topic=" << topic << " partition=" << partition
              << " records=" << record_count << "\n";
}

void log_replica_fetch(const std::string& topic, std::uint32_t partition,
                       std::uint64_t from_offset, std::size_t record_count) {
    std::cerr << "[broker] replica_fetch topic=" << topic << " partition=" << partition
              << " from_offset=" << from_offset << " records=" << record_count << "\n";
}

void log_join_group(const std::string& group_id, const std::string& member_id,
                    std::size_t partition_count) {
    std::cerr << "[broker] join_group group=" << group_id << " member=" << member_id
              << " partitions=" << partition_count << "\n";
}

void log_leave_group(const std::string& group_id, const std::string& member_id) {
    std::cerr << "[broker] leave_group group=" << group_id << " member=" << member_id << "\n";
}

void log_offset_commit(const std::string& group_id, const std::string& topic,
                       std::uint32_t partition, std::uint64_t offset) {
    std::cerr << "[broker] offset_commit group=" << group_id << " topic=" << topic
              << " partition=" << partition << " offset=" << offset << "\n";
}

void log_group_consume(const std::string& group_id, const std::string& topic,
                       std::uint32_t partition, std::uint64_t from_offset,
                       std::size_t record_count) {
    std::cerr << "[broker] group_consume group=" << group_id << " topic=" << topic
              << " partition=" << partition << " from_offset=" << from_offset
              << " records=" << record_count << "\n";
}

void log_error(const char* message) {
    std::cerr << "[broker] error: " << message << "\n";
}

void log_metrics_summary(const BrokerMetricsSnapshot& metrics) {
    std::cerr << "[broker] metrics produce=" << metrics.produce_count
              << " consume=" << metrics.consume_count << " errors=" << metrics.error_count
              << "\n";
}

const TopicMetadata& topic_metadata_for(PartitionLogStore& store, const std::string& topic) {
    const TopicMetadata* meta = store.topics().find(topic);
    if (meta == nullptr) {
        throw std::runtime_error("broker: unknown topic: " + topic);
    }
    return *meta;
}

std::vector<uint8_t> handle_request(PartitionLogStore& store, ConsumerGroupRegistry& groups,
                                    CommittedOffsetStore& offsets, BrokerMetrics& metrics,
                                    BrokerRole role, const std::vector<uint8_t>& request) {
    if (request.empty()) {
        throw std::runtime_error("broker: empty request");
    }

    const uint8_t request_type = request[0];
    if (request_type == static_cast<uint8_t>(RequestType::Produce)) {
        if (role == BrokerRole::Follower) {
            throw std::runtime_error("broker: produce not allowed on follower; use the leader");
        }
        const ProduceRequest produce = decode_produce_request(request);
        store.append_by_key(produce.topic, produce.record);
        metrics.on_produce();
        log_produce(produce.topic);
        return encode_produce_ok_response();
    }
    if (request_type == static_cast<uint8_t>(RequestType::Consume)) {
        const ConsumeRequest consume = decode_consume_request(request);
        const std::vector<Record> records =
                store.read_all(consume.topic, consume.partition);
        metrics.on_consume();
        log_consume(consume.topic, consume.partition, records.size());
        return encode_consume_response(records);
    }
    if (request_type == static_cast<uint8_t>(RequestType::ReplicaFetch)) {
        if (role != BrokerRole::Leader) {
            throw std::runtime_error("broker: replica fetch only supported on leader");
        }
        const ReplicaFetchRequest fetch = decode_replica_fetch_request(request);
        const std::vector<Record> records =
                store.read_from(fetch.topic, fetch.partition, fetch.from_offset);
        log_replica_fetch(fetch.topic, fetch.partition, fetch.from_offset, records.size());
        return encode_consume_response(records);
    }
    if (request_type == static_cast<uint8_t>(RequestType::JoinGroup)) {
        const JoinGroupRequest join = decode_join_group_request(request);
        const std::string member_id = groups.join(join.group_id, join.member_id);
        const TopicMetadata& topic = topic_metadata_for(store, join.topic);
        const PartitionAssignment assignment = assign_partitions_round_robin(
                groups.members(join.group_id), topic.partition_count);
        JoinGroupResponse response;
        response.member_id = member_id;
        response.partitions = assignment.at(member_id);
        log_join_group(join.group_id, member_id, response.partitions.size());
        return encode_join_group_response(response);
    }
    if (request_type == static_cast<uint8_t>(RequestType::LeaveGroup)) {
        const LeaveGroupRequest leave = decode_leave_group_request(request);
        groups.leave(leave.group_id, leave.member_id);
        log_leave_group(leave.group_id, leave.member_id);
        return encode_leave_group_ok_response();
    }
    if (request_type == static_cast<uint8_t>(RequestType::OffsetCommit)) {
        const OffsetCommitRequest commit = decode_offset_commit_request(request);
        offsets.commit(commit.group_id, commit.topic, commit.partition, commit.offset);
        log_offset_commit(commit.group_id, commit.topic, commit.partition, commit.offset);
        return encode_offset_commit_ok_response();
    }
    if (request_type == static_cast<uint8_t>(RequestType::GroupConsume)) {
        const GroupConsumeRequest consume = decode_group_consume_request(request);
        const std::uint64_t from_offset =
                offsets.committed_offset(consume.group_id, consume.topic, consume.partition);
        const std::vector<Record> records =
                store.read_from(consume.topic, consume.partition, from_offset);
        metrics.on_consume();
        log_group_consume(consume.group_id, consume.topic, consume.partition, from_offset,
                          records.size());
        return encode_consume_response(records);
    }
    throw std::runtime_error("broker: unknown request type");
}

}  // namespace

Broker::Broker(std::string data_dir, uint16_t port)
        : Broker(BrokerOptions{std::move(data_dir), port, BrokerRole::Leader, std::string(), 0}) {}

Broker::Broker(BrokerOptions options)
        : store_(std::move(options.data_dir)),
          role_(options.role),
          leader_host_(std::move(options.leader_host)),
          leader_port_(options.leader_port),
          listen_fd_(-1),
          port_(0) {
    if (options.promoted && role_ == BrokerRole::Follower) {
        throw std::runtime_error("broker: --promote cannot be used with --follower");
    }
    if (role_ == BrokerRole::Follower) {
        if (leader_host_.empty() || leader_port_ == 0) {
            throw std::runtime_error(
                    "follower broker requires non-empty leader_host and non-zero leader_port");
        }
    }

    add_topic(make_topic_metadata("default", 1));
    listen_fd_ = create_listen_socket(options.port, &port_);

    if (role_ == BrokerRole::Leader && options.promoted) {
        std::cerr << "[broker] role=leader (manually promoted)\n";
    } else if (role_ == BrokerRole::Leader) {
        std::cerr << "[broker] role=leader\n";
    } else {
        std::cerr << "[broker] role=follower leader=" << leader_host_ << ":" << leader_port_
                  << "\n";
    }
}

Broker::~Broker() {
    stop_workers();
    close_fd(listen_fd_);
    const BrokerMetricsSnapshot snapshot = metrics_.snapshot();
    if (snapshot.produce_count + snapshot.consume_count + snapshot.error_count > 0) {
        log_metrics_summary(snapshot);
    }
}

uint16_t Broker::port() const {
    return port_;
}

BrokerRole Broker::role() const {
    return role_;
}

const std::string& Broker::leader_host() const {
    return leader_host_;
}

uint16_t Broker::leader_port() const {
    return leader_port_;
}

BrokerMetricsSnapshot Broker::metrics() const {
    return metrics_.snapshot();
}

void Broker::add_topic(TopicMetadata topic) {
    store_.add_topic(std::move(topic));
}

void Broker::sync_from_leader() {
    if (role_ != BrokerRole::Follower) {
        return;
    }

    for (const TopicMetadata& topic : store_.topics().topics()) {
        for (std::uint32_t partition = 0; partition < topic.partition_count; ++partition) {
            const std::uint64_t from_offset =
                    store_.record_count(topic.name, partition);
            const std::vector<Record> records = replica_fetch(
                    leader_host_, leader_port_, topic.name, partition, from_offset);
            for (const Record& record : records) {
                store_.append(topic.name, partition, record);
            }
            if (!records.empty()) {
                std::cerr << "[broker] synced topic=" << topic.name << " partition=" << partition
                          << " records=" << records.size() << "\n";
            }
        }
    }
}

void Broker::sync_from_leader_if_follower() {
    if (role_ == BrokerRole::Follower) {
        sync_from_leader();
    }
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
        try {
            handle_client(client_fd);
        } catch (const std::exception& ex) {
            metrics_.on_error();
            log_error(ex.what());
        }
    }
}

void Broker::handle_client(int client_fd) {
    SocketHandle client(client_fd);
    try {
        const std::vector<uint8_t> request = read_frame(client.get());
        const std::vector<uint8_t> response =
                handle_request(store_, groups_, offsets_, metrics_, role_, request);
        write_frame(client.get(), response);
    } catch (const std::exception& ex) {
        metrics_.on_error();
        log_error(ex.what());
        try {
            write_frame(client.get(), encode_error_response(ex.what()));
        } catch (const std::exception& write_ex) {
            log_error(write_ex.what());
        }
    }
}

void Broker::serve_forever() {
    sync_from_leader_if_follower();
    start_workers();
    while (true) {
        const int client_fd = accept_client(listen_fd_);
        enqueue_client(client_fd);
    }
}

void Broker::serve_n(const std::size_t max_connections) {
    sync_from_leader_if_follower();
    for (std::size_t i = 0; i < max_connections; ++i) {
        handle_client(accept_client(listen_fd_));
    }
}

}  // namespace mini_kafka
