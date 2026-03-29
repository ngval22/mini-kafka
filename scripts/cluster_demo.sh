#!/usr/bin/env bash
# Leader + follower demo: produce on leader, read from follower, produce on follower fails.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${ROOT}/build/src"

LEADER_DIR="/tmp/mini_kafka_cluster_demo_leader"
FOLLOWER_DIR="/tmp/mini_kafka_cluster_demo_follower"
LEADER_PORT=9092
FOLLOWER_PORT=9093
TOPIC="events"
PARTITIONS=1

LEADER_PID=""
FOLLOWER_PID=""

cleanup() {
    if [[ -n "${FOLLOWER_PID}" ]]; then
        kill "${FOLLOWER_PID}" 2>/dev/null || true
        wait "${FOLLOWER_PID}" 2>/dev/null || true
    fi
    if [[ -n "${LEADER_PID}" ]]; then
        kill "${LEADER_PID}" 2>/dev/null || true
        wait "${LEADER_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

require_binary() {
    if [[ ! -x "${BIN}/${1}" ]]; then
        echo "missing ${BIN}/${1} — build first:" >&2
        echo "  cmake -S . -B build && cmake --build build" >&2
        exit 1
    fi
}

wait_for_listen() {
    local port=$1
    local pid=$2
    for _ in $(seq 1 50); do
        if ! kill -0 "${pid}" 2>/dev/null; then
            echo "broker on port ${port} exited early" >&2
            return 1
        fi
        if (echo >/dev/tcp/127.0.0.1/"${port}") 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "timed out waiting for port ${port}" >&2
    return 1
}

require_binary mini_kafka_broker
require_binary mini_kafka_produce
require_binary mini_kafka_consume

rm -rf "${LEADER_DIR}" "${FOLLOWER_DIR}"

echo "==> starting leader on ${LEADER_PORT}"
"${BIN}/mini_kafka_broker" "${LEADER_DIR}" "${LEADER_PORT}" "${TOPIC}" "${PARTITIONS}" \
    >/dev/null 2>&1 &
LEADER_PID=$!
wait_for_listen "${LEADER_PORT}" "${LEADER_PID}"

echo "==> producing on leader"
"${BIN}/mini_kafka_produce" 127.0.0.1 "${LEADER_PORT}" "${TOPIC}" alice hello
"${BIN}/mini_kafka_produce" 127.0.0.1 "${LEADER_PORT}" "${TOPIC}" bob hi

echo "==> starting follower on ${FOLLOWER_PORT} (syncs from leader on startup)"
"${BIN}/mini_kafka_broker" "${FOLLOWER_DIR}" "${FOLLOWER_PORT}" \
    --follower 127.0.0.1 "${LEADER_PORT}" "${TOPIC}" "${PARTITIONS}" \
    >/dev/null 2>&1 &
FOLLOWER_PID=$!
wait_for_listen "${FOLLOWER_PORT}" "${FOLLOWER_PID}"

echo "==> consuming from follower (partition 0)"
OUTPUT="$("${BIN}/mini_kafka_consume" 127.0.0.1 "${FOLLOWER_PORT}" "${TOPIC}" 0)"
printf '%s\n' "${OUTPUT}"

if [[ "${OUTPUT}" != $'alice\thello\nbob\thi' ]]; then
    echo "unexpected records from follower" >&2
    exit 1
fi

echo "==> produce on follower should fail"
if "${BIN}/mini_kafka_produce" 127.0.0.1 "${FOLLOWER_PORT}" "${TOPIC}" carol nope 2>/dev/null; then
    echo "expected produce on follower to fail" >&2
    exit 1
fi

echo "cluster demo ok"
