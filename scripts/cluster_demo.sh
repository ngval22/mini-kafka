#!/usr/bin/env bash
# Repeatable local leader + follower demo
# Leader accepts writes; follower syncs on startup and serves reads.
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

require_binary mini_kafka_broker
require_binary mini_kafka_produce
require_binary mini_kafka_consume

rm -rf "${LEADER_DIR}" "${FOLLOWER_DIR}"

echo "==> starting leader on port ${LEADER_PORT}"
"${BIN}/mini_kafka_broker" "${LEADER_DIR}" "${LEADER_PORT}" "${TOPIC}" "${PARTITIONS}" &
LEADER_PID=$!
sleep 0.5

echo "==> producing on leader"
"${BIN}/mini_kafka_produce" 127.0.0.1 "${LEADER_PORT}" "${TOPIC}" alice hello
"${BIN}/mini_kafka_produce" 127.0.0.1 "${LEADER_PORT}" "${TOPIC}" bob world

echo "==> starting follower (syncs from leader on startup)"
"${BIN}/mini_kafka_broker" "${FOLLOWER_DIR}" "${FOLLOWER_PORT}" \
    --follower 127.0.0.1 "${LEADER_PORT}" "${TOPIC}" "${PARTITIONS}" &
FOLLOWER_PID=$!
sleep 0.5

echo "==> consuming partition 0 from follower"
OUTPUT="$("${BIN}/mini_kafka_consume" 127.0.0.1 "${FOLLOWER_PORT}" "${TOPIC}" 0)"
printf '%s\n' "${OUTPUT}"

if ! printf '%s\n' "${OUTPUT}" | grep -Fq $'alice\thello'; then
    echo "expected alice\\thello on follower" >&2
    exit 1
fi
if ! printf '%s\n' "${OUTPUT}" | grep -Fq $'bob\tworld'; then
    echo "expected bob\\tworld on follower" >&2
    exit 1
fi

echo "==> produce on follower should fail"
if "${BIN}/mini_kafka_produce" 127.0.0.1 "${FOLLOWER_PORT}" "${TOPIC}" x bad 2>/dev/null; then
    echo "follower accepted produce (expected error)" >&2
    exit 1
fi

echo "cluster demo ok"
