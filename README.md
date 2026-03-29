# Mini Kafka

A  non-production Kafka-inspired (or should I say Kafkaesque) messaging system in C++.

## Requirements

- CMake >= 3.16
- A C++17 compiler (clang or gcc)
- Network access on first configure (CMake fetches GoogleTest)
- `clang-format` (optional, for formatting)

## Build

```bash
cmake -S . -B build
cmake --build build
```

## For LSP (After you add/remove sources or change CMake)

```bash
ln -sf build/compile_commands.json .
```

## Test

```bash
ctest --test-dir build --output-on-failure
```

## Cluster demo

```bash
./scripts/cluster_demo.sh
```

Starts a leader (9092) and follower (9093), produces on the leader, reads from the follower, and checks that produce on the follower fails.

## Basic TCP Demo

Start the broker with a data directory and an `events` topic (4 partitions).
The broker always has a `default` topic (1 partition) as well.

```bash
./build/src/mini_kafka_broker /tmp/mini_kafka_data 9092 events 4
```

Produce records (partition is chosen from the key):

```bash
./build/src/mini_kafka_produce 127.0.0.1 9092 events alice hello
./build/src/mini_kafka_produce 127.0.0.1 9092 events bob hi
```

Consume partition 0 from the beginning (repeat for other partitions as needed):

```bash
./build/src/mini_kafka_consume 127.0.0.1 9092 events 0
```

`mini_kafka_consume` prints one record per line as `key<TAB>value`.

### Consumer groups

```bash
# Join (use - for broker-assigned member id). Prints member_id, then assigned partitions.
./build/src/mini_kafka_group join 127.0.0.1 9092 my-group alice events

# Read from committed offset (0 if never committed); same key<TAB>value lines as consume.
./build/src/mini_kafka_group consume 127.0.0.1 9092 my-group events 0

# Commit next offset to read (e.g. after reading 2 records, commit 2).
./build/src/mini_kafka_group commit 127.0.0.1 9092 my-group events 0 2

./build/src/mini_kafka_group leave 127.0.0.1 9092 my-group alice
```

Re-join after another member joins to refresh partition assignment. Offsets and membership are in-memory; broker restart clears them.

### Broker roles (leader / follower)

```bash
# Leader (default).
./build/src/mini_kafka_broker /tmp/mini_kafka_data 9092 events 4

# Follower: own data dir and port, points at the leader.
./build/src/mini_kafka_broker /tmp/mini_kafka_follower 9093 --follower 127.0.0.1 9092 events 4
```

On startup, stderr logs `role=leader` or `role=follower` and the leader endpoint.

Followers run a one-shot sync from the leader before serving (same topic list on both brokers). Produce is rejected on followers. Consume reads the follower’s local copy.

Leaders accept an internal `ReplicaFetch` (topic, partition, `from_offset`) for replication; followers reject it.

### Manual promotion (failover)

There is no automatic failover. To treat a follower as the new leader after you believe the old leader is gone:

1. Run the follower with `--follower` so it syncs from the leader.
2. Stop the old leader process (or accept that it may still be running; see below).
3. Start the **same follower data directory** as a leader with `--promote`:

```bash
./build/src/mini_kafka_broker /tmp/mini_kafka_follower 9093 --promote events 4
```

Stderr logs `role=leader (manually promoted)`. New produces go to this broker; it no longer syncs from the old leader.

### Failure semantics (learning notes)

| Situation | What happens |
|-----------|----------------|
| Leader down, follower not promoted | Follower serves stale reads; produce fails on follower. |
| Promoted follower, old leader still up | **Split brain**: two leaders can accept writes with no coordination. Data diverges. |
| Promote without syncing first | Promoted broker may be missing records that only exist on the old leader. |
| No quorum / consensus | Promotion is a manual operator choice only; the cluster does not vote or fence the old leader. |
| Follower lag | Consume on a follower can be behind the leader until the next restart sync. |

Real Kafka uses controller election, ISR, and epoch fencing to avoid split brain. This project skips all of that on purpose.

Log segments are stored under the data directory, one directory per topic partition,
for example `/tmp/mini_kafka_data/events-p0/00000000000000000000.seg`.
A sparse index (`sparse.idx`) maps logical offsets to segment file positions.
On startup the broker truncates any partial record at the end of the active segment and rebuilds the index.

## Durability

after a clean restart, you can read back every complete record that was on disk, incomplete tails from crashes are dropped.

### Flush policy

`SegmentedLog` takes an optional `FlushPolicy` (default: `Flush`):

| Policy | On each append | Typical use |
|--------|----------------|---------------|
| `Buffered` | nothing | benchmarks; data may sit in the page cache |
| `Flush` | `ostream::flush` to the OS | broker default; good for demos |
| `Fsync` | flush + `fsync` the segment file | strongest single-node durability; slower |

The broker opens partition logs with `Flush` by default. Override at startup:

```bash
./build/src/mini_kafka_broker /tmp/mini_kafka_data 9092 --flush fsync events 4
```

Stderr logs `flush=buffered`, `flush=flush`, or `flush=fsync`. Tests and library callers can also pass `FlushPolicy` to `SegmentedLog` directly.

`Flush` pushes bytes out of the C++ stream but does not force the kernel to write them to disk. Only `Fsync` helps survive a power loss or kernel panic. Even with `Fsync`, a crash can still lose the last append if it never finished writing.

### Partial-write recovery

Records are length-prefixed with a CRC. On open, the log scans from the start and keeps a prefix of valid, complete records:

- **`SegmentedLog`**: on startup, truncates the active (last) segment file to the last valid byte, then rebuilds `sparse.idx`.

Garbage after a complete record (half-written record, random bytes, bad CRC) is removed. Appends then continue after the last good offset. Older sealed segments are not re-scanned on every open. only the tail segment is repaired.

## Tools

Read a partition log from disk (no broker needed). Same `key<TAB>value` lines as consume:

```bash
./build/src/mini_kafka_log_dump /tmp/mini_kafka_data events 0
```

Local append/read benchmark (disk only, not TCP):

```bash
./build/src/mini_kafka_bench
./build/src/mini_kafka_bench 10000
```

The broker logs each request and a summary on exit to stderr:

```
[broker] produce topic=events
[broker] consume topic=events partition=0 records=2
[broker] join_group group=my-group member=alice partitions=2
[broker] group_consume group=my-group topic=events partition=0 from_offset=0 records=2
[broker] offset_commit group=my-group topic=events partition=0 offset=2
[broker] metrics produce=2 consume=1 errors=0
```

## Performance notes

- **Throughput vs durability**: `Buffered` is fastest; `Flush` is the broker default; `Fsync` is slowest but safest on one machine. Run `mini_kafka_bench` to compare on your hardware.
- **Reads**: `read_all` on a warm log is usually much faster than append with `Fsync`.
- **Broker**: four worker threads handle clients; each partition has its own lock, so different partitions can append in parallel.
- **Numbers**: bench results are rough and machine-specific. They do not include network or replication.
- **Not a load test**: no sustained multi-client TCP benchmark yet; use the local bench for log I/O only.

## Format

```bash
clang-format -i $(find src include tests -name '*.cpp' -o -name '*.h')
```

## Layout

```
include/mini_kafka/   public headers
src/                  library, broker, CLIs, bench, log_dump
tests/                unit tests (GoogleTest)
```

