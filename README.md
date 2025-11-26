# dm-live

**dm-live** is a high-performance, low-latency RTSP streaming server built in Rust.

It uses Tokio's `current_thread` runtime with a Thread-per-Core, multi-reactor architecture.

## Core Features

- **Thread-per-Core architecture**
  - Does not use Tokio's work-stealing mode. `RuntimePool` creates independent runtimes per CPU core, binds connections to threads to implement the multi-reactor model, removes mutex contention, and maximizes CPU cache locality.
- **Zero-copy data path**
  - Wraps data using `Arc/Rc` pointers to eliminate copies along the forwarding path and achieve zero-copy.
- **Batch I/O optimizations**
  - **UDP**: On Linux, uses `sendmmsg` for syscall-level batch sending.
  - **TCP**: Implements user-space write-queue coalescing (vectored I/O) and `writev`, greatly reducing syscall counts.

## Performance Benchmarks

We conducted stress tests in a loopback environment:

| Metric | Result |
| :--- | :--- |
| **Concurrent connections** | **20,000** |
| **Total throughput** | **~19.8 Gbps** |
| **CPU usage** | **~355%** |
| **Per-core throughput** | **~5.6 Gbps/Core** |
| **Memory per connection** | **~19 KB** |

> Detailed test report and charts: [BENCHMARK.md](docs/pull_bench.md)

## Architecture Overview

```text
       [ Runtime-A (publish thread) ]                    [ Runtime-B (pull thread) ]
+-----------------------------------+          +-----------------------------------+
|           PushSession             |          |           PullSession             |
+----------------+------------------+          +----------------+------------------+
                 | Write                                       ^ Read
                 v                                              |
+----------------+------------------+          +----------------+------------------+
|           MediaSource             |          |             Reader                |
|                                   |          +----------------+------------------+
|  +-----------------------------+  |                           ^
|  | GopRingBuffer (Thread-Local)|  |                           |
|  +-------------+---------------+  |          +----------------+------------------+
+----------------|------------------+          |           Dispatcher              |
                 |                             |      (Thread-Local Shard)         |
                 |                             +----------------+------------------+
                 |                                              ^
                 | Cross-thread notify (MPSC Channel)           |
                 +==============================================+
                 |  Payload: Arc<StorageSnapshot> (Zero-Copy)   |
                 +==============================================+
```
### Data Flow
* **Ingest and store (O(1))**: Socket data is parsed into `Arc<RtpPacket>` (zero-copy), reordered by `PacketSorter` (UDP out-of-order), aggregated by `PacketCache`, and written into `GopRingBuffer` with constant overhead.
* **Distribute and transmit (O(N/M))**: RingBuffer broadcasts immutable snapshots via Channel to each runtime's Dispatcher; the Dispatcher linearly schedules Readers to pull packets from the snapshot and `SharedTcpWriter` sends them in batches.


## Current Status

### Supported
* Protocol: RTSP 1.0 (TCP / UDP Unicast).
* Formats: H.264, AAC.
* Features: instant startup (GOP cache), low-latency forwarding.

### Not Implemented
* H.265 (HEVC)/PCM
* RTCP support
* Transcoding support
