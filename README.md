# SpacetimeDB Timestamp Collision Reproduction

This repository demonstrates that `ctx.timestamp` in SpacetimeDB reducers is **not safe to use as a unique identifier**. Two different reducer calls can have the same timestamp, and timestamps can even go backwards for sequentially executed reducers.

## The Problem

**Important:** This issue only manifests when there are **multiple concurrent connections** sending reducer calls. A single connection sending requests sequentially will not experience this problem because its requests are processed in order. However, any real-world application with multiple users will have multiple concurrent connections.

When multiple clients send reducer calls concurrently, the timestamps assigned to those calls can:

1. **Collide**: Two reducers get the exact same timestamp (same microsecond)
2. **Go backwards**: A reducer that executes *later* can have an *earlier* timestamp than one that executed before it

This happens because timestamps are assigned **before** the serialization point, not at execution time. Each connection's request gets a timestamp when it arrives, but the execution order is determined by which connection wins the race to acquire the instance lock.

## Root Cause

The issue is in how timestamps are assigned to reducer calls. Looking at SpacetimeDB commit [`e8f9079dc50aff44af71840ef17ee94cf678268a`](https://github.com/clockworklabs/SpacetimeDB/tree/e8f9079dc50aff44af71840ef17ee94cf678268a):

### 1. Timestamp assignment happens early

In [`crates/core/src/host/module_host.rs:1493-1505`](https://github.com/clockworklabs/SpacetimeDB/blob/e8f9079dc50aff44af71840ef17ee94cf678268a/crates/core/src/host/module_host.rs#L1493-L1505):

```rust
let call_reducer_params = CallReducerParams {
    timestamp: Timestamp::now(),  // <-- TIMESTAMP ASSIGNED HERE
    caller_identity,
    caller_connection_id,
    client,
    request_id,
    timer,
    reducer_id,
    args,
};

Ok(self
    .call(
        &reducer_def.name,
        (None, call_reducer_params),
        ...
    )
    .await?)  // <-- QUEUED FOR EXECUTION HERE
```

### 2. Execution is serialized later

In [`crates/core/src/host/module_host.rs:1145`](https://github.com/clockworklabs/SpacetimeDB/blob/e8f9079dc50aff44af71840ef17ee94cf678268a/crates/core/src/host/module_host.rs#L1145):

```rust
let inst = self.instance_manager.lock().await.get_instance().await;
```

The lock acquisition determines execution order, but timestamps were already assigned before reaching this point.

### 3. Race condition

Consider this sequence:

1. **Request A** arrives, calls `Timestamp::now()` → gets T=100
2. **Request B** arrives, calls `Timestamp::now()` → gets T=101
3. Both requests race to acquire `instance_manager.lock()`
4. **B wins the race**, executes first with T=101
5. **A executes second** with T=100

From the reducer's perspective, timestamp went backwards from 101 to 100.

### 4. Timestamp resolution

`Timestamp::now()` uses `SystemTime::now()` with microsecond resolution ([`crates/sats/src/timestamp.rs:20-22`](https://github.com/clockworklabs/SpacetimeDB/blob/e8f9079dc50aff44af71840ef17ee94cf678268a/crates/sats/src/timestamp.rs#L20-L22)):

```rust
pub fn now() -> Self {
    Self::from_system_time(SystemTime::now())
}
```

Two calls within the same microsecond return identical values.

## Related Issues

- [#2529: Timestamps are not monotonic for non-scheduled reducers](https://github.com/clockworklabs/SpacetimeDB/issues/2529)
- [#2618: Monotonic timestamps](https://github.com/clockworklabs/SpacetimeDB/pull/2618) - Fixed this for **scheduled reducers only**, not for client-initiated reducers

## Running the Reproduction

### Prerequisites

- SpacetimeDB CLI (`spacetime`) installed and in PATH
- A local SpacetimeDB instance running (`spacetime start`)
- Rust toolchain with `wasm32-unknown-unknown` target

### Run the test

```bash
./run_test.sh
```

This script:
1. Builds the WASM module
2. Generates Rust SDK bindings
3. Publishes the module to a local SpacetimeDB instance
4. Runs the test client which creates 10 connections sending 100 messages each (1000 total)

## Example Output

```
Creating 10 connections, each sending 100 messages (1000 total)...
Waiting for 10 connections to establish...
All connections established!
Sending 100 messages per connection...
Waiting for all callbacks...
REDUCER ERROR #1: TIMESTAMP COLLISION! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470033556 } (1768254470033556 micros), existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470033556 } (1768254470033556 micros)
REDUCER ERROR #2: TIMESTAMP COLLISION! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470037125 } (1768254470037125 micros), existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470037125 } (1768254470037125 micros)
REDUCER ERROR #3: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470067073 } (1768254470067073 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470067074 } (1768254470067074 micros)
REDUCER ERROR #4: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470068728 } (1768254470068728 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470068739 } (1768254470068739 micros)
REDUCER ERROR #5: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470068730 } (1768254470068730 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470068739 } (1768254470068739 micros)
REDUCER ERROR #6: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470069811 } (1768254470069811 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470069823 } (1768254470069823 micros)
REDUCER ERROR #7: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470070705 } (1768254470070705 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470070750 } (1768254470070750 micros)
REDUCER ERROR #8: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470070723 } (1768254470070723 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470070750 } (1768254470070750 micros)
REDUCER ERROR #9: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470070737 } (1768254470070737 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470070750 } (1768254470070750 micros)
REDUCER ERROR #10: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071844 } (1768254470071844 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071873 } (1768254470071873 micros)
REDUCER ERROR #13: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071102 } (1768254470071102 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071144 } (1768254470071144 micros)
REDUCER ERROR #12: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470070955 } (1768254470070955 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470070967 } (1768254470070967 micros)
REDUCER ERROR #11: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071119 } (1768254470071119 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071144 } (1768254470071144 micros)
REDUCER ERROR #14: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071316 } (1768254470071316 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071345 } (1768254470071345 micros)
REDUCER ERROR #15: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071310 } (1768254470071310 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071345 } (1768254470071345 micros)
REDUCER ERROR #16: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470070946 } (1768254470070946 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470070967 } (1768254470070967 micros)
REDUCER ERROR #17: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071856 } (1768254470071856 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470071873 } (1768254470071873 micros)
REDUCER ERROR #18: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073110 } (1768254470073110 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073118 } (1768254470073118 micros)
REDUCER ERROR #19: TIMESTAMP COLLISION! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073907 } (1768254470073907 micros), existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073907 } (1768254470073907 micros)
REDUCER ERROR #20: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073908 } (1768254470073908 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073948 } (1768254470073948 micros)
REDUCER ERROR #21: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073938 } (1768254470073938 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073948 } (1768254470073948 micros)
REDUCER ERROR #22: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073909 } (1768254470073909 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073948 } (1768254470073948 micros)
REDUCER ERROR #23: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073924 } (1768254470073924 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470073948 } (1768254470073948 micros)
REDUCER ERROR #24: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470075583 } (1768254470075583 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470075603 } (1768254470075603 micros)
REDUCER ERROR #25: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470076730 } (1768254470076730 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470076736 } (1768254470076736 micros)
REDUCER ERROR #26: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470076987 } (1768254470076987 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470077006 } (1768254470077006 micros)
REDUCER ERROR #27: TIMESTAMP COLLISION! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470077085 } (1768254470077085 micros), existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470077085 } (1768254470077085 micros)
REDUCER ERROR #28: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470077154 } (1768254470077154 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470077205 } (1768254470077205 micros)
REDUCER ERROR #29: TIMESTAMP COLLISION! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470077290 } (1768254470077290 micros), existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470077290 } (1768254470077290 micros)
REDUCER ERROR #30: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470077414 } (1768254470077414 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470077452 } (1768254470077452 micros)
REDUCER ERROR #31: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470078237 } (1768254470078237 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470078255 } (1768254470078255 micros)
REDUCER ERROR #32: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470078248 } (1768254470078248 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470078255 } (1768254470078255 micros)
REDUCER ERROR #33: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470078713 } (1768254470078713 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470078727 } (1768254470078727 micros)
REDUCER ERROR #34: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470079164 } (1768254470079164 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470079210 } (1768254470079210 micros)
REDUCER ERROR #35: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470079184 } (1768254470079184 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470079210 } (1768254470079210 micros)
REDUCER ERROR #36: TIMESTAMP COLLISION! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470080070 } (1768254470080070 micros), existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470080070 } (1768254470080070 micros)
REDUCER ERROR #37: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470081171 } (1768254470081171 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470081240 } (1768254470081240 micros)
REDUCER ERROR #38: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470081221 } (1768254470081221 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470081240 } (1768254470081240 micros)
REDUCER ERROR #39: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470081378 } (1768254470081378 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470081427 } (1768254470081427 micros)
REDUCER ERROR #40: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470081398 } (1768254470081398 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470081427 } (1768254470081427 micros)
REDUCER ERROR #41: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470084045 } (1768254470084045 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470084054 } (1768254470084054 micros)
REDUCER ERROR #42: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470084259 } (1768254470084259 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470084286 } (1768254470084286 micros)
REDUCER ERROR #43: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470084710 } (1768254470084710 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470084739 } (1768254470084739 micros)
REDUCER ERROR #44: TIMESTAMP WENT BACKWARDS! new_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470085850 } (1768254470085850 micros) < existing_ts=Timestamp { __timestamp_micros_since_unix_epoch__: 1768254470085876 } (1768254470085876 micros)
Done! 956 succeeded, 44 failed out of 1000 total
```

Note that:
- **6 timestamp collisions** occurred (exact same microsecond)
- **38 backwards timestamps** occurred (later reducer had earlier timestamp)
- The backwards deltas are small (tens of microseconds), confirming this is execution reordering, not clock drift
