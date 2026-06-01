# stitch (Rust workspace)

State-sync libraries for the LabOverWire stack, in two flavors plus an
exerciser. Each member crate has its own README.

| Crate | Path | What it is |
|---|---|---|
| [`stitch`](crates/stitch) | `crates/stitch` | Reactive store with **server-authoritative** sync: in-memory cache, fjall persistence, MQTT/MQDB remote sync, version-LWW. The Rust port of `@laboverwire/stitch`. Compiles for both native and `wasm32` — the browser build runs the full stack: in-memory cache, durable IndexedDB persistence (plaintext or AES-GCM encrypted) via `mqdb-wasm`, remote MQTT sync over WebSocket via `mqtt5-wasm` with MQTT v5 JWT enhanced-auth, and the durable offline queue (writes made while disconnected persist and replay on reconnect). |
| [`stitch-wasm`](crates/stitch-wasm) | `crates/stitch-wasm` | **Browser bindings** (`wasm-bindgen`) over `stitch`: a `createStore` factory and `Store` class for JavaScript — a drop-in for the **framework-agnostic core** of the TypeScript `@laboverwire/stitch`. `createStore(config, { persistence: { dbName, passphrase? }, remote: { url, clientId?, ticket? } })` (remote `url` is a `ws://`/`wss://` MQTT endpoint, `ticket` a JWT). The `Store` mirrors the TS core: CRUD, `list`/`listRootEntities`/`getChildCount`/`getSnapshot`/`getSnapshotAsMap`, scope ops, `subscribeToEntity`/`subscribeToScope`/`subscribeToConnectionStatus` (each returns an unsubscribe fn), connection (`connectionStatus`/`disconnect`/`reconnect`/`isReconnecting`), batch, `request`, local-state, `resetForLogout`, capability getters. React/Vue subpath bindings are a separate upcoming milestone. |
| [`stitch-p2p`](crates/stitch-p2p) | `crates/stitch-p2p` | **Pure peer-to-peer** sync engine: multi-leader, HLC last-writer-wins, signed writes, owner-controlled membership, tombstone reclamation. Protocols are TLA+-verified (`crates/stitch-p2p/spec`). |
| [`stitch-tasks`](crates/stitch-tasks) | `crates/stitch-tasks` | A collaborative task board on `stitch-p2p`, with two ways to exercise it: a chaos/soak harness that drives N peers to convergence under partitions and membership churn, and a narrated multi-process `demo` that syncs three real peers over a broker + QUIC. |

`stitch` and `stitch-p2p` are siblings, not layers: they share the `Store`
API shape but have incompatible conflict models (version-LWW vs. HLC-LWW), so
neither depends on the other. `stitch-tasks` depends on `stitch-p2p`.

External dependencies (`mqp2p`, the MQDB crates, `mqtt5`) live in their own
repos and are referenced as path dependencies, not workspace members.

## Build

```
cargo check --workspace
cargo clippy --workspace --all-targets
cargo test --workspace          # broker-backed integration tests need a running mqdb
cargo test -p stitch-tasks      # self-contained chaos/soak
cargo run -p stitch-tasks --bin demo   # narrated 3-peer sync over broker + QUIC (needs mqdb)

# browser build of the store
cargo check -p stitch --target wasm32-unknown-unknown
wasm-pack test --headless --chrome crates/stitch-wasm   # in-browser smoke test
```

Browser remote sync (manual end-to-end): run an `mqtt5` broker with a WebSocket
listener, then point a browser store at it —
`createStore(config, { remote: { url: "ws://localhost:<ws-port>" } })`,
`await store.initialize()` — and two tabs on the same scope converge live. The
headless `wasm-pack` suite cannot host a broker, so it only asserts the wasm
client builds and fails gracefully when the broker is unreachable; the native
broker-backed `tests/wire.rs` covers the sync engine itself.
