# stitch (Rust workspace)

State-sync libraries for the LabOverWire stack, in two flavors plus an
exerciser. Each member crate has its own README.

| Crate | Path | What it is |
|---|---|---|
| [`stitch`](crates/stitch) | `crates/stitch` | Reactive store with **server-authoritative** sync: in-memory cache, fjall persistence, MQTT/MQDB remote sync, version-LWW. The Rust port of `@laboverwire/stitch`. Compiles for both native and `wasm32` — the browser build runs the in-memory cache plus durable IndexedDB persistence (plaintext or AES-GCM encrypted) via `mqdb-wasm`; remote MQTT sync is native-only for now. |
| [`stitch-wasm`](crates/stitch-wasm) | `crates/stitch-wasm` | **Browser bindings** (`wasm-bindgen`) over `stitch`: a `createStore` factory and `Store` class for JavaScript, intended as a drop-in for the TypeScript `@laboverwire/stitch` core. Exposes the in-memory store and IndexedDB persistence (`createStore(config, { persistence: { dbName, passphrase? } })`); WebSocket MQTT sync is an upcoming milestone. |
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
