# stitch-p2p

Pure peer-to-peer state sync for [stitch](../stitch-rs) — multi-leader,
eventually-consistent replication with **no central authority**. Peers resolve
conflicts locally using last-writer-wins keyed by a Hybrid Logical Clock and a
peer-fingerprint tiebreak, giving a deterministic total order without a server.

## Usage

```rust
use std::sync::Arc;
use std::time::Duration;
use serde_json::json;
use mqp2p::{Peer, PeerConfig};
use stitch_p2p::{Store, Swarm, peer_id_from_fingerprint};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mut peer = Peer::new(PeerConfig::new("alice", "127.0.0.1:1883")).await?;
peer.register().await?;
let store = Store::new(peer_id_from_fingerprint(peer.fingerprint()).unwrap());

// Attach discovery: dials/accepts peers and syncs in the background.
let _swarm = Swarm::spawn(Arc::new(peer), store.node().clone(), Duration::from_secs(1));

store.create("task", "t1", json!({"title": "ship it", "done": false})).await?;
let mut events = store.subscribe().await;
tokio::spawn(async move {
    while let Ok(ev) = events.recv().await {
        println!("{:?} {}/{}", ev.op, ev.entity, ev.id);
    }
});
# Ok(()) }
```

`Store` is the app-facing facade — a sibling to `stitch::Store` with the same
shape (`create` / `read` / `update` / `delete` / `list` / `subscribe`) but a
multi-leader HLC engine instead of the broker-authoritative version-LWW one.
The two conflict models can't share an inbound-apply path, so this is a
separate Store rather than a mode of `stitch::Store`.

## Why a formal spec first

Multi-leader sync has subtle convergence properties. Before writing code, the
design was modeled in TLA+ and checked with TLC (`spec/`). The model found a
real **tombstone-resurrection bug** in naive garbage collection and confirmed
the fix (a per-record GC floor). The `lww` module is a direct port of the
verified model. See [`spec/README.md`](./spec/README.md) for the results.

## Status

Implemented and tested (the full verified core, transport excluded):

- `hlc` — Hybrid Logical Clock (`tick`, `observe`) and `Stamp` total order.
- `lww` — the verified merge core: LWW apply, tombstones, per-record GC floor.
- `wire` — compact binary `WriteFrame` (60-byte header + entity/id/data),
  descended from MQDB's `ReplicationWrite`, extended with HLC + peer id + seq.
- `replog` — per-origin append-only logs + cursors + in-order catchup
  (`delta_since`), mirroring the verified `truelog`/`seen`/`Sync` model.
- `sync_state` — a peer's complete state (clock + log + applier); the unit a
  peer session drives. `tests/transitive.rs` runs it through the verified
  line topology `1—2—3` and confirms transitive convergence end to end.
- `protocol` — length-prefixed `Hello`/`Delta` messages over any
  `AsyncRead + AsyncWrite`, with a message-size cap.
- `session` — symmetric per-connection driver. Periodic **pull-based
  anti-entropy** (the verified `Sync` action on a timer): send `Hello(cursors)`,
  reply to inbound `Hello` with the `Delta` they're missing, apply inbound
  deltas, and live-push local writes for low latency. Generic over the stream,
  tested over an in-memory pipe *and* a real QUIC connection
  (`tests/quic_loopback.rs`, via mqp2p's `QuicEndpoint` with fingerprint mTLS —
  no broker, no STUN).
- `node` — `SyncNode`, the fan-out layer. One shared `SyncState` across all of
  a device's sessions; `register_session` hands a session its outbound channel,
  `local_write` applies and fans out to every live session. **Transitive
  forwarding falls out of the shared state**: a write pulled from one peer is
  served to the others on their next pull — no re-broadcast, no echo
  suppression. `tests/node_mesh.rs` proves a line `A—B—C` converges through the
  hub.

- `discovery` — `Swarm`, the connection-lifecycle layer over mqp2p's `Peer`.
  An accept loop and a connect loop drive a `SyncNode`: discovered peers are
  dialed (role broken by peer-id order, so each pair forms one connection),
  each connection opens a sync stream and runs `session::run` against the shared
  state. `peer_id_from_fingerprint` ties the sync writer identity to the
  cryptographic cert fingerprint. `tests/discovery_broker.rs` runs two peers
  through a real `mqdb` broker — register, discover, NAT-traverse to QUIC, and
  converge — end to end.

- `store` — `Store`, the app-facing document facade over `SyncNode`. JSON
  records keyed by `(entity, id)`; `create`/`read`/`update` (read-merge-write)/
  `delete`/`list`/`subscribe`. `SyncState` carries a mutation event bus that
  fires on both local and peer-applied writes. `tests/store_sync.rs` shows two
  `Store`s converging on JSON documents, including a concurrent-edit conflict.

## Feature flags

| Feature | Default | Pulls in | Gates |
|---|---|---|---|
| (none) | — | `tokio`, `thiserror` | the verified core: `hlc`, `lww`, `wire`, `replog`, `sync_state`, `protocol`, `session`, `node` |
| `store` | on | `serde_json` | `stitch_p2p::store::Store` (JSON document facade) |
| `discovery` | on | `mqp2p` (→ quinn, mqtt5) | `stitch_p2p::discovery::Swarm` (peer discovery + NAT + QUIC) |

`default-features = false` builds the formally-verified engine with just
`tokio` + `thiserror` — no networking, no JSON, no transitive QUIC/MQTT stack.
Add `store` for the document API, `discovery` for the mqp2p transport.

Not yet built:

- **M3** — membership (invite/revoke), signed entries, tombstone reclamation.
- **Durable persistence** — the engine is in-memory; records don't survive
  restart.

mqp2p (discovery + NAT + QUIC) is now a runtime dependency. The
`tests/discovery_broker.rs` test requires the `mqdb` binary on PATH and skips
with a message if it's absent.

## Architecture

```
Store (stitch public API, unchanged)
  └─ PeerSyncEngine        (replaces RemoteSyncLayer)
       ├─ Applier          (lww: LWW + GC, verified)
       ├─ per-peer cursors (anti-entropy)         [M2]
       └─ PeerSession × N  (mqp2p QUIC bidi)      [M2]
```

## Dependencies

- `thiserror` — error types.
- Later: `mqp2p` (discovery + NAT traversal + QUIC), `mqdb-agent` (local
  storage), `stitch` (the `Store` facade).

All LabOverWire-owned. No central broker holds canonical state.
