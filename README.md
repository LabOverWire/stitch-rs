# stitch-p2p

Pure peer-to-peer state sync for [stitch](../stitch-rs) — multi-leader,
eventually-consistent replication with **no central authority**. Peers resolve
conflicts locally using last-writer-wins keyed by a Hybrid Logical Clock and a
peer-fingerprint tiebreak, giving a deterministic total order without a server.

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

Not yet built:

- **Discovery wiring** — drive `SyncNode` from mqp2p's `Peer` (MQDB signaling +
  NAT traversal): accept/connect loops that `register_session` + spawn
  `session::run` as peers appear and disappear. The protocol and fan-out are
  done; this is connection-lifecycle glue, testable with an MQDB broker fixture.
- **`stitch::Store` integration** — swap the P2P engine in behind the existing
  facade via config.
- **M3** — membership (invite/revoke), signed entries, tombstone reclamation.

Everything from the wire frame up through the fan-out node is pure or
transport-generic and tested against the TLA+ models in `spec/`. mqp2p
(discovery + NAT + QUIC) is a dev-dependency for now; it becomes a runtime
dependency when the discovery wiring lands.

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
