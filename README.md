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

Not yet built:

- **M2 transport** — wrap mqp2p QUIC bidi streams to carry `WriteFrame`s and
  cursor exchanges; session manager that opens/closes as peers come and go.
- **M3** — membership (invite/revoke), signed entries, tombstone reclamation.

Apps will keep the existing `stitch::Store` API; the P2P engine swaps in behind
it via config. Everything above the transport is pure and unit-tested against
the TLA+ models in `spec/`.

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
