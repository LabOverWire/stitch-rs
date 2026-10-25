# Changelog

All notable changes to the `stitch-p2p` crate are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-10-24

Initial public release: a pure peer-to-peer state-sync engine — multi-leader,
HLC last-writer-wins, signed writes, and owner-controlled membership — over the
`mqp2p` QUIC transport.

### Added

- Membership-proof mesh bootstrap. A gated `Swarm` (`Swarm::spawn_gated`) now
  admits a peer that presents a signed invite chain rooted at the genesis owner
  (`MemberGate::local_proof` / `admits_with_proof`, `Membership::proof` /
  `verify_proof`), so two members can pair directly even when neither has synced
  the other's `_members` record and the owner is offline — breaking the
  member-to-member bootstrap deadlock. The admission decision is evaluated on a
  scratch copy of the LWW state, so it is immune to replication-log sequence
  gaps and a stale invite loses to a newer revoke already held. A peer offers
  only its own authorizing chain, never the full roster.
- Core sync engine: HLC-stamped multi-leader last-writer-wins with tombstone
  GC-floor, a per-origin replication log with in-order delivery and prefix
  reclamation, and Ed25519-signed writes verified against the author's peer id.
- Owner-controlled membership (`_members`): authorization resolved to a fixpoint
  from a genesis owner through delegated admin grants, applied as a read-time
  filter over converged state.
- Peer discovery, NAT traversal, and QUIC transport via `mqp2p` (`discovery`
  feature); an optional JSON-document `Store` facade (`store`); and a durable
  replication log on fjall (`persistence`).
