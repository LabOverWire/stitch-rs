# Changelog

All notable changes to the `mqp2p` crate are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- The advertised server-reflexive address is now re-discovered mid-session. STUN
  previously ran only once at registration, so a NAT mapping that rebound while a
  peer was running left a stale WAN address in the peer record with nothing to
  correct it. The presence heartbeat now re-runs STUN over the live QUIC socket
  (via a transparent demultiplexer that peels binding responses off before quinn
  sees them, so discovery shares the exact QUIC NAT mapping) and updates the
  record whenever the mapping changes. Peers with STUN disabled are unaffected.

## [0.3.0] - 2026-10-24

Initial public release: a peer-to-peer transport that carries QUIC between
peers over an MQTT signaling channel, with NAT traversal and a relay fallback.

### Added

- QUIC peer-to-peer transport (`Peer`, `P2pConnection`) with MQTT-based peer
  discovery and signaling. Peers register on a broker, discover each other, and
  negotiate a direct QUIC connection; established connections expose bidi and
  uni streams and the remote's verified transport key.
- STUN/TURN message codec and a `TurnClient`, with a relay socket and relayed
  candidate emission, so a pair that cannot form a direct path falls back to a
  TURN-relayed QUIC connection. Covered by a coturn-gated integration test.
- Ed25519 transport identity from a caller-supplied 32-byte seed
  (`PeerConfig::with_ed25519_identity`), tying the QUIC certificate fingerprint
  to a persisted signing key. Key material is zeroized on drop.
- Heartbeat-TTL presence in discovery so stale peers age out, and routing of
  concurrent inbound connections by certificate fingerprint so each pair forms
  exactly one connection.
