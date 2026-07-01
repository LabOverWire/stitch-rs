# mqp2p

Peer-to-peer file transfer over QUIC, using MQTT as the signaling channel.

Peers register with an [MQDB](https://github.com/LabOverWire/MQDB) broker, discover each other through MQTT queries, perform STUN-based NAT discovery, punch through NATs with RTT-synchronized UDP probes, and establish a QUIC connection for encrypted file transfer — all without a relay server.

## How it works

```
Peer A                       MQDB Broker                    Peer B
  |                              |                            |
  |--- register (MQTT $DB/) ---->|                            |
  |                              |<--- register (MQTT $DB/) --|
  |--- list peers (MQTT $DB/) -->|                            |
  |<-- peer B info --------------|                            |
  |                              |                            |
  |--- offer (candidates) ------>|--- offer ----------------->|
  | t0                           |<--- answer (candidates) ---|
  |<-- answer -------------------|                            |
  | t1, RTT = t1 - t0           |                            |
  |--- sync(RTT) -------------->|--- sync(RTT) ------------->|
  | immediately dial             |                    wait RTT/2
  |-- UDP probe + QUIC -------->|                    then dial|
  |                              |<-- UDP probe + QUIC accept-|
  |<============ QUIC connection (direct, no relay) =========>|
  |<============ encrypted file transfer ===================>|
```

### NAT traversal

1. Each peer binds a UDP socket and queries a STUN server (default: `stun.l.google.com:19302`) to discover its server-reflexive (public) address
2. Both host and srflx candidates are exchanged via MQTT signaling
3. The initiator measures the signaling round-trip time from the offer/answer exchange
4. A SYNC message carrying the measured RTT is sent to the responder
5. The initiator dials immediately; the responder waits RTT/2 then dials — both NAT pinholes open at approximately the same instant
6. Candidates are tried in priority order: srflx first (5s timeout per candidate), then host as fallback

This RTT-synchronized approach (similar to [libp2p DCUtR](https://github.com/libp2p/specs/blob/master/relay/DCUtR.md)) replaces the naive probe-storm strategy. Instead of flooding UDP packets for 2 seconds, each side sends a single 4-byte probe timed to coincide with the other, reducing the chance of triggering carrier NAT rate-limits.

### Security

- Each peer generates an ephemeral Ed25519 certificate at startup
- SHA-256 fingerprints are exchanged through the signaling channel
- QUIC mutual TLS verifies both sides match the expected fingerprint (TOFU model)
- File integrity is verified end-to-end with SHA-256

## Usage

### As a library

```rust
use mqp2p::{Peer, PeerConfig};

let config = PeerConfig::new("alice", "broker.example.com:1883")
    .with_credentials("user", "pass")
    .with_stun_server("stun.l.google.com:19302");

let mut peer = Peer::new(config).await?;
peer.register().await?;

// Sender
let peers = peer.discover_peers().await?;
let target = peers.iter().find(|p| p.name == "bob").unwrap();
let conn = peer.connect_to(target).await?;
let result = conn.send_file(path, |p| println!("{}%", p.bytes_transferred * 100 / p.total_bytes)).await?;

// Receiver
let conn = peer.accept_connection().await?;
let result = conn.receive_file(output_dir, |_offer| true, |p| println!("{}%", p.bytes_transferred * 100 / p.total_bytes)).await?;
```

### Example binary

```bash
cargo build --example transfer

# Start receiver (waits for incoming connection)
RUST_LOG=info cargo run --example transfer -- \
    --broker 127.0.0.1:1883 \
    --user myuser --pass mypass \
    receive --name bob --output /tmp

# Send a file
RUST_LOG=info cargo run --example transfer -- \
    --broker 127.0.0.1:1883 \
    --user myuser --pass mypass \
    send --name alice --file ./data.bin --peer bob
```

### Broker setup

mqp2p uses [MQDB](https://github.com/LabOverWire/MQDB) as both the MQTT broker and the peer registry database. Any MQTT 5 broker works for the signaling (offer/answer/sync), but peer discovery (`list_peers`, `register_peer`) relies on MQDB's `$DB/` topic API.

```bash
# Generate password file
mqdb passwd -b mypassword -n myuser >> passwd.txt

# Start broker with authentication
mqdb agent start --bind 0.0.0.0:1883 --passwd passwd.txt
```

## Architecture

```
src/
├── lib.rs          Public API re-exports
├── error.rs        Error types (thiserror)
├── stun.rs         STUN binding client (RFC 5389) + UDP hole-punch probes
├── quic.rs         QUIC endpoint, self-signed Ed25519 certs, fingerprint verification
├── signaling.rs    MQTT-based peer registry + offer/answer/sync exchange
├── transfer.rs     Length-prefixed binary frames over QUIC bidirectional streams
└── peer.rs         High-level Peer API tying everything together
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| [quinn](https://crates.io/crates/quinn) | QUIC transport |
| [rustls](https://crates.io/crates/rustls) | TLS for QUIC |
| [rcgen](https://crates.io/crates/rcgen) | Self-signed certificate generation |
| [ring](https://crates.io/crates/ring) | SHA-256 hashing, STUN transaction IDs |
| [mqtt5](https://github.com/LabOverWire/mqtt-lib) | MQTT 5 client (patched fork) |
| [flume](https://crates.io/crates/flume) | Async channels for signaling subscriptions |
| [tokio](https://crates.io/crates/tokio) | Async runtime |

## License

MIT
