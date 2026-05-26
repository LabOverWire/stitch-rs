//! Pure peer-to-peer state sync for stitch.
//!
//! Multi-leader, eventually-consistent replication with no central authority.
//! Conflict resolution is last-writer-wins keyed by a Hybrid Logical Clock plus
//! a peer-fingerprint tiebreak, giving a deterministic total order across peers.
//!
//! The conflict-resolution and tombstone-GC logic in [`lww`] is a direct port of
//! the TLA+ models in `spec/`, which TLC verified for convergence — including
//! the per-record GC floor that prevents stale writes from resurrecting a
//! deleted record. See `spec/README.md`.
//!
//! # Milestone status
//!
//! M1 (this code): the wire frame ([`wire`]), the clock ([`hlc`]), and the
//! verified merge core ([`lww`]). Transport (mqp2p QUIC sessions), anti-entropy
//! cursors, and membership are later milestones.

pub mod discovery;
pub mod hlc;
pub mod lww;
pub mod node;
pub mod protocol;
pub mod replog;
pub mod session;
pub mod store;
pub mod sync_state;
pub mod wire;

pub use discovery::{Swarm, peer_id_from_fingerprint};
pub use hlc::{Hlc, PEER_ID_LEN, PeerId, Stamp};
pub use lww::{Applier, MergeOutcome, Op, StampedWrite};
pub use node::SyncNode;
pub use protocol::{MAX_MESSAGE_LEN, ProtocolError, SyncMessage};
pub use replog::{Cursors, RecordOutcome, ReplLog};
pub use store::{Store, StoreError};
pub use sync_state::{MutationEvent, SyncState, WriteOrigin};
pub use wire::{HEADER_LEN, WIRE_VERSION, WireError, WriteFrame};
