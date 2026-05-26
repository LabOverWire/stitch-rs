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

pub mod hlc;
pub mod lww;
pub mod node;
pub mod protocol;
pub mod replog;
pub mod session;
pub mod sync_state;
pub mod wire;

#[cfg(feature = "discovery")]
pub mod discovery;
#[cfg(feature = "membership")]
pub mod membership;
#[cfg(feature = "persistence")]
pub mod persistence;
#[cfg(feature = "store")]
pub mod store;

pub use hlc::{Hlc, PEER_ID_LEN, PeerId, Stamp};
pub use lww::{Applier, MergeOutcome, Op, StampedWrite};
pub use node::SyncNode;
pub use protocol::{MAX_MESSAGE_LEN, ProtocolError, SyncMessage};
pub use replog::{Cursors, RecordOutcome, ReplLog};
pub use sync_state::{MutationEvent, SyncState, WriteOrigin};
pub use wire::{HEADER_LEN, WIRE_VERSION, WireError, WriteFrame};

pub use sync_state::{FrameAuth, FramePersister};

#[cfg(feature = "discovery")]
pub use discovery::{Swarm, peer_id_from_fingerprint};
#[cfg(feature = "membership")]
pub use membership::{Identity, verify_frame};
#[cfg(feature = "persistence")]
pub use persistence::{FjallLog, PersistError};
#[cfg(feature = "store")]
pub use store::{Store, StoreError};
