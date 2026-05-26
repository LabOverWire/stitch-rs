use crate::hlc::{Hlc, PeerId, Stamp};
use crate::lww::{Applier, MergeOutcome, Op};
use crate::replog::{Cursors, RecordOutcome, ReplLog};
use crate::wire::{SIGNATURE_LEN, WriteFrame};
use std::sync::Arc;
use tokio::sync::broadcast;

const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Durable sink for the per-origin replication log. Every frame that enters the
/// log (local or received) is handed to `append`; `load` returns all persisted
/// frames at startup so state can be rebuilt by replay. Implemented by the
/// `persistence` feature; the verified core stays storage-agnostic.
pub trait FramePersister: Send + Sync {
    fn append(&self, frame: &WriteFrame);
    fn load(&self) -> Vec<WriteFrame>;
}

/// Signs locally-originated frames and verifies/authorizes received ones.
/// Implemented by the `membership` feature (Ed25519); the verified core stays
/// crypto-agnostic. With no `FrameAuth` set, writes are unsigned and accepted
/// as today.
pub trait FrameAuth: Send + Sync {
    /// Sign a local frame's [`WriteFrame::signing_bytes`].
    fn sign(&self, signing_bytes: &[u8]) -> [u8; SIGNATURE_LEN];
    /// Accept a received frame? Checks the signature and (later) authorization.
    fn verify(&self, frame: &WriteFrame) -> bool;
}

/// Where an observable mutation came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOrigin {
    Local,
    Remote,
}

/// Emitted whenever a write changes the visible state (the LWW winner), whether
/// originated locally or applied from a peer. Delivered on the bus exposed by
/// [`SyncState::subscribe`].
#[derive(Debug, Clone)]
pub struct MutationEvent {
    pub op: Op,
    pub entity: String,
    pub id: String,
    /// Serialized record for inserts/updates; `None` for deletes.
    pub data: Option<Vec<u8>>,
    pub origin: WriteOrigin,
}

/// One peer's complete sync state: the HLC, the per-origin replication log
/// (delivery), and the LWW applier (conflict resolution + GC). This is the
/// executable analog of the verified TLA+ models — `ReplLog` mirrors
/// `truelog`/`seen`/`Sync`, `Applier` mirrors the LWW + GC-floor logic.
pub struct SyncState {
    self_id: PeerId,
    clock: Hlc,
    log: ReplLog,
    applier: Applier,
    events: broadcast::Sender<MutationEvent>,
    persister: Option<Arc<dyn FramePersister>>,
    auth: Option<Arc<dyn FrameAuth>>,
}

impl SyncState {
    #[must_use]
    pub fn new(self_id: PeerId) -> Self {
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            self_id,
            clock: Hlc::default(),
            log: ReplLog::new(self_id),
            applier: Applier::new(),
            events,
            persister: None,
            auth: None,
        }
    }

    /// Attach signing/verification. Local writes are signed; received frames
    /// failing `verify` are rejected before they touch the log or state.
    pub fn set_auth(&mut self, auth: Arc<dyn FrameAuth>) {
        self.auth = Some(auth);
    }

    /// Attach a durable sink. Call after [`SyncState::replay`]-ing any persisted
    /// frames so replay doesn't re-persist what's already stored.
    pub fn set_persister(&mut self, persister: Arc<dyn FramePersister>) {
        self.persister = Some(persister);
    }

    /// Rebuild state from a persisted frame at startup: advances the clock,
    /// records into the log, and merges into the applier — without re-persisting
    /// or emitting events.
    pub fn replay(&mut self, frame: WriteFrame) {
        self.clock.observe(frame.stamp.hlc, frame.stamp.hlc.physical);
        if self.log.record(frame.clone()) == RecordOutcome::Appended {
            self.applier.merge(frame.into_stamped());
        }
    }

    #[must_use]
    pub fn self_id(&self) -> PeerId {
        self.self_id
    }

    /// Subscribe to visible-state mutations (local and remote).
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<MutationEvent> {
        self.events.subscribe()
    }

    /// Originate a local mutation. Advances the HLC, builds and (if a
    /// [`FrameAuth`] is set) signs the frame, records it to our own log, applies
    /// it locally, emits a [`MutationEvent`], and returns the frame to broadcast.
    pub fn local_write(
        &mut self,
        wall_ms: u64,
        op: Op,
        entity: impl Into<String>,
        id: impl Into<String>,
        data: Vec<u8>,
    ) -> WriteFrame {
        let hlc = self.clock.tick(wall_ms);
        let stamp = Stamp::new(hlc, self.self_id);
        let seq = self.log.next_seq(&self.self_id);
        let mut frame = WriteFrame {
            stamp,
            seq,
            op,
            entity: entity.into(),
            id: id.into(),
            data,
            signature: None,
        };
        if let Some(auth) = &self.auth
            && let Ok(bytes) = frame.signing_bytes()
        {
            frame.signature = Some(auth.sign(&bytes));
        }
        self.log.record(frame.clone());
        self.persist(&frame);
        self.applier.merge(frame.clone().into_stamped());
        self.emit(&frame, WriteOrigin::Local);
        frame
    }

    /// Apply a frame received from a peer. With a [`FrameAuth`] set, an
    /// unverified frame is rejected before it touches the log or state. Always
    /// advances the HLC by observing the remote stamp. Merges only when the
    /// frame is the next in-order write for its origin; emits a
    /// [`MutationEvent`] only when it changes the visible state (won LWW).
    pub fn receive(&mut self, frame: WriteFrame, wall_ms: u64) -> RecordOutcome {
        if let Some(auth) = &self.auth
            && !auth.verify(&frame)
        {
            return RecordOutcome::Rejected;
        }
        self.clock.observe(frame.stamp.hlc, wall_ms);
        let outcome = self.log.record(frame.clone());
        if outcome == RecordOutcome::Appended {
            self.persist(&frame);
            if self.applier.merge(frame.clone().into_stamped()) == MergeOutcome::Applied {
                self.emit(&frame, WriteOrigin::Remote);
            }
        }
        outcome
    }

    fn persist(&self, frame: &WriteFrame) {
        if let Some(persister) = &self.persister {
            persister.append(frame);
        }
    }

    fn emit(&self, frame: &WriteFrame, origin: WriteOrigin) {
        let data = if frame.op == Op::Delete {
            None
        } else {
            Some(frame.data.clone())
        };
        let _ = self.events.send(MutationEvent {
            op: frame.op,
            entity: frame.entity.clone(),
            id: frame.id.clone(),
            data,
            origin,
        });
    }

    #[must_use]
    pub fn cursors(&self) -> Cursors {
        self.log.cursors()
    }

    #[must_use]
    pub fn delta_since(&self, their: &Cursors) -> Vec<WriteFrame> {
        self.log.delta_since(their)
    }

    #[must_use]
    pub fn visible(&self, entity: &str, id: &str) -> Option<&[u8]> {
        self.applier.visible(entity, id)
    }

    /// Every visible (non-deleted) record of an entity, as `(id, data)`.
    #[must_use]
    pub fn visible_entity(&self, entity: &str) -> Vec<(String, Vec<u8>)> {
        self.applier.visible_entity(entity)
    }

    pub fn collect_tombstone(&mut self, entity: &str, id: &str) -> bool {
        self.applier.collect_tombstone(entity, id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::PEER_ID_LEN;

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; PEER_ID_LEN];
        id[0] = n;
        id
    }

    #[test]
    fn local_write_is_visible_and_broadcastable() {
        let mut s = SyncState::new(peer(1));
        let frame = s.local_write(100, Op::Insert, "task", "t1", b"hi".to_vec());
        assert_eq!(frame.seq, 1);
        assert_eq!(s.visible("task", "t1"), Some(&b"hi"[..]));
    }

    #[test]
    fn receive_applies_remote_write() {
        let mut a = SyncState::new(peer(1));
        let mut b = SyncState::new(peer(2));
        let f = a.local_write(100, Op::Insert, "task", "t1", b"from-a".to_vec());
        assert_eq!(b.receive(f, 50), RecordOutcome::Appended);
        assert_eq!(b.visible("task", "t1"), Some(&b"from-a"[..]));
    }

    #[test]
    fn receiving_advances_clock_past_remote() {
        let mut a = SyncState::new(peer(1));
        let mut b = SyncState::new(peer(2));
        let f = a.local_write(1000, Op::Insert, "task", "t1", b"x".to_vec());
        b.receive(f, 10);
        let later = b.local_write(10, Op::Update, "task", "t1", b"y".to_vec());
        assert!(later.stamp.hlc.physical >= 1000);
    }
}
