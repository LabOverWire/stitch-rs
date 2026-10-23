use crate::hlc::{Hlc, PeerId, Stamp};
use crate::lww::{Applier, MergeOutcome, Op};
use crate::replog::{Cursors, RecordOutcome, ReplLog};
use crate::wire::{SIGNATURE_LEN, WireError, WriteFrame, check_lengths};
use std::sync::Arc;
use tokio::sync::broadcast;

const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Durable sink for the per-origin replication log. Every frame that enters the
/// log (local or received) is handed to `append` first, and enters the log
/// only if `append` succeeds; `load` returns all persisted frames at startup so
/// state can be rebuilt by replay. Implemented by the `persistence` feature;
/// the verified core stays storage-agnostic.
pub trait FramePersister: Send + Sync {
    fn append(&self, frame: &WriteFrame) -> std::io::Result<()>;
    fn load(&self) -> std::io::Result<Vec<WriteFrame>>;
}

/// Why a local write was refused. A refused write takes no sequence number
/// and changes no state.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("write does not fit a frame: {0}")]
    Wire(#[from] WireError),
    #[error("write could not be persisted: {0}")]
    Persist(#[source] std::io::Error),
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
    peer_cursors: std::collections::HashMap<PeerId, Cursors>,
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
            peer_cursors: std::collections::HashMap::new(),
        }
    }

    /// Record a peer's reported cursors (from its `Hello`). Feeds the
    /// reclamation low-water-mark.
    pub fn note_peer_cursors(&mut self, peer: PeerId, cursors: Cursors) {
        self.peer_cursors.insert(peer, cursors);
    }

    #[cfg(test)]
    pub(crate) fn peer_cursor(&self, peer: &PeerId, origin: &PeerId) -> u64 {
        self.peer_cursors
            .get(peer)
            .and_then(|c| c.get(origin))
            .copied()
            .unwrap_or(0)
    }

    /// Reclaim **in-memory** replication-log prefixes that every listed member
    /// has already delivered. For each origin, the low-water-mark is the minimum
    /// cursor across this peer and all `members`; frames at or below it are
    /// dropped from the in-memory log. Returns frames dropped.
    ///
    /// Per `spec/StitchP2PReclaim.tla` this is safe: no member can still send an
    /// older write once all have delivered through the mark. A member whose
    /// cursor is unknown (never heard from) holds the mark at 0, so nothing is
    /// reclaimed until it reports in — no resurrection, but no GC meanwhile.
    ///
    /// This bounds a long-running peer's memory. It does not truncate durable
    /// storage: a persisted store rebuilds state by replaying the full log on
    /// reopen, so on-disk reclamation requires a state snapshot (future work).
    pub fn reclaim(&mut self, members: &[PeerId]) -> usize {
        let mut dropped = 0;
        let origins: Vec<PeerId> = self.cursors().keys().copied().collect();
        for origin in origins {
            let mut lwm = self.log.cursor_for(&origin);
            for member in members {
                if *member == self.self_id {
                    continue;
                }
                let cursor = self
                    .peer_cursors
                    .get(member)
                    .and_then(|c| c.get(&origin))
                    .copied()
                    .unwrap_or(0);
                lwm = lwm.min(cursor);
            }
            if lwm > 0 {
                dropped += self.log.truncate(&origin, lwm);
            }
        }
        dropped
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
        self.clock
            .observe(frame.stamp.hlc, frame.stamp.hlc.physical);
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
    /// [`FrameAuth`] is set) signs the frame, persists it, records it to our
    /// own log, applies it locally, emits a [`MutationEvent`], and returns the
    /// frame to broadcast.
    ///
    /// # Errors
    ///
    /// [`WriteError::Wire`] when the entity, id or data do not fit a frame, and
    /// [`WriteError::Persist`] when the persister fails. Either way the write
    /// takes no sequence number and changes no state.
    pub fn local_write(
        &mut self,
        wall_ms: u64,
        op: Op,
        entity: impl Into<String>,
        id: impl Into<String>,
        data: Vec<u8>,
    ) -> Result<WriteFrame, WriteError> {
        let entity = entity.into();
        let id = id.into();
        check_lengths(&entity, &id, &data)?;
        let hlc = self.clock.tick(wall_ms);
        let stamp = Stamp::new(hlc, self.self_id);
        let seq = self.log.next_seq(&self.self_id);
        let mut frame = WriteFrame {
            stamp,
            seq,
            op,
            entity,
            id,
            data,
            signature: None,
        };
        if let Some(auth) = &self.auth {
            frame.signature = Some(auth.sign(&frame.signing_bytes()?));
        }
        self.persist(&frame).map_err(WriteError::Persist)?;
        self.log.record(frame.clone());
        self.applier.merge(frame.clone().into_stamped());
        self.emit(&frame, WriteOrigin::Local);
        Ok(frame)
    }

    /// Apply a frame received from a peer. With a [`FrameAuth`] set, an
    /// unverified frame is rejected before it touches the log or state. Always
    /// advances the HLC by observing the remote stamp. Merges only when the
    /// frame is the next in-order write for its origin; emits a
    /// [`MutationEvent`] only when it changes the visible state (won LWW). A
    /// frame the persister fails to store is not recorded
    /// ([`RecordOutcome::Unpersisted`]), so the peer offers it again.
    pub fn receive(&mut self, frame: WriteFrame, wall_ms: u64) -> RecordOutcome {
        if let Some(auth) = &self.auth
            && !auth.verify(&frame)
        {
            return RecordOutcome::Rejected;
        }
        self.clock.observe(frame.stamp.hlc, wall_ms);
        let outcome = self.log.classify(&frame);
        if outcome != RecordOutcome::Appended {
            return outcome;
        }
        if let Err(error) = self.persist(&frame) {
            tracing::error!(seq = frame.seq, %error, "received frame not persisted");
            return RecordOutcome::Unpersisted;
        }
        self.log.record(frame.clone());
        if self.applier.merge(frame.clone().into_stamped()) == MergeOutcome::Applied {
            self.emit(&frame, WriteOrigin::Remote);
        }
        outcome
    }

    fn persist(&self, frame: &WriteFrame) -> std::io::Result<()> {
        match &self.persister {
            Some(persister) => persister.append(frame),
            None => Ok(()),
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

    /// Every frame this node holds for an entity, across all origins. Used to
    /// present a signed membership proof to a peer that has not yet synced the
    /// records.
    #[must_use]
    pub fn frames_for_entity(&self, entity: &str) -> Vec<WriteFrame> {
        self.delta_since(&Cursors::new())
            .into_iter()
            .filter(|frame| frame.entity == entity)
            .collect()
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

    /// The visible record plus the peer id that authored the winning write.
    #[must_use]
    pub fn visible_with_author(&self, entity: &str, id: &str) -> Option<(Vec<u8>, PeerId)> {
        self.applier.visible_with_author(entity, id)
    }

    /// Every visible record of an entity as `(id, data, author)`.
    #[must_use]
    pub fn entries_with_authors(&self, entity: &str) -> Vec<(String, Vec<u8>, PeerId)> {
        self.applier.entries_with_authors(entity)
    }

    pub fn collect_tombstone(&mut self, entity: &str, id: &str) -> bool {
        self.applier.collect_tombstone(entity, id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::PEER_ID_LEN;
    use crate::wire::WireError;

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; PEER_ID_LEN];
        id[0] = n;
        id
    }

    struct Flaky {
        failing: std::sync::atomic::AtomicBool,
        stored: std::sync::Mutex<Vec<WriteFrame>>,
    }

    impl Flaky {
        fn new(failing: bool) -> Arc<Self> {
            Arc::new(Self {
                failing: std::sync::atomic::AtomicBool::new(failing),
                stored: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn set_failing(&self, failing: bool) {
            self.failing
                .store(failing, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl FramePersister for Flaky {
        fn append(&self, frame: &WriteFrame) -> std::io::Result<()> {
            if self.failing.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(std::io::Error::other("disk full"));
            }
            self.stored.lock().expect("lock").push(frame.clone());
            Ok(())
        }

        fn load(&self) -> std::io::Result<Vec<WriteFrame>> {
            Ok(self.stored.lock().expect("lock").clone())
        }
    }

    #[test]
    fn oversized_fields_are_refused_without_taking_a_sequence_number() {
        let mut s = SyncState::new(peer(1));
        let long = "x".repeat(256);
        assert!(matches!(
            s.local_write(1, Op::Insert, "task", long.clone(), b"a".to_vec()),
            Err(WriteError::Wire(WireError::IdTooLong))
        ));
        assert!(matches!(
            s.local_write(1, Op::Insert, long, "t1", b"a".to_vec()),
            Err(WriteError::Wire(WireError::EntityTooLong))
        ));
        let next = s
            .local_write(2, Op::Insert, "task", "t1", b"a".to_vec())
            .expect("write");
        assert_eq!(next.seq, 1);
        assert_eq!(
            next.encode().map(|bytes| bytes.len()),
            Ok(next.encoded_len())
        );
    }

    #[test]
    fn unpersisted_local_write_changes_nothing() {
        let disk = Flaky::new(true);
        let mut s = SyncState::new(peer(1));
        s.set_persister(disk.clone());
        let mut events = s.subscribe();
        assert!(matches!(
            s.local_write(1, Op::Insert, "task", "t1", b"a".to_vec()),
            Err(WriteError::Persist(_))
        ));
        assert_eq!(s.visible("task", "t1"), None);
        assert!(s.cursors().is_empty());
        assert!(events.try_recv().is_err());

        disk.set_failing(false);
        let frame = s
            .local_write(2, Op::Insert, "task", "t1", b"a".to_vec())
            .expect("write");
        assert_eq!(frame.seq, 1);
        assert_eq!(disk.load().expect("load"), [frame]);
    }

    #[test]
    fn unpersisted_received_frame_is_not_recorded_and_is_taken_again() {
        let mut a = SyncState::new(peer(1));
        let frame = a
            .local_write(1, Op::Insert, "task", "t1", b"a".to_vec())
            .expect("write");
        let disk = Flaky::new(true);
        let mut b = SyncState::new(peer(2));
        b.set_persister(disk.clone());

        assert_eq!(b.receive(frame.clone(), 1), RecordOutcome::Unpersisted);
        assert_eq!(b.visible("task", "t1"), None);
        assert_eq!(b.cursors().get(&peer(1)), None);

        disk.set_failing(false);
        assert_eq!(b.receive(frame, 2), RecordOutcome::Appended);
        assert_eq!(b.visible("task", "t1"), Some(&b"a"[..]));
    }

    #[test]
    fn local_write_is_visible_and_broadcastable() {
        let mut s = SyncState::new(peer(1));
        let frame = s
            .local_write(100, Op::Insert, "task", "t1", b"hi".to_vec())
            .expect("write");
        assert_eq!(frame.seq, 1);
        assert_eq!(s.visible("task", "t1"), Some(&b"hi"[..]));
    }

    #[test]
    fn receive_applies_remote_write() {
        let mut a = SyncState::new(peer(1));
        let mut b = SyncState::new(peer(2));
        let f = a
            .local_write(100, Op::Insert, "task", "t1", b"from-a".to_vec())
            .expect("write");
        assert_eq!(b.receive(f, 50), RecordOutcome::Appended);
        assert_eq!(b.visible("task", "t1"), Some(&b"from-a"[..]));
    }

    #[test]
    fn receiving_advances_clock_past_remote() {
        let mut a = SyncState::new(peer(1));
        let mut b = SyncState::new(peer(2));
        let f = a
            .local_write(1000, Op::Insert, "task", "t1", b"x".to_vec())
            .expect("write");
        b.receive(f, 10);
        let later = b
            .local_write(10, Op::Update, "task", "t1", b"y".to_vec())
            .expect("write");
        assert!(later.stamp.hlc.physical >= 1000);
    }

    #[test]
    fn reclaim_waits_for_lagging_member_then_truncates() {
        let mut a = SyncState::new(peer(1));
        a.local_write(1, Op::Insert, "task", "t1", b"a".to_vec())
            .expect("write");
        a.local_write(2, Op::Insert, "task", "t2", b"b".to_vec())
            .expect("write");

        // No cursor heard from peer 2 → low-water-mark 0 → nothing reclaimed.
        assert_eq!(a.reclaim(&[peer(2)]), 0);

        // Peer 2 reports it has both of peer 1's writes.
        let mut cursors = Cursors::new();
        cursors.insert(peer(1), 2);
        a.note_peer_cursors(peer(2), cursors);

        assert_eq!(
            a.reclaim(&[peer(2)]),
            2,
            "both delivered everywhere → reclaim"
        );
        // Visible state is retained; only the log prefix is dropped.
        assert_eq!(a.visible("task", "t1"), Some(&b"a"[..]));
        assert_eq!(a.visible("task", "t2"), Some(&b"b"[..]));
        // The clock/seq continuity survives reclamation.
        let next = a
            .local_write(3, Op::Insert, "task", "t3", b"c".to_vec())
            .expect("write");
        assert_eq!(next.seq, 3);
    }

    #[test]
    fn reclaim_truncates_only_to_the_slowest_member() {
        let mut a = SyncState::new(peer(1));
        for i in 1..=3u64 {
            a.local_write(i, Op::Insert, "task", format!("t{i}"), vec![i as u8])
                .expect("write");
        }
        let mut fast = Cursors::new();
        fast.insert(peer(1), 3);
        let mut slow = Cursors::new();
        slow.insert(peer(1), 1);
        a.note_peer_cursors(peer(2), fast);
        a.note_peer_cursors(peer(3), slow);

        // Low-water-mark is the slow member's cursor (1).
        assert_eq!(a.reclaim(&[peer(2), peer(3)]), 1);
    }
}
