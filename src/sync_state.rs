use crate::hlc::{Hlc, PeerId, Stamp};
use crate::lww::{Applier, Op};
use crate::replog::{Cursors, RecordOutcome, ReplLog};
use crate::wire::WriteFrame;

/// One peer's complete sync state: the HLC, the per-origin replication log
/// (delivery), and the LWW applier (conflict resolution + GC). This is the
/// executable analog of the verified TLA+ models — `ReplLog` mirrors
/// `truelog`/`seen`/`Sync`, `Applier` mirrors the LWW + GC-floor logic.
#[derive(Debug)]
pub struct SyncState {
    self_id: PeerId,
    clock: Hlc,
    log: ReplLog,
    applier: Applier,
}

impl SyncState {
    #[must_use]
    pub fn new(self_id: PeerId) -> Self {
        Self {
            self_id,
            clock: Hlc::default(),
            log: ReplLog::new(self_id),
            applier: Applier::new(),
        }
    }

    #[must_use]
    pub fn self_id(&self) -> PeerId {
        self.self_id
    }

    /// Originate a local mutation. Advances the HLC, appends to our own log,
    /// applies it locally, and returns the frame to broadcast to peers.
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
        let frame = self.log.append_local(stamp, op, entity, id, data);
        self.applier.merge(frame.clone().into_stamped());
        frame
    }

    /// Apply a frame received from a peer. Always advances the HLC by observing
    /// the remote stamp. Only merges into state when the frame is the next
    /// in-order write for its origin (`RecordOutcome::Appended`); duplicates and
    /// gaps leave state untouched.
    pub fn receive(&mut self, frame: WriteFrame, wall_ms: u64) -> RecordOutcome {
        self.clock.observe(frame.stamp.hlc, wall_ms);
        let outcome = self.log.record(frame.clone());
        if outcome == RecordOutcome::Appended {
            self.applier.merge(frame.into_stamped());
        }
        outcome
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
