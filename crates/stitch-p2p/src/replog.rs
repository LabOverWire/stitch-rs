use crate::hlc::PeerId;
use crate::wire::WriteFrame;
use std::collections::HashMap;

pub type Cursors = HashMap<PeerId, u64>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    Appended,
    Duplicate,
    Gap { expected: u64, got: u64 },
    /// The frame failed authentication/authorization and was not recorded.
    Rejected,
}

#[derive(Debug)]
pub struct ReplLog {
    self_id: PeerId,
    observed: HashMap<PeerId, Vec<WriteFrame>>,
    /// Seq of the last reclaimed (truncated) frame per origin. Stored frames
    /// are those with seq in `base + 1 ..= base + observed.len()`.
    base: HashMap<PeerId, u64>,
}

impl ReplLog {
    #[must_use]
    pub fn new(self_id: PeerId) -> Self {
        Self {
            self_id,
            observed: HashMap::new(),
            base: HashMap::new(),
        }
    }

    #[must_use]
    pub fn self_id(&self) -> PeerId {
        self.self_id
    }

    fn base_of(&self, origin: &PeerId) -> u64 {
        self.base.get(origin).copied().unwrap_or(0)
    }

    /// Highest seq applied for `origin` (covers reclaimed frames too).
    #[must_use]
    pub fn cursor_for(&self, origin: &PeerId) -> u64 {
        self.base_of(origin) + self.observed.get(origin).map_or(0, Vec::len) as u64
    }

    /// The seq the next write from `origin` will take (1-based).
    #[must_use]
    pub fn next_seq(&self, origin: &PeerId) -> u64 {
        self.cursor_for(origin) + 1
    }

    pub fn record(&mut self, frame: WriteFrame) -> RecordOutcome {
        let origin = frame.stamp.peer;
        let have = self.cursor_for(&origin);
        if frame.seq <= have {
            return RecordOutcome::Duplicate;
        }
        if frame.seq != have + 1 {
            return RecordOutcome::Gap {
                expected: have + 1,
                got: frame.seq,
            };
        }
        self.observed.entry(origin).or_default().push(frame);
        RecordOutcome::Appended
    }

    #[must_use]
    pub fn cursors(&self) -> Cursors {
        self.observed
            .keys()
            .chain(self.base.keys())
            .map(|origin| (*origin, self.cursor_for(origin)))
            .collect()
    }

    #[must_use]
    pub fn delta_since(&self, their: &Cursors) -> Vec<WriteFrame> {
        let mut out = Vec::new();
        for (origin, log) in &self.observed {
            let base = self.base_of(origin);
            let from = their.get(origin).copied().unwrap_or(0);
            let skip = from.saturating_sub(base);
            let start = usize::try_from(skip).unwrap_or(usize::MAX).min(log.len());
            out.extend_from_slice(&log[start..]);
        }
        out
    }

    /// Drop the prefix of `origin`'s log with seq `<= up_to`, advancing the
    /// base. Safe only when every member has already delivered through `up_to`
    /// (the cursor low-water-mark — see `spec/StitchP2PReclaim.tla`). Returns
    /// the number of frames dropped.
    pub fn truncate(&mut self, origin: &PeerId, up_to: u64) -> usize {
        let base = self.base_of(origin);
        if up_to <= base {
            return 0;
        }
        let Some(log) = self.observed.get_mut(origin) else {
            return 0;
        };
        let drop = usize::try_from(up_to - base).unwrap_or(usize::MAX).min(log.len());
        log.drain(..drop);
        self.base.insert(*origin, base + drop as u64);
        drop
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::{Hlc, PEER_ID_LEN, Stamp};
    use crate::lww::Op;

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; PEER_ID_LEN];
        id[0] = n;
        id
    }

    fn frame(origin: u8, seq: u64) -> WriteFrame {
        WriteFrame {
            stamp: Stamp::new(Hlc::new(seq, 0), peer(origin)),
            seq,
            op: Op::Insert,
            entity: "task".into(),
            id: format!("t{seq}"),
            data: vec![origin],
            signature: None,
        }
    }

    #[test]
    fn next_seq_advances_with_records() {
        let mut log = ReplLog::new(peer(1));
        assert_eq!(log.next_seq(&peer(1)), 1);
        log.record(frame(1, 1));
        assert_eq!(log.next_seq(&peer(1)), 2);
        log.record(frame(1, 2));
        assert_eq!(log.next_seq(&peer(1)), 3);
    }

    #[test]
    fn truncate_drops_prefix_and_preserves_cursor() {
        let mut log = ReplLog::new(peer(1));
        log.record(frame(2, 1));
        log.record(frame(2, 2));
        log.record(frame(2, 3));

        assert_eq!(log.truncate(&peer(2), 2), 2);
        assert_eq!(log.cursor_for(&peer(2)), 3, "cursor unchanged after truncate");
        assert_eq!(log.next_seq(&peer(2)), 4);

        let mut from2 = Cursors::new();
        from2.insert(peer(2), 2);
        assert_eq!(log.delta_since(&from2).len(), 1, "only seq 3 remains to serve");

        assert_eq!(
            log.record(frame(2, 4)),
            RecordOutcome::Appended,
            "log keeps accepting in order after truncation"
        );
    }

    #[test]
    fn truncate_is_idempotent_and_bounded() {
        let mut log = ReplLog::new(peer(1));
        log.record(frame(2, 1));
        assert_eq!(log.truncate(&peer(2), 5), 1, "clamps to available frames");
        assert_eq!(log.truncate(&peer(2), 1), 0, "already at or below base");
    }

    #[test]
    fn record_appends_in_order() {
        let mut log = ReplLog::new(peer(1));
        assert_eq!(log.record(frame(2, 1)), RecordOutcome::Appended);
        assert_eq!(log.record(frame(2, 2)), RecordOutcome::Appended);
        assert_eq!(log.cursor_for(&peer(2)), 2);
    }

    #[test]
    fn record_rejects_gap() {
        let mut log = ReplLog::new(peer(1));
        log.record(frame(2, 1));
        assert_eq!(
            log.record(frame(2, 3)),
            RecordOutcome::Gap { expected: 2, got: 3 }
        );
        assert_eq!(log.cursor_for(&peer(2)), 1);
    }

    #[test]
    fn record_treats_old_seq_as_duplicate() {
        let mut log = ReplLog::new(peer(1));
        log.record(frame(2, 1));
        log.record(frame(2, 2));
        assert_eq!(log.record(frame(2, 1)), RecordOutcome::Duplicate);
        assert_eq!(log.record(frame(2, 2)), RecordOutcome::Duplicate);
    }

    #[test]
    fn delta_since_returns_only_unseen() {
        let mut server = ReplLog::new(peer(2));
        server.record(frame(2, 1));
        server.record(frame(2, 2));
        server.record(frame(3, 1));

        let mut their = Cursors::new();
        their.insert(peer(2), 1);
        let delta = server.delta_since(&their);
        assert_eq!(delta.len(), 2);
        assert!(delta.iter().any(|f| f.stamp.peer == peer(2) && f.seq == 2));
        assert!(delta.iter().any(|f| f.stamp.peer == peer(3) && f.seq == 1));
    }

    #[test]
    fn delta_since_empty_cursor_returns_everything() {
        let mut server = ReplLog::new(peer(2));
        server.record(frame(2, 1));
        server.record(frame(3, 1));
        let delta = server.delta_since(&Cursors::new());
        assert_eq!(delta.len(), 2);
    }
}
