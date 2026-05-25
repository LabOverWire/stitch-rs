use crate::hlc::{PeerId, Stamp};
use crate::lww::Op;
use crate::wire::WriteFrame;
use std::collections::HashMap;

pub type Cursors = HashMap<PeerId, u64>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    Appended,
    Duplicate,
    Gap { expected: u64, got: u64 },
}

#[derive(Debug)]
pub struct ReplLog {
    self_id: PeerId,
    observed: HashMap<PeerId, Vec<WriteFrame>>,
}

impl ReplLog {
    #[must_use]
    pub fn new(self_id: PeerId) -> Self {
        Self {
            self_id,
            observed: HashMap::new(),
        }
    }

    #[must_use]
    pub fn self_id(&self) -> PeerId {
        self.self_id
    }

    pub fn append_local(
        &mut self,
        stamp: Stamp,
        op: Op,
        entity: impl Into<String>,
        id: impl Into<String>,
        data: Vec<u8>,
    ) -> WriteFrame {
        debug_assert_eq!(stamp.peer, self.self_id, "local writes must be self-stamped");
        let origin = self.self_id;
        let log = self.observed.entry(origin).or_default();
        let seq = log.len() as u64 + 1;
        let frame = WriteFrame {
            stamp,
            seq,
            op,
            entity: entity.into(),
            id: id.into(),
            data,
        };
        log.push(frame.clone());
        frame
    }

    pub fn record(&mut self, frame: WriteFrame) -> RecordOutcome {
        let origin = frame.stamp.peer;
        let log = self.observed.entry(origin).or_default();
        let have = log.len() as u64;
        if frame.seq <= have {
            return RecordOutcome::Duplicate;
        }
        if frame.seq != have + 1 {
            return RecordOutcome::Gap {
                expected: have + 1,
                got: frame.seq,
            };
        }
        log.push(frame);
        RecordOutcome::Appended
    }

    #[must_use]
    pub fn cursors(&self) -> Cursors {
        self.observed
            .iter()
            .map(|(origin, log)| (*origin, log.len() as u64))
            .collect()
    }

    #[must_use]
    pub fn cursor_for(&self, origin: &PeerId) -> u64 {
        self.observed.get(origin).map_or(0, |log| log.len() as u64)
    }

    #[must_use]
    pub fn delta_since(&self, their: &Cursors) -> Vec<WriteFrame> {
        let mut out = Vec::new();
        for (origin, log) in &self.observed {
            let from = their.get(origin).copied().unwrap_or(0);
            let start = usize::try_from(from).unwrap_or(usize::MAX).min(log.len());
            out.extend_from_slice(&log[start..]);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::{Hlc, PEER_ID_LEN};

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
        }
    }

    #[test]
    fn append_local_assigns_increasing_seq() {
        let mut log = ReplLog::new(peer(1));
        let a = log.append_local(Stamp::new(Hlc::new(5, 0), peer(1)), Op::Insert, "task", "t1", vec![]);
        let b = log.append_local(Stamp::new(Hlc::new(6, 0), peer(1)), Op::Insert, "task", "t2", vec![]);
        assert_eq!(a.seq, 1);
        assert_eq!(b.seq, 2);
        assert_eq!(log.cursor_for(&peer(1)), 2);
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
