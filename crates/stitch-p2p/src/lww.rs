use crate::hlc::{PeerId, Stamp};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Insert,
    Update,
    Delete,
}

impl Op {
    #[must_use]
    pub fn is_delete(self) -> bool {
        matches!(self, Op::Delete)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StampedWrite {
    pub stamp: Stamp,
    pub op: Op,
    pub entity: String,
    pub id: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeOutcome {
    Applied,
    StaleDropped,
    BelowGcFloor,
}

#[derive(Debug, Default, Clone)]
struct Cell {
    current: Option<StampedWrite>,
    gc_floor: Option<Stamp>,
}

#[derive(Debug, Default)]
pub struct Applier {
    cells: HashMap<(String, String), Cell>,
}

impl Applier {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn merge(&mut self, write: StampedWrite) -> MergeOutcome {
        let key = (write.entity.clone(), write.id.clone());
        let cell = self.cells.entry(key).or_default();

        if let Some(floor) = cell.gc_floor
            && write.stamp <= floor
        {
            return MergeOutcome::BelowGcFloor;
        }
        if let Some(current) = &cell.current
            && write.stamp <= current.stamp
        {
            return MergeOutcome::StaleDropped;
        }
        cell.current = Some(write);
        MergeOutcome::Applied
    }

    pub fn collect_tombstone(&mut self, entity: &str, id: &str) -> bool {
        let key = (entity.to_string(), id.to_string());
        let Some(cell) = self.cells.get_mut(&key) else {
            return false;
        };
        let Some(current) = &cell.current else {
            return false;
        };
        if !current.op.is_delete() {
            return false;
        }
        let stamp = current.stamp;
        cell.gc_floor = Some(match cell.gc_floor {
            Some(prev) if prev >= stamp => prev,
            _ => stamp,
        });
        cell.current = None;
        true
    }

    #[must_use]
    pub fn visible(&self, entity: &str, id: &str) -> Option<&[u8]> {
        let cell = self.cells.get(&(entity.to_string(), id.to_string()))?;
        match &cell.current {
            Some(w) if !w.op.is_delete() => Some(&w.data),
            _ => None,
        }
    }

    #[must_use]
    pub fn current_stamp(&self, entity: &str, id: &str) -> Option<Stamp> {
        self.cells
            .get(&(entity.to_string(), id.to_string()))?
            .current
            .as_ref()
            .map(|w| w.stamp)
    }

    /// Every visible (non-deleted) record of an entity, as `(id, data)`.
    #[must_use]
    pub fn visible_entity(&self, entity: &str) -> Vec<(String, Vec<u8>)> {
        self.cells
            .iter()
            .filter(|((e, _), _)| e == entity)
            .filter_map(|((_, id), cell)| match &cell.current {
                Some(w) if !w.op.is_delete() => Some((id.clone(), w.data.clone())),
                _ => None,
            })
            .collect()
    }

    /// The visible record plus the peer id that authored the winning write.
    #[must_use]
    pub fn visible_with_author(&self, entity: &str, id: &str) -> Option<(Vec<u8>, PeerId)> {
        match &self.cells.get(&(entity.to_string(), id.to_string()))?.current {
            Some(w) if !w.op.is_delete() => Some((w.data.clone(), w.stamp.peer)),
            _ => None,
        }
    }

    /// Every visible record of an entity as `(id, data, author)`.
    #[must_use]
    pub fn entries_with_authors(&self, entity: &str) -> Vec<(String, Vec<u8>, PeerId)> {
        self.cells
            .iter()
            .filter(|((e, _), _)| e == entity)
            .filter_map(|((_, id), cell)| match &cell.current {
                Some(w) if !w.op.is_delete() => Some((id.clone(), w.data.clone(), w.stamp.peer)),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::{Hlc, PEER_ID_LEN, PeerId};

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; PEER_ID_LEN];
        id[0] = n;
        id
    }

    fn write(seq: u64, p: u8, op: Op, data: &[u8]) -> StampedWrite {
        StampedWrite {
            stamp: Stamp::new(Hlc::new(seq, 0), peer(p)),
            op,
            entity: "task".into(),
            id: "t1".into(),
            data: data.to_vec(),
        }
    }

    #[test]
    fn newer_write_wins() {
        let mut a = Applier::new();
        assert_eq!(a.merge(write(1, 1, Op::Insert, b"a")), MergeOutcome::Applied);
        assert_eq!(a.merge(write(2, 1, Op::Update, b"b")), MergeOutcome::Applied);
        assert_eq!(a.visible("task", "t1"), Some(&b"b"[..]));
    }

    #[test]
    fn older_write_dropped() {
        let mut a = Applier::new();
        a.merge(write(5, 1, Op::Insert, b"new"));
        assert_eq!(
            a.merge(write(3, 1, Op::Update, b"old")),
            MergeOutcome::StaleDropped
        );
        assert_eq!(a.visible("task", "t1"), Some(&b"new"[..]));
    }

    #[test]
    fn peer_breaks_tie_deterministically() {
        let mut a = Applier::new();
        let mut b = Applier::new();
        a.merge(write(1, 1, Op::Insert, b"from1"));
        a.merge(write(1, 2, Op::Insert, b"from2"));
        b.merge(write(1, 2, Op::Insert, b"from2"));
        b.merge(write(1, 1, Op::Insert, b"from1"));
        assert_eq!(a.visible("task", "t1"), b.visible("task", "t1"));
        assert_eq!(a.visible("task", "t1"), Some(&b"from2"[..]));
    }

    #[test]
    fn delete_makes_record_absent() {
        let mut a = Applier::new();
        a.merge(write(1, 1, Op::Insert, b"x"));
        a.merge(write(2, 1, Op::Delete, b""));
        assert_eq!(a.visible("task", "t1"), None);
    }

    #[test]
    fn gc_floor_blocks_stale_resurrection() {
        let mut a = Applier::new();
        a.merge(write(2, 2, Op::Delete, b""));
        assert!(a.collect_tombstone("task", "t1"));
        assert_eq!(a.visible("task", "t1"), None);
        assert_eq!(
            a.merge(write(1, 1, Op::Insert, b"resurrect")),
            MergeOutcome::BelowGcFloor
        );
        assert_eq!(a.visible("task", "t1"), None);
    }

    #[test]
    fn write_above_gc_floor_still_applies() {
        let mut a = Applier::new();
        a.merge(write(2, 2, Op::Delete, b""));
        a.collect_tombstone("task", "t1");
        assert_eq!(
            a.merge(write(3, 1, Op::Insert, b"legit")),
            MergeOutcome::Applied
        );
        assert_eq!(a.visible("task", "t1"), Some(&b"legit"[..]));
    }

    #[test]
    fn gc_floor_is_per_record() {
        let mut a = Applier::new();
        let del = StampedWrite {
            stamp: Stamp::new(Hlc::new(1, 0), peer(2)),
            op: Op::Delete,
            entity: "task".into(),
            id: "t1".into(),
            data: vec![],
        };
        a.merge(del);
        a.collect_tombstone("task", "t1");
        let other = StampedWrite {
            stamp: Stamp::new(Hlc::new(1, 0), peer(1)),
            op: Op::Insert,
            entity: "task".into(),
            id: "t2".into(),
            data: b"unrelated".to_vec(),
        };
        assert_eq!(a.merge(other), MergeOutcome::Applied);
        assert_eq!(a.visible("task", "t2"), Some(&b"unrelated"[..]));
    }
}
