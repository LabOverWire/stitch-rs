use std::cmp::Ordering;

pub const PEER_ID_LEN: usize = 32;

pub type PeerId = [u8; PEER_ID_LEN];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Hlc {
    pub physical: u64,
    pub logical: u32,
}

impl Hlc {
    #[must_use]
    pub fn new(physical: u64, logical: u32) -> Self {
        Self { physical, logical }
    }

    pub fn tick(&mut self, wall_ms: u64) -> Self {
        let prev = *self;
        self.physical = prev.physical.max(wall_ms);
        self.logical = if self.physical == prev.physical {
            prev.logical + 1
        } else {
            0
        };
        *self
    }

    pub fn observe(&mut self, remote: Hlc, wall_ms: u64) -> Self {
        let prev = *self;
        let physical = prev.physical.max(remote.physical).max(wall_ms);
        self.physical = physical;
        self.logical = if physical == prev.physical && physical == remote.physical {
            prev.logical.max(remote.logical) + 1
        } else if physical == prev.physical {
            prev.logical + 1
        } else if physical == remote.physical {
            remote.logical + 1
        } else {
            0
        };
        *self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Stamp {
    pub hlc: Hlc,
    pub peer: PeerId,
}

impl Stamp {
    #[must_use]
    pub fn new(hlc: Hlc, peer: PeerId) -> Self {
        Self { hlc, peer }
    }
}

impl PartialOrd for Stamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Stamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hlc
            .cmp(&other.hlc)
            .then_with(|| self.peer.cmp(&other.peer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; PEER_ID_LEN];
        id[0] = n;
        id
    }

    #[test]
    fn tick_advances_physical_to_wall_clock() {
        let mut c = Hlc::default();
        let stamped = c.tick(100);
        assert_eq!(stamped, Hlc::new(100, 0));
    }

    #[test]
    fn tick_bumps_logical_when_wall_clock_stalls() {
        let mut c = Hlc::new(100, 0);
        let a = c.tick(100);
        let b = c.tick(100);
        assert_eq!(a, Hlc::new(100, 1));
        assert_eq!(b, Hlc::new(100, 2));
    }

    #[test]
    fn tick_never_goes_backward() {
        let mut c = Hlc::new(500, 3);
        let stamped = c.tick(100);
        assert_eq!(stamped, Hlc::new(500, 4));
    }

    #[test]
    fn observe_takes_max_physical_and_bumps_logical() {
        let mut c = Hlc::new(100, 5);
        let stamped = c.observe(Hlc::new(100, 9), 100);
        assert_eq!(stamped, Hlc::new(100, 10));
    }

    #[test]
    fn observe_jumps_to_remote_future() {
        let mut c = Hlc::new(100, 0);
        let stamped = c.observe(Hlc::new(900, 2), 50);
        assert_eq!(stamped, Hlc::new(900, 3));
    }

    #[test]
    fn observe_then_tick_is_strictly_greater_than_remote() {
        let remote = Hlc::new(900, 2);
        let mut c = Hlc::new(100, 0);
        c.observe(remote, 50);
        let next = c.tick(50);
        assert!(next > remote);
    }

    #[test]
    fn stamp_orders_by_hlc_then_peer() {
        let lo = Stamp::new(Hlc::new(1, 0), peer(2));
        let hi = Stamp::new(Hlc::new(1, 0), peer(9));
        assert!(hi > lo);

        let earlier = Stamp::new(Hlc::new(1, 0), peer(9));
        let later = Stamp::new(Hlc::new(2, 0), peer(1));
        assert!(later > earlier);
    }
}
