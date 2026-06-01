//! In-process multi-peer cluster for exercising the sync engine under load and
//! chaos. Peers run the real session protocol over duplex pipes (no QUIC), so
//! the harness is deterministic in workload, fast, and supports precise
//! partition injection — toggling individual links on and off.
//!
//! Peer 0 is the project owner; the rest join and are invited as members, so
//! membership filtering is exercised alongside CRUD conflicts and tombstones.

use crate::TaskBoard;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::HashMap;
use std::time::Duration;
use stitch_p2p::membership::Role;
use stitch_p2p::{Identity, PeerId, Store, session};
use tokio::io::split;
use tokio::task::JoinHandle;

fn link_key(a: usize, b: usize) -> (usize, usize) {
    if a < b { (a, b) } else { (b, a) }
}

/// A cluster of in-process peers, each a [`TaskBoard`], with toggleable links.
pub struct Cluster {
    boards: Vec<TaskBoard>,
    ids: Vec<PeerId>,
    pull: Duration,
    links: HashMap<(usize, usize), Vec<JoinHandle<()>>>,
}

impl Cluster {
    /// Build `n` peers. Peer 0 owns the project; peers `1..n` join it.
    #[must_use]
    pub fn new(n: usize, pull: Duration) -> Self {
        let mut boards = Vec::with_capacity(n);
        let mut ids = Vec::with_capacity(n);
        let owner_identity = Identity::from_seed(seed(0));
        let owner_id = owner_identity.peer_id();
        boards.push(TaskBoard::new(Store::with_owner(owner_identity)));
        ids.push(owner_id);
        for i in 1..n {
            let identity = Identity::from_seed(seed(i));
            ids.push(identity.peer_id());
            boards.push(TaskBoard::new(Store::join(identity, owner_id)));
        }
        Self {
            boards,
            ids,
            pull,
            links: HashMap::new(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.boards.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.boards.is_empty()
    }

    #[must_use]
    pub fn board(&self, i: usize) -> &TaskBoard {
        &self.boards[i]
    }

    #[must_use]
    pub fn peer_id(&self, i: usize) -> PeerId {
        self.ids[i]
    }

    /// Owner invites every other peer as a member.
    pub async fn invite_all(&self) {
        for i in 1..self.boards.len() {
            self.boards[0]
                .store()
                .invite(self.ids[i], Role::Member)
                .await;
        }
    }

    /// Owner revokes peer `i`.
    pub async fn revoke(&self, i: usize) {
        self.boards[0].store().revoke(self.ids[i]).await;
    }

    /// Establish a bidirectional session between peers `a` and `b` (idempotent).
    pub fn connect(&mut self, a: usize, b: usize) {
        let key = link_key(a, b);
        if a == b || self.links.contains_key(&key) {
            return;
        }
        let (io_a, io_b) = tokio::io::duplex(256 * 1024);
        let mut handles = Vec::with_capacity(2);
        for (peer, io) in [(a, io_a), (b, io_b)] {
            let rx = self.boards[peer].store().node().register_session();
            let (r, w) = split(io);
            let state = self.boards[peer].store().node().state();
            let pull = self.pull;
            handles.push(tokio::spawn(async move {
                let _ = session::run(state, r, w, rx, pull).await;
            }));
        }
        self.links.insert(key, handles);
    }

    /// Tear down the session between `a` and `b` (partition).
    pub fn partition(&mut self, a: usize, b: usize) {
        if let Some(handles) = self.links.remove(&link_key(a, b)) {
            for h in handles {
                h.abort();
            }
        }
    }

    /// Fully connect every pair of peers.
    pub fn full_mesh(&mut self) {
        for a in 0..self.boards.len() {
            for b in (a + 1)..self.boards.len() {
                self.connect(a, b);
            }
        }
    }

    #[must_use]
    pub fn active_links(&self) -> usize {
        self.links.len()
    }

    /// Every peer's board snapshot, indexed by peer.
    pub async fn snapshots(&self) -> Vec<std::collections::BTreeMap<String, crate::Task>> {
        let mut out = Vec::with_capacity(self.boards.len());
        for board in &self.boards {
            out.push(board.snapshot().await);
        }
        out
    }

    /// True once every peer's board is identical. The converged invariant.
    pub async fn converged(&self) -> bool {
        let snaps = self.snapshots().await;
        snaps.windows(2).all(|w| w[0] == w[1])
    }

    /// Poll until all peers converge or the timeout elapses.
    pub async fn await_convergence(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.converged().await {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Abort all sessions (use at the end of a run).
    pub fn shutdown(&mut self) {
        for handles in self.links.values() {
            for h in handles {
                h.abort();
            }
        }
        self.links.clear();
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn seed(i: usize) -> [u8; 32] {
    let mut s = [0u8; 32];
    s[0] = 1 + i as u8;
    s
}

/// Knobs for a chaos run.
#[derive(Debug, Clone, Copy)]
pub struct Chaos {
    pub peers: usize,
    pub rounds: usize,
    pub id_pool: usize,
    pub seed: u64,
    pub pull: Duration,
}

impl Default for Chaos {
    fn default() -> Self {
        Self {
            peers: 4,
            rounds: 240,
            id_pool: 6,
            seed: 0,
            pull: Duration::from_millis(15),
        }
    }
}

/// What a chaos run did, for reporting.
#[derive(Debug, Default, Clone, Copy)]
pub struct ChaosReport {
    pub adds: u64,
    pub renames: u64,
    pub toggles: u64,
    pub removes: u64,
    pub partitions: u64,
    pub heals: u64,
    pub revokes: u64,
    pub converged: bool,
    pub final_tasks: usize,
}

/// Run a randomized, partition-and-membership-churning workload against a fresh
/// cluster, then heal, restore membership, and wait for convergence. Returns a
/// report; `report.converged` is the invariant a caller asserts.
pub async fn run_chaos(cfg: Chaos) -> (Chaos, ChaosReport) {
    let mut cluster = Cluster::new(cfg.peers, cfg.pull);
    cluster.full_mesh();
    cluster.invite_all().await;

    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let ids: Vec<String> = (0..cfg.id_pool).map(|i| format!("t{i}")).collect();
    let mut report = ChaosReport::default();

    for _ in 0..cfg.rounds {
        let p = rng.random_range(0..cfg.peers);
        let board = cluster.board(p).clone();
        let id = ids[rng.random_range(0..cfg.id_pool)].clone();
        match rng.random_range(0..4u8) {
            0 => {
                board.add(&id, &format!("t-{}", rng.random::<u16>())).await;
                report.adds += 1;
            }
            1 => {
                board
                    .rename(&id, &format!("r-{}", rng.random::<u16>()))
                    .await;
                report.renames += 1;
            }
            2 => {
                board.set_done(&id, rng.random::<bool>()).await;
                report.toggles += 1;
            }
            _ => {
                board.remove(&id).await;
                report.removes += 1;
            }
        }

        if rng.random_range(0..6u8) == 0 {
            let a = rng.random_range(0..cfg.peers);
            let b = rng.random_range(0..cfg.peers);
            if a != b {
                if rng.random::<bool>() {
                    cluster.partition(a, b);
                    report.partitions += 1;
                } else {
                    cluster.connect(a, b);
                    report.heals += 1;
                }
            }
        }

        if cfg.peers > 1 && rng.random_range(0..40u8) == 0 {
            cluster.revoke(rng.random_range(1..cfg.peers)).await;
            report.revokes += 1;
        }

        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    cluster.full_mesh();
    cluster.invite_all().await;
    report.converged = cluster.await_convergence(Duration::from_secs(20)).await;
    report.final_tasks = cluster.board(0).len().await;
    (cfg, report)
}
