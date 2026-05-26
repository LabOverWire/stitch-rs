use crate::lww::Op;
use crate::sync_state::SyncState;
use crate::wire::WriteFrame;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, mpsc};

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// One device's view of a scope: a single shared [`SyncState`] plus the set of
/// live peer sessions. Sessions share the state, so a write pulled in from one
/// peer is automatically served to every other peer on its next pull — that is
/// how transitive forwarding works without any per-session re-broadcast.
///
/// A local write applies to the shared state and is pushed to every live
/// session for low latency; the periodic pull in [`crate::session::run`] is the
/// correctness backstop.
#[derive(Clone)]
pub struct SyncNode {
    state: Arc<Mutex<SyncState>>,
    sessions: Arc<StdMutex<Vec<mpsc::UnboundedSender<WriteFrame>>>>,
}

impl SyncNode {
    #[must_use]
    pub fn new(self_id: crate::hlc::PeerId) -> Self {
        Self {
            state: Arc::new(Mutex::new(SyncState::new(self_id))),
            sessions: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    /// Build a node from explicit state, e.g. with a persister and/or signing
    /// identity already attached. Most callers use [`SyncNode::new`] or the
    /// `Store` constructors.
    #[must_use]
    pub fn from_state(state: SyncState) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
            sessions: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    /// Build a node whose state is rebuilt by replaying the persister's stored
    /// frames, then continues to persist new writes to it.
    #[must_use]
    pub fn with_persister(
        self_id: crate::hlc::PeerId,
        persister: Arc<dyn crate::sync_state::FramePersister>,
    ) -> Self {
        let mut state = SyncState::new(self_id);
        for frame in persister.load() {
            state.replay(frame);
        }
        state.set_persister(persister);
        Self::from_state(state)
    }

    #[must_use]
    pub fn state(&self) -> Arc<Mutex<SyncState>> {
        Arc::clone(&self.state)
    }

    /// Register a new peer connection. Returns the receiver to hand to
    /// [`crate::session::run`]; the matching sender is retained for fan-out.
    pub fn register_session(&self) -> mpsc::UnboundedReceiver<WriteFrame> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.sessions.lock().expect("sessions lock").push(tx);
        rx
    }

    /// Apply a local mutation and fan it out to every live session. Closed
    /// session channels are pruned.
    pub async fn local_write(
        &self,
        op: Op,
        entity: impl Into<String>,
        id: impl Into<String>,
        data: Vec<u8>,
    ) -> WriteFrame {
        let frame = {
            let mut guard = self.state.lock().await;
            guard.local_write(now_millis(), op, entity, id, data)
        };
        self.sessions
            .lock()
            .expect("sessions lock")
            .retain(|tx| tx.send(frame.clone()).is_ok());
        frame
    }

    #[must_use]
    pub async fn visible(&self, entity: &str, id: &str) -> Option<Vec<u8>> {
        self.state
            .lock()
            .await
            .visible(entity, id)
            .map(<[u8]>::to_vec)
    }

    /// Every visible record of an entity as `(id, data)`.
    #[must_use]
    pub async fn list_entity(&self, entity: &str) -> Vec<(String, Vec<u8>)> {
        self.state.lock().await.visible_entity(entity)
    }

    /// Subscribe to visible-state mutations (local and remote).
    pub async fn subscribe(&self) -> tokio::sync::broadcast::Receiver<crate::sync_state::MutationEvent> {
        self.state.lock().await.subscribe()
    }

    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.lock().expect("sessions lock").len()
    }
}
