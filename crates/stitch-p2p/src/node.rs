use crate::lww::Op;
use crate::replog::RecordOutcome;
use crate::sync_state::{SyncState, WriteError};
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
    ///
    /// # Errors
    ///
    /// When the persister cannot load its frames. Starting without them would
    /// reuse sequence numbers peers already hold.
    pub fn with_persister(
        self_id: crate::hlc::PeerId,
        persister: Arc<dyn crate::sync_state::FramePersister>,
    ) -> std::io::Result<Self> {
        let mut state = SyncState::new(self_id);
        for frame in persister.load()? {
            state.replay(frame);
        }
        state.set_persister(persister);
        Ok(Self::from_state(state))
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
    ///
    /// # Errors
    ///
    /// See [`SyncState::local_write`]; a refused write is not sent.
    pub async fn local_write(
        &self,
        op: Op,
        entity: impl Into<String>,
        id: impl Into<String>,
        data: Vec<u8>,
    ) -> Result<WriteFrame, WriteError> {
        let frame = {
            let mut guard = self.state.lock().await;
            guard.local_write(now_millis(), op, entity, id, data)?
        };
        self.sessions
            .lock()
            .expect("sessions lock")
            .retain(|tx| tx.send(frame.clone()).is_ok());
        Ok(frame)
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

    /// The visible record plus the peer that authored the winning write.
    #[must_use]
    pub async fn visible_with_author(
        &self,
        entity: &str,
        id: &str,
    ) -> Option<(Vec<u8>, crate::hlc::PeerId)> {
        self.state.lock().await.visible_with_author(entity, id)
    }

    /// Every visible record of an entity as `(id, data, author)`.
    #[must_use]
    pub async fn entries_with_authors(
        &self,
        entity: &str,
    ) -> Vec<(String, Vec<u8>, crate::hlc::PeerId)> {
        self.state.lock().await.entries_with_authors(entity)
    }

    /// Every frame held for an entity, across all origins, for presenting a
    /// signed proof to a peer that has not yet synced the records.
    #[must_use]
    pub async fn frames_for_entity(&self, entity: &str) -> Vec<WriteFrame> {
        self.state.lock().await.frames_for_entity(entity)
    }

    /// Apply externally supplied frames through the normal received-frame path
    /// (verification, HLC observation, LWW merge) and fan the newly appended
    /// ones out to live sessions. Returns how many were appended.
    pub async fn ingest(&self, frames: Vec<WriteFrame>) -> usize {
        let now = now_millis();
        let mut applied = Vec::new();
        {
            let mut guard = self.state.lock().await;
            for frame in frames {
                if guard.receive(frame.clone(), now) == RecordOutcome::Appended {
                    applied.push(frame);
                }
            }
        }
        if !applied.is_empty() {
            self.sessions
                .lock()
                .expect("sessions lock")
                .retain(|tx| applied.iter().all(|frame| tx.send(frame.clone()).is_ok()));
        }
        applied.len()
    }

    /// Subscribe to visible-state mutations (local and remote).
    pub async fn subscribe(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::sync_state::MutationEvent> {
        self.state.lock().await.subscribe()
    }

    /// Reclaim replication-log prefixes every listed member has delivered.
    /// Returns frames dropped. See [`crate::sync_state::SyncState::reclaim`].
    pub async fn reclaim(&self, members: &[crate::hlc::PeerId]) -> usize {
        self.state.lock().await.reclaim(members)
    }

    /// Live sessions: those whose receiver has not been dropped.
    #[must_use]
    pub fn session_count(&self) -> usize {
        let mut sessions = self.sessions.lock().expect("sessions lock");
        sessions.retain(|tx| !tx.is_closed());
        sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Unreadable;

    impl crate::sync_state::FramePersister for Unreadable {
        fn append(&self, _: &WriteFrame) -> std::io::Result<()> {
            Ok(())
        }

        fn load(&self) -> std::io::Result<Vec<WriteFrame>> {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "corrupt frame",
            ))
        }
    }

    #[test]
    fn session_count_skips_dropped_sessions() {
        let node = SyncNode::new([1; crate::hlc::PEER_ID_LEN]);
        let live = node.register_session();
        drop(node.register_session());
        assert_eq!(node.session_count(), 1);
        drop(live);
        assert_eq!(node.session_count(), 0);
    }

    #[test]
    fn a_log_that_cannot_be_loaded_is_an_error() {
        let opened = SyncNode::with_persister([1; crate::hlc::PEER_ID_LEN], Arc::new(Unreadable));
        assert!(opened.is_err());
    }
}
