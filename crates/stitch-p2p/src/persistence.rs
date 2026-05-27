use crate::hlc::PEER_ID_LEN;
use crate::sync_state::FramePersister;
use crate::wire::WriteFrame;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use std::path::Path;

const KEY_LEN: usize = PEER_ID_LEN + 8;

/// Durable replication log on fjall. Frames are stored keyed by
/// `origin_fingerprint || seq` (big-endian), so iteration yields each origin's
/// log in sequence order — exactly the order [`SyncState::replay`] needs.
///
/// [`SyncState::replay`]: crate::sync_state::SyncState::replay
pub struct FjallLog {
    db: Database,
    frames: Keyspace,
}

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("fjall: {0}")]
    Fjall(#[from] fjall::Error),
}

impl FjallLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PersistError> {
        let db = Database::builder(path.as_ref()).open()?;
        let frames = db.keyspace("frames", KeyspaceCreateOptions::default)?;
        Ok(Self { db, frames })
    }
}

fn frame_key(frame: &WriteFrame) -> [u8; KEY_LEN] {
    let mut key = [0u8; KEY_LEN];
    key[..PEER_ID_LEN].copy_from_slice(&frame.stamp.peer);
    key[PEER_ID_LEN..].copy_from_slice(&frame.seq.to_be_bytes());
    key
}

impl FramePersister for FjallLog {
    fn append(&self, frame: &WriteFrame) {
        let Ok(bytes) = frame.encode() else {
            return;
        };
        if self.frames.insert(&frame_key(frame)[..], bytes).is_ok() {
            let _ = self.db.persist(PersistMode::SyncAll);
        }
    }

    fn load(&self) -> Vec<WriteFrame> {
        let mut out = Vec::new();
        for guard in self.frames.iter() {
            let Ok((_, value)) = guard.into_inner() else {
                continue;
            };
            if let Ok(frame) = WriteFrame::decode(&value) {
                out.push(frame);
            }
        }
        out
    }
}
