use crate::hlc::PEER_ID_LEN;
use crate::sync_state::FramePersister;
use crate::wire::WriteFrame;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use std::io;
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
    #[error("replication log unreadable: {0}")]
    Load(#[from] io::Error),
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
    fn append(&self, frame: &WriteFrame) -> io::Result<()> {
        let bytes = frame
            .encode()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        self.frames
            .insert(&frame_key(frame)[..], bytes)
            .map_err(io::Error::other)?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(io::Error::other)
    }

    fn load(&self) -> io::Result<Vec<WriteFrame>> {
        self.frames
            .iter()
            .map(|guard| {
                let (_, value) = guard.into_inner().map_err(io::Error::other)?;
                WriteFrame::decode(&value)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
            })
            .collect()
    }
}
