//! A collaborative task board on top of [`stitch_p2p`].
//!
//! Each task is a JSON document in the `task` entity, keyed by a unique id.
//! Edits resolve last-writer-wins (HLC); deletes are tombstones; project
//! membership (owner-controlled) gates which peers' tasks are visible. The
//! board is a thin typed wrapper over [`stitch_p2p::Store`] — the point is to
//! exercise the sync engine with a realistic, conflict-prone workload (see the
//! `soak` harness).

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use stitch_p2p::Store;

pub mod harness;

pub const ENTITY: &str = "task";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub title: String,
    pub done: bool,
}

/// A task board view backed by a [`Store`]. Cloneable; clones share the store.
#[derive(Clone)]
pub struct TaskBoard {
    store: Store,
}

impl TaskBoard {
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    #[must_use]
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Add a task under `id` (caller supplies the id so the harness can pick
    /// collision-prone ids deliberately).
    pub async fn add(&self, id: &str, title: &str) {
        let _ = self
            .store
            .create(ENTITY, id, json!({ "title": title, "done": false }))
            .await;
    }

    pub async fn rename(&self, id: &str, title: &str) {
        let mut fields = serde_json::Map::new();
        fields.insert("title".into(), json!(title));
        let _ = self.store.update(ENTITY, id, fields).await;
    }

    pub async fn set_done(&self, id: &str, done: bool) {
        let mut fields = serde_json::Map::new();
        fields.insert("done".into(), json!(done));
        let _ = self.store.update(ENTITY, id, fields).await;
    }

    pub async fn remove(&self, id: &str) {
        self.store.delete(ENTITY, id).await;
    }

    #[must_use]
    pub async fn get(&self, id: &str) -> Option<Task> {
        let value = self.store.read(ENTITY, id).await?;
        serde_json::from_value(value).ok()
    }

    /// The current board as an ordered `id -> Task` map — the canonical form
    /// for comparing two peers' views.
    #[must_use]
    pub async fn snapshot(&self) -> BTreeMap<String, Task> {
        self.store
            .entries(ENTITY)
            .await
            .into_iter()
            .filter_map(|(id, value)| serde_json::from_value(value).ok().map(|t| (id, t)))
            .collect()
    }

    #[must_use]
    pub async fn len(&self) -> usize {
        self.snapshot().await.len()
    }

    #[must_use]
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stitch_p2p::{PEER_ID_LEN, PeerId};

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; PEER_ID_LEN];
        id[0] = n;
        id
    }

    #[tokio::test]
    async fn add_get_snapshot() {
        let board = TaskBoard::new(Store::new(peer(1)));
        board.add("t1", "write tests").await;
        board.add("t2", "ship it").await;
        assert_eq!(
            board.get("t1").await,
            Some(Task {
                title: "write tests".into(),
                done: false
            })
        );
        assert_eq!(board.len().await, 2);
    }

    #[tokio::test]
    async fn rename_toggle_remove() {
        let board = TaskBoard::new(Store::new(peer(1)));
        board.add("t1", "draft").await;
        board.rename("t1", "final").await;
        board.set_done("t1", true).await;
        assert_eq!(
            board.get("t1").await,
            Some(Task {
                title: "final".into(),
                done: true
            })
        );
        board.remove("t1").await;
        assert_eq!(board.get("t1").await, None);
        assert!(board.is_empty().await);
    }
}
