use crate::hlc::PeerId;
use crate::lww::Op;
use crate::node::SyncNode;
use crate::sync_state::MutationEvent;
use serde_json::{Map, Value};
use tokio::sync::broadcast;

/// App-facing document store backed by the verified P2P sync engine.
///
/// Records are JSON objects keyed by `(entity, id)`. Conflict resolution is
/// last-writer-wins per record, ordered by Hybrid Logical Clock with a
/// peer-fingerprint tiebreak (see `spec/`). This is a sibling to
/// `stitch::Store` — same shape, but a multi-leader HLC engine rather than the
/// broker-authoritative version-LWW one, because the two conflict models can't
/// share an inbound-apply path.
///
/// Attach peers with [`crate::Swarm::spawn`] on [`Store::node`].
#[derive(Clone)]
pub struct Store {
    node: SyncNode,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("serialize: {0}")]
    Serialize(#[from] serde_json::Error),
}

impl Store {
    #[must_use]
    pub fn new(self_id: PeerId) -> Self {
        Self {
            node: SyncNode::new(self_id),
        }
    }

    /// The underlying node, for wiring discovery (`Swarm::spawn`).
    #[must_use]
    pub fn node(&self) -> &SyncNode {
        &self.node
    }

    /// Insert or replace a record.
    pub async fn create(
        &self,
        entity: &str,
        id: &str,
        record: Value,
    ) -> Result<(), StoreError> {
        let data = serde_json::to_vec(&record)?;
        self.node.local_write(Op::Insert, entity, id, data).await;
        Ok(())
    }

    /// Read a record from local converged state.
    #[must_use]
    pub async fn read(&self, entity: &str, id: &str) -> Option<Value> {
        let bytes = self.node.visible(entity, id).await?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Merge `fields` into an existing record (read-merge-write). If the record
    /// is absent, the fields become the new record. Last-writer-wins per record
    /// under concurrent edits.
    pub async fn update(
        &self,
        entity: &str,
        id: &str,
        fields: Map<String, Value>,
    ) -> Result<(), StoreError> {
        let mut merged = match self.read(entity, id).await {
            Some(Value::Object(existing)) => existing,
            _ => Map::new(),
        };
        for (k, v) in fields {
            merged.insert(k, v);
        }
        let data = serde_json::to_vec(&Value::Object(merged))?;
        self.node.local_write(Op::Update, entity, id, data).await;
        Ok(())
    }

    /// Delete a record (writes a tombstone).
    pub async fn delete(&self, entity: &str, id: &str) {
        self.node.local_write(Op::Delete, entity, id, Vec::new()).await;
    }

    /// All visible records of an entity.
    #[must_use]
    pub async fn list(&self, entity: &str) -> Vec<Value> {
        self.node
            .list_entity(entity)
            .await
            .into_iter()
            .filter_map(|(_, bytes)| serde_json::from_slice(&bytes).ok())
            .collect()
    }

    /// Subscribe to mutation events (local and from peers).
    pub async fn subscribe(&self) -> broadcast::Receiver<MutationEvent> {
        self.node.subscribe().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::PEER_ID_LEN;
    use serde_json::json;

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; PEER_ID_LEN];
        id[0] = n;
        id
    }

    #[tokio::test]
    async fn create_read_round_trip() {
        let store = Store::new(peer(1));
        store
            .create("task", "t1", json!({"title": "hi", "done": false}))
            .await
            .unwrap();
        let got = store.read("task", "t1").await.unwrap();
        assert_eq!(got, json!({"title": "hi", "done": false}));
    }

    #[tokio::test]
    async fn update_merges_fields() {
        let store = Store::new(peer(1));
        store.create("task", "t1", json!({"title": "hi", "done": false})).await.unwrap();
        let mut fields = Map::new();
        fields.insert("done".into(), json!(true));
        store.update("task", "t1", fields).await.unwrap();
        let got = store.read("task", "t1").await.unwrap();
        assert_eq!(got, json!({"title": "hi", "done": true}));
    }

    #[tokio::test]
    async fn delete_removes_record() {
        let store = Store::new(peer(1));
        store.create("task", "t1", json!({"x": 1})).await.unwrap();
        store.delete("task", "t1").await;
        assert_eq!(store.read("task", "t1").await, None);
    }

    #[tokio::test]
    async fn list_returns_visible_records() {
        let store = Store::new(peer(1));
        store.create("task", "t1", json!({"n": 1})).await.unwrap();
        store.create("task", "t2", json!({"n": 2})).await.unwrap();
        store.create("note", "n1", json!({"n": 3})).await.unwrap();
        store.delete("task", "t2").await;
        let tasks = store.list("task").await;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0], json!({"n": 1}));
    }

    #[tokio::test]
    async fn subscribe_observes_local_writes() {
        let store = Store::new(peer(1));
        let mut rx = store.subscribe().await;
        store.create("task", "t1", json!({"x": 1})).await.unwrap();
        let event = rx.recv().await.unwrap();
        assert_eq!(event.entity, "task");
        assert_eq!(event.id, "t1");
        assert_eq!(event.op, Op::Insert);
    }
}
