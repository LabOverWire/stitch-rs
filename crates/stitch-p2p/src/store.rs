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
    /// Genesis owner when membership filtering is enabled; `None` = open store
    /// (every validly-signed author's records are visible).
    #[cfg_attr(not(feature = "membership"), allow(dead_code))]
    owner: Option<PeerId>,
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
            owner: None,
        }
    }

    /// Open a durable store: rebuilds state from the fjall log at `path` and
    /// persists every subsequent write.
    #[cfg(feature = "persistence")]
    pub fn open(
        self_id: PeerId,
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, crate::persistence::PersistError> {
        let log = std::sync::Arc::new(crate::persistence::FjallLog::open(path)?);
        Ok(Self {
            node: SyncNode::with_persister(self_id, log),
            owner: None,
        })
    }

    /// A store whose writes are Ed25519-signed by `identity` and whose peer id
    /// is the identity's public key. Received writes failing verification are
    /// rejected. No membership filtering — see [`Store::with_owner`].
    #[cfg(feature = "membership")]
    #[must_use]
    pub fn with_identity(identity: crate::membership::Identity) -> Self {
        let mut state = crate::SyncState::new(identity.peer_id());
        state.set_auth(std::sync::Arc::new(identity));
        Self {
            node: SyncNode::from_state(state),
            owner: None,
        }
    }

    /// A store where this identity is the genesis owner of the scope. Writes are
    /// signed; reads are filtered to records authored by authorized members
    /// (the owner, plus peers it or its admins invited). See [`Store::invite`].
    #[cfg(feature = "membership")]
    #[must_use]
    pub fn with_owner(identity: crate::membership::Identity) -> Self {
        let owner = identity.peer_id();
        let mut state = crate::SyncState::new(owner);
        state.set_auth(std::sync::Arc::new(identity));
        Self {
            node: SyncNode::from_state(state),
            owner: Some(owner),
        }
    }

    /// A store that joins a scope owned by `owner`. Writes are signed by
    /// `identity`; reads filter by the same membership rules. The peer becomes
    /// effective once the owner (or an admin) has invited it and that record
    /// has replicated in.
    #[cfg(feature = "membership")]
    #[must_use]
    pub fn join(identity: crate::membership::Identity, owner: PeerId) -> Self {
        let mut state = crate::SyncState::new(identity.peer_id());
        state.set_auth(std::sync::Arc::new(identity));
        Self {
            node: SyncNode::from_state(state),
            owner: Some(owner),
        }
    }

    /// Durable + signed: replays the fjall log at `path`, persists new writes,
    /// signs with `identity`, and verifies inbound writes.
    #[cfg(all(feature = "membership", feature = "persistence"))]
    pub fn open_with_identity(
        identity: crate::membership::Identity,
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, crate::persistence::PersistError> {
        let log = std::sync::Arc::new(crate::persistence::FjallLog::open(path)?);
        let mut state = crate::SyncState::new(identity.peer_id());
        for frame in crate::FramePersister::load(log.as_ref()) {
            state.replay(frame);
        }
        state.set_persister(log);
        state.set_auth(std::sync::Arc::new(identity));
        Ok(Self {
            node: SyncNode::from_state(state),
            owner: None,
        })
    }

    /// The underlying node, for wiring discovery (`Swarm::spawn`).
    #[must_use]
    pub fn node(&self) -> &SyncNode {
        &self.node
    }

    /// Insert or replace a record.
    pub async fn create(&self, entity: &str, id: &str, record: Value) -> Result<(), StoreError> {
        let data = serde_json::to_vec(&record)?;
        self.node.local_write(Op::Insert, entity, id, data).await;
        Ok(())
    }

    /// Read a record from local converged state. When the store has an owner,
    /// records authored by non-members are filtered out.
    #[must_use]
    pub async fn read(&self, entity: &str, id: &str) -> Option<Value> {
        let (bytes, author) = self.node.visible_with_author(entity, id).await?;
        if !self.author_authorized(author).await {
            return None;
        }
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
        self.node
            .local_write(Op::Delete, entity, id, Vec::new())
            .await;
    }

    /// All visible records of an entity, filtered to authorized authors when the
    /// store has an owner.
    #[must_use]
    pub async fn list(&self, entity: &str) -> Vec<Value> {
        self.entries(entity)
            .await
            .into_iter()
            .map(|(_, value)| value)
            .collect()
    }

    /// All visible `(id, value)` records of an entity, filtered to authorized
    /// authors when the store has an owner.
    #[must_use]
    pub async fn entries(&self, entity: &str) -> Vec<(String, Value)> {
        let authorized = self.authorized_authors().await;
        self.node
            .entries_with_authors(entity)
            .await
            .into_iter()
            .filter(|(_, _, author)| match &authorized {
                Some(set) => set.contains_key(author),
                None => true,
            })
            .filter_map(|(id, bytes, _)| serde_json::from_slice(&bytes).ok().map(|v| (id, v)))
            .collect()
    }

    /// Invite a peer at `role` (owner/admin only — enforced when authorization
    /// is evaluated). No-op semantics if the caller lacks authority: the record
    /// is written but ignored by `authorized_members`.
    #[cfg(feature = "membership")]
    pub async fn invite(&self, target: PeerId, role: crate::membership::Role) {
        let id = crate::membership::member_record_id(&target);
        self.node
            .local_write(
                Op::Insert,
                crate::membership::MEMBERS_ENTITY,
                &id,
                vec![role.as_byte()],
            )
            .await;
    }

    /// Revoke a peer's membership.
    #[cfg(feature = "membership")]
    pub async fn revoke(&self, target: PeerId) {
        let id = crate::membership::member_record_id(&target);
        self.node
            .local_write(
                Op::Delete,
                crate::membership::MEMBERS_ENTITY,
                &id,
                Vec::new(),
            )
            .await;
    }

    async fn author_authorized(&self, author: PeerId) -> bool {
        match self.authorized_authors().await {
            Some(set) => set.contains_key(&author),
            None => true,
        }
    }

    #[cfg(feature = "membership")]
    async fn authorized_authors(
        &self,
    ) -> Option<std::collections::HashMap<PeerId, crate::membership::Role>> {
        let owner = self.owner?;
        let records: Vec<(PeerId, PeerId, crate::membership::Role)> = self
            .node
            .entries_with_authors(crate::membership::MEMBERS_ENTITY)
            .await
            .into_iter()
            .filter_map(|(id, data, granter)| {
                let target = crate::membership::peer_from_record_id(&id)?;
                let role = crate::membership::Role::from_byte(*data.first()?)?;
                Some((target, granter, role))
            })
            .collect();
        Some(crate::membership::authorized_members(owner, &records))
    }

    #[cfg(not(feature = "membership"))]
    #[allow(clippy::unused_async)]
    async fn authorized_authors(&self) -> Option<std::collections::HashMap<PeerId, ()>> {
        None
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
        store
            .create("task", "t1", json!({"title": "hi", "done": false}))
            .await
            .unwrap();
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
