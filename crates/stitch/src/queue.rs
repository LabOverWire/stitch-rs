use crate::error::Result;
use crate::types::{Operation, PendingMutation, Record};
use async_trait::async_trait;

/// A queued mutation the broker rejected with no retry left during flush — a
/// `Conflict` (409) or `Ownership` denial (403) on an insert, or a terminal
/// publish refusal on any op. The store rolls the optimistic local copy back to
/// `pre_image`: `None` deletes the phantom row (a rejected insert), `Some` the
/// prior state (a rejected update or delete) reverts or reinstates it.
#[derive(Debug, Clone)]
pub struct RejectedMutation {
    pub entity: String,
    pub id: String,
    pub scope_id: String,
    pub pre_image: Option<Record>,
}

/// Outcome of an offline-queue flush: how many mutations were retained (still
/// pending, needing another pass) and which mutations the broker rejected so the
/// caller can roll their local rows back.
#[derive(Debug, Default)]
pub struct FlushSummary {
    pub retained: usize,
    pub rejected_mutations: Vec<RejectedMutation>,
}

/// Sends a queued mutation to the remote during an offline-queue flush, and
/// reads/deletes local rows while reconciling. Implemented by the remote sync
/// layer.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait MutationSender: Send + Sync {
    async fn sync_create(&self, entity: &str, scope_id: &str, data: Record) -> Result<()>;
    async fn sync_update(&self, entity: &str, scope_id: &str, id: &str, data: Record)
    -> Result<()>;
    async fn sync_delete(&self, entity: &str, scope_id: &str, id: &str) -> Result<()>;
    async fn read_entity(&self, entity: &str, id: &str) -> Result<Option<Record>>;
    async fn delete_entity(&self, entity: &str, id: &str) -> Result<()>;
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait MutationSender {
    async fn sync_create(&self, entity: &str, scope_id: &str, data: Record) -> Result<()>;
    async fn sync_update(&self, entity: &str, scope_id: &str, id: &str, data: Record)
    -> Result<()>;
    async fn sync_delete(&self, entity: &str, scope_id: &str, id: &str) -> Result<()>;
    async fn read_entity(&self, entity: &str, id: &str) -> Result<Option<Record>>;
    async fn delete_entity(&self, entity: &str, id: &str) -> Result<()>;
}

/// Durable buffer of local mutations made while disconnected, flushed to the
/// remote on reconnect. The concrete implementations live in `offline_queue`
/// (native only for now); this trait is platform-neutral so cross-platform sync
/// code can accept an optional queue.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
pub trait OfflineQueue: Send + Sync {
    async fn queue(&self, mutation: PendingMutation) -> Result<()>;
    async fn remove(
        &self,
        entity: &str,
        entity_id: &str,
        scope_id: &str,
        op: Operation,
    ) -> Result<()>;
    async fn flush(&self, sender: &dyn MutationSender) -> Result<FlushSummary>;
    async fn clear(&self) -> Result<()>;
    async fn pending_for_scope(&self, scope_id: &str) -> Result<Vec<PendingMutation>>;
    async fn has_pending_insert(&self, entity: &str, entity_id: &str) -> Result<bool>;
    fn set_authenticated_user(&self, user_id: Option<String>);
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
pub trait OfflineQueue {
    async fn queue(&self, mutation: PendingMutation) -> Result<()>;
    async fn remove(
        &self,
        entity: &str,
        entity_id: &str,
        scope_id: &str,
        op: Operation,
    ) -> Result<()>;
    async fn flush(&self, sender: &dyn MutationSender) -> Result<FlushSummary>;
    async fn clear(&self) -> Result<()>;
    async fn pending_for_scope(&self, scope_id: &str) -> Result<Vec<PendingMutation>>;
    async fn has_pending_insert(&self, entity: &str, entity_id: &str) -> Result<bool>;
    fn set_authenticated_user(&self, user_id: Option<String>);
}
