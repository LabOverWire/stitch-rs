use crate::error::Result;
use crate::types::{Operation, PendingMutation, Record};
use async_trait::async_trait;

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
    async fn flush(&self, sender: &dyn MutationSender) -> Result<usize>;
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
    async fn flush(&self, sender: &dyn MutationSender) -> Result<usize>;
    async fn clear(&self) -> Result<()>;
    async fn pending_for_scope(&self, scope_id: &str) -> Result<Vec<PendingMutation>>;
    async fn has_pending_insert(&self, entity: &str, entity_id: &str) -> Result<bool>;
    fn set_authenticated_user(&self, user_id: Option<String>);
}
