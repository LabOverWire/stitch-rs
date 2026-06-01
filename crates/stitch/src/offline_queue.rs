use crate::error::Result;
use crate::origin::Origin;
use crate::persistence::PersistenceLayer;
pub use crate::queue::{MutationSender, OfflineQueue};
use crate::types::{Operation, PendingMutation, Record};
use async_trait::async_trait;
use mqdb_core::types::{Filter, FilterOp};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const PENDING_ENTITY: &str = "pending_sync";

#[derive(Debug, Clone)]
struct ConsolidatedMutation {
    op: Operation,
    entity: String,
    id: String,
    scope_id: String,
    data: Option<Record>,
    record_ids: Vec<String>,
    min_created_at: u64,
}

fn op_label(op: Operation) -> &'static str {
    match op {
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
    }
}

fn parse_op(label: &str) -> Option<Operation> {
    match label {
        "insert" => Some(Operation::Insert),
        "update" => Some(Operation::Update),
        "delete" => Some(Operation::Delete),
        _ => None,
    }
}

fn now_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[derive(Clone)]
struct StoredRow {
    record_id: String,
    op: Operation,
    entity: String,
    entity_id: String,
    scope_id: String,
    data: Option<Record>,
    created_at: u64,
}

fn consolidate(rows: Vec<StoredRow>) -> Vec<ConsolidatedMutation> {
    let mut groups: HashMap<(String, String), Vec<StoredRow>> = HashMap::new();
    for row in rows {
        let key = (row.entity.clone(), row.entity_id.clone());
        groups.entry(key).or_default().push(row);
    }

    let mut result: Vec<ConsolidatedMutation> = Vec::new();
    for (_, mut entries) in groups {
        entries.sort_by_key(|e| e.created_at);
        let min_created_at = entries.first().map(|e| e.created_at).unwrap_or(0);
        let record_ids: Vec<String> = entries.iter().map(|e| e.record_id.clone()).collect();
        let entity = entries[0].entity.clone();
        let entity_id = entries[0].entity_id.clone();
        let scope_id = entries[0].scope_id.clone();

        let has_insert = entries.iter().any(|e| e.op == Operation::Insert);
        let has_delete = entries.iter().any(|e| e.op == Operation::Delete);

        let consolidated = if has_insert && has_delete {
            ConsolidatedMutation {
                op: Operation::Delete,
                entity,
                id: entity_id,
                scope_id,
                data: None,
                record_ids,
                min_created_at,
            }
        } else if has_insert {
            let mut merged: Record = entries
                .iter()
                .find(|e| e.op == Operation::Insert)
                .and_then(|e| e.data.clone())
                .unwrap_or_default();
            for entry in &entries {
                if entry.op != Operation::Insert
                    && let Some(data) = &entry.data
                {
                    for (k, v) in data {
                        merged.insert(k.clone(), v.clone());
                    }
                }
            }
            ConsolidatedMutation {
                op: Operation::Insert,
                entity,
                id: entity_id,
                scope_id,
                data: Some(merged),
                record_ids,
                min_created_at,
            }
        } else if has_delete {
            ConsolidatedMutation {
                op: Operation::Delete,
                entity,
                id: entity_id,
                scope_id,
                data: None,
                record_ids,
                min_created_at,
            }
        } else {
            let mut merged: Record = Record::new();
            for entry in &entries {
                if let Some(data) = &entry.data {
                    for (k, v) in data {
                        merged.insert(k.clone(), v.clone());
                    }
                }
            }
            ConsolidatedMutation {
                op: Operation::Update,
                entity,
                id: entity_id,
                scope_id,
                data: if merged.is_empty() {
                    None
                } else {
                    Some(merged)
                },
                record_ids,
                min_created_at,
            }
        };
        result.push(consolidated);
    }

    result.sort_by(|a, b| {
        a.min_created_at
            .cmp(&b.min_created_at)
            .then_with(|| a.op.priority().cmp(&b.op.priority()))
    });
    result
}

async fn flush_consolidated(
    consolidated: Vec<ConsolidatedMutation>,
    sender: &dyn MutationSender,
    root_entity: &str,
    mut remove_records: impl FnMut(Vec<String>) -> futures::future::BoxFuture<'static, ()>,
) -> usize {
    use futures::FutureExt;
    let mut retained = 0usize;
    for mutation in consolidated {
        let attempt = match mutation.op {
            Operation::Insert => {
                if let Some(data) = mutation.data.clone() {
                    sender
                        .sync_create(&mutation.entity, &mutation.scope_id, data)
                        .await
                } else {
                    Ok(())
                }
            }
            Operation::Update => {
                if let Some(data) = mutation.data.clone() {
                    sender
                        .sync_update(&mutation.entity, &mutation.scope_id, &mutation.id, data)
                        .await
                } else {
                    Ok(())
                }
            }
            Operation::Delete => {
                sender
                    .sync_delete(&mutation.entity, &mutation.scope_id, &mutation.id)
                    .await
            }
        };

        let outcome = match attempt {
            Ok(()) => FlushOutcome::Drop,
            Err(err) if err.is_transient() => FlushOutcome::Keep,
            Err(err) if err.is_ownership() => FlushOutcome::Drop,
            Err(err) if err.is_not_found() && mutation.op == Operation::Delete => {
                FlushOutcome::Drop
            }
            Err(err)
                if err.is_not_found()
                    && mutation.op == Operation::Update
                    && mutation.entity == root_entity =>
            {
                let _ = sender.delete_entity(&mutation.entity, &mutation.id).await;
                FlushOutcome::Drop
            }
            Err(err) if err.is_not_found() && mutation.op == Operation::Update => {
                match sender.read_entity(&mutation.entity, &mutation.id).await {
                    Ok(Some(full)) => match sender
                        .sync_create(&mutation.entity, &mutation.scope_id, full)
                        .await
                    {
                        Ok(()) => FlushOutcome::Drop,
                        Err(e) if e.is_transient() => FlushOutcome::Keep,
                        Err(_) => FlushOutcome::Drop,
                    },
                    Ok(None) => FlushOutcome::Drop,
                    Err(e) if e.is_transient() => FlushOutcome::Keep,
                    Err(_) => FlushOutcome::Drop,
                }
            }
            Err(err) if err.is_conflict() && mutation.op == Operation::Insert => {
                if let Some(data) = mutation.data.clone() {
                    match sender
                        .sync_update(&mutation.entity, &mutation.scope_id, &mutation.id, data)
                        .await
                    {
                        Ok(()) => FlushOutcome::Drop,
                        Err(e) if e.is_transient() => FlushOutcome::Keep,
                        Err(_) => FlushOutcome::Drop,
                    }
                } else {
                    FlushOutcome::Drop
                }
            }
            Err(err) if err.is_permanent_mutation() => {
                tracing::error!(
                    entity = %mutation.entity,
                    id = %mutation.id,
                    op = ?mutation.op,
                    error = %err,
                    "dropping mutation after permanent error"
                );
                FlushOutcome::Drop
            }
            Err(err) => {
                tracing::error!(
                    entity = %mutation.entity,
                    id = %mutation.id,
                    op = ?mutation.op,
                    error = %err,
                    "dropping mutation after unknown error"
                );
                FlushOutcome::Drop
            }
        };

        match outcome {
            FlushOutcome::Drop => {
                remove_records(mutation.record_ids).boxed().await;
            }
            FlushOutcome::Keep => {
                retained += 1;
            }
        }
    }
    retained
}

enum FlushOutcome {
    Drop,
    Keep,
}

pub struct PersistentOfflineQueue {
    persistence: Arc<PersistenceLayer>,
    root_entity: String,
    authenticated_user: Mutex<Option<String>>,
    flushing: Mutex<bool>,
}

impl PersistentOfflineQueue {
    pub async fn new(persistence: Arc<PersistenceLayer>, root_entity: String) -> Result<Self> {
        let pending_def = pending_sync_definition();
        persistence
            .register_schema(PENDING_ENTITY, &pending_def)
            .await?;
        Ok(Self {
            persistence,
            root_entity,
            authenticated_user: Mutex::new(None),
            flushing: Mutex::new(false),
        })
    }

    fn current_user(&self) -> Option<String> {
        self.authenticated_user.lock().ok().and_then(|g| g.clone())
    }

    async fn list_rows(&self, filters: Vec<Filter>) -> Result<Vec<StoredRow>> {
        let records = self
            .persistence
            .list(PENDING_ENTITY, filters, Vec::new(), None)
            .await?;
        Ok(records.into_iter().filter_map(row_from_record).collect())
    }
}

#[async_trait]
impl OfflineQueue for PersistentOfflineQueue {
    async fn queue(&self, mutation: PendingMutation) -> Result<()> {
        let Some(user) = self.current_user() else {
            tracing::warn!(
                entity = %mutation.entity,
                id = %mutation.id,
                op = ?mutation.op,
                "offline queue dropped mutation: no authenticated user set"
            );
            return Ok(());
        };
        let row_id = uuid::Uuid::now_v7().to_string();
        let created_at = if mutation.created_at == 0 {
            now_millis()
        } else {
            mutation.created_at
        };
        let mut record = Record::new();
        record.insert("id".into(), Value::String(row_id));
        record.insert("op".into(), Value::String(op_label(mutation.op).into()));
        record.insert("entity".into(), Value::String(mutation.entity));
        record.insert("entityId".into(), Value::String(mutation.id));
        record.insert("scopeId".into(), Value::String(mutation.scope_id));
        record.insert("userId".into(), Value::String(user));
        record.insert("createdAt".into(), Value::Number(created_at.into()));
        if let Some(data) = mutation.data {
            record.insert("data".into(), Value::Object(data));
        }
        self.persistence
            .create(PENDING_ENTITY, record, Origin::Local)
            .await?;
        Ok(())
    }

    async fn remove(
        &self,
        entity: &str,
        entity_id: &str,
        scope_id: &str,
        op: Operation,
    ) -> Result<()> {
        let mut filters = vec![
            Filter::new("entity".into(), FilterOp::Eq, Value::String(entity.into())),
            Filter::new(
                "entityId".into(),
                FilterOp::Eq,
                Value::String(entity_id.into()),
            ),
            Filter::new(
                "scopeId".into(),
                FilterOp::Eq,
                Value::String(scope_id.into()),
            ),
            Filter::new(
                "op".into(),
                FilterOp::Eq,
                Value::String(op_label(op).into()),
            ),
        ];
        if let Some(user) = self.current_user() {
            filters.push(Filter::new(
                "userId".into(),
                FilterOp::Eq,
                Value::String(user),
            ));
        }
        let rows = self.list_rows(filters).await?;
        for row in rows {
            let _ = self
                .persistence
                .delete(PENDING_ENTITY, &row.record_id, Origin::Local)
                .await;
        }
        Ok(())
    }

    async fn flush(&self, sender: &dyn MutationSender) -> Result<usize> {
        let _guard = match FlushGuard::try_acquire(&self.flushing) {
            Some(g) => g,
            None => return Ok(0),
        };
        self.do_flush(sender).await
    }

    async fn clear(&self) -> Result<()> {
        let Some(user) = self.current_user() else {
            return Ok(());
        };
        let filters = vec![Filter::new(
            "userId".into(),
            FilterOp::Eq,
            Value::String(user),
        )];
        let rows = self.list_rows(filters).await?;
        for row in rows {
            let _ = self
                .persistence
                .delete(PENDING_ENTITY, &row.record_id, Origin::Local)
                .await;
        }
        Ok(())
    }

    async fn pending_for_scope(&self, scope_id: &str) -> Result<Vec<PendingMutation>> {
        let mut filters = vec![Filter::new(
            "scopeId".into(),
            FilterOp::Eq,
            Value::String(scope_id.into()),
        )];
        if let Some(user) = self.current_user() {
            filters.push(Filter::new(
                "userId".into(),
                FilterOp::Eq,
                Value::String(user),
            ));
        }
        let rows = self.list_rows(filters).await?;
        Ok(rows.into_iter().map(pending_from_row).collect())
    }

    async fn has_pending_insert(&self, entity: &str, entity_id: &str) -> Result<bool> {
        let filters = vec![
            Filter::new("entity".into(), FilterOp::Eq, Value::String(entity.into())),
            Filter::new(
                "entityId".into(),
                FilterOp::Eq,
                Value::String(entity_id.into()),
            ),
            Filter::new("op".into(), FilterOp::Eq, Value::String("insert".into())),
        ];
        let rows = self.list_rows(filters).await?;
        Ok(!rows.is_empty())
    }

    fn set_authenticated_user(&self, user_id: Option<String>) {
        *self.authenticated_user.lock().expect("user lock") = user_id;
    }
}

impl PersistentOfflineQueue {
    async fn do_flush(&self, sender: &dyn MutationSender) -> Result<usize> {
        let Some(user) = self.current_user() else {
            return Ok(0);
        };
        let filters = vec![Filter::new(
            "userId".into(),
            FilterOp::Eq,
            Value::String(user),
        )];
        let rows = self.list_rows(filters).await?;
        if rows.is_empty() {
            return Ok(0);
        }
        let consolidated = consolidate(rows);
        let persistence = Arc::clone(&self.persistence);
        let remove = move |ids: Vec<String>| {
            let persistence = Arc::clone(&persistence);
            use futures::FutureExt;
            async move {
                for id in ids {
                    let _ = persistence.delete(PENDING_ENTITY, &id, Origin::Local).await;
                }
            }
            .boxed()
        };
        let retained = flush_consolidated(consolidated, sender, &self.root_entity, remove).await;
        Ok(retained)
    }
}

pub struct InMemoryOfflineQueue {
    rows: Mutex<Vec<StoredRow>>,
    root_entity: String,
    flushing: Mutex<bool>,
}

impl InMemoryOfflineQueue {
    #[must_use]
    pub fn new(root_entity: String) -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
            root_entity,
            flushing: Mutex::new(false),
        }
    }
}

#[async_trait]
impl OfflineQueue for InMemoryOfflineQueue {
    async fn queue(&self, mutation: PendingMutation) -> Result<()> {
        let row = StoredRow {
            record_id: uuid::Uuid::now_v7().to_string(),
            op: mutation.op,
            entity: mutation.entity,
            entity_id: mutation.id,
            scope_id: mutation.scope_id,
            data: mutation.data,
            created_at: if mutation.created_at == 0 {
                now_millis()
            } else {
                mutation.created_at
            },
        };
        self.rows.lock().expect("rows lock").push(row);
        Ok(())
    }

    async fn remove(
        &self,
        entity: &str,
        entity_id: &str,
        scope_id: &str,
        op: Operation,
    ) -> Result<()> {
        let mut rows = self.rows.lock().expect("rows lock");
        rows.retain(|r| {
            !(r.entity == entity
                && r.entity_id == entity_id
                && r.scope_id == scope_id
                && r.op == op)
        });
        Ok(())
    }

    async fn flush(&self, sender: &dyn MutationSender) -> Result<usize> {
        let _guard = match FlushGuard::try_acquire(&self.flushing) {
            Some(g) => g,
            None => return Ok(0),
        };
        let snapshot: Vec<StoredRow> = self.rows.lock().expect("rows lock").clone();
        if snapshot.is_empty() {
            return Ok(0);
        }
        let consolidated = consolidate(snapshot);
        let rows_handle: &Mutex<Vec<StoredRow>> = &self.rows;
        let remove_set = Arc::new(Mutex::new(Vec::<String>::new()));
        let remove_set_clone = Arc::clone(&remove_set);
        let remove = move |ids: Vec<String>| {
            let set = Arc::clone(&remove_set_clone);
            use futures::FutureExt;
            async move {
                set.lock().expect("remove_set lock").extend(ids);
            }
            .boxed()
        };
        let retained = flush_consolidated(consolidated, sender, &self.root_entity, remove).await;
        let flushed: Vec<String> = remove_set.lock().expect("remove_set lock").clone();
        rows_handle
            .lock()
            .expect("rows lock")
            .retain(|r| !flushed.contains(&r.record_id));
        Ok(retained)
    }

    async fn clear(&self) -> Result<()> {
        self.rows.lock().expect("rows lock").clear();
        Ok(())
    }

    async fn pending_for_scope(&self, scope_id: &str) -> Result<Vec<PendingMutation>> {
        Ok(self
            .rows
            .lock()
            .expect("rows lock")
            .iter()
            .filter(|r| r.scope_id == scope_id)
            .cloned()
            .map(pending_from_row)
            .collect())
    }

    async fn has_pending_insert(&self, entity: &str, entity_id: &str) -> Result<bool> {
        Ok(self
            .rows
            .lock()
            .expect("rows lock")
            .iter()
            .any(|r| r.entity == entity && r.entity_id == entity_id && r.op == Operation::Insert))
    }

    fn set_authenticated_user(&self, _user_id: Option<String>) {}
}

fn pending_sync_definition() -> crate::config::EntityDefinition {
    use crate::config::{EntityDefinition, FieldType, SchemaField};
    EntityDefinition {
        fields: vec![
            SchemaField {
                name: "id".into(),
                r#type: FieldType::String,
                required: true,
                default: None,
            },
            SchemaField {
                name: "op".into(),
                r#type: FieldType::String,
                required: true,
                default: None,
            },
            SchemaField {
                name: "entity".into(),
                r#type: FieldType::String,
                required: true,
                default: None,
            },
            SchemaField {
                name: "entityId".into(),
                r#type: FieldType::String,
                required: true,
                default: None,
            },
            SchemaField {
                name: "scopeId".into(),
                r#type: FieldType::String,
                required: true,
                default: None,
            },
            SchemaField {
                name: "userId".into(),
                r#type: FieldType::String,
                required: true,
                default: None,
            },
            SchemaField {
                name: "data".into(),
                r#type: FieldType::Object,
                required: false,
                default: None,
            },
            SchemaField {
                name: "createdAt".into(),
                r#type: FieldType::Number,
                required: false,
                default: None,
            },
        ],
        indexes: vec!["scopeId".into(), "entity".into()],
        ..Default::default()
    }
}

fn row_from_record(record: Record) -> Option<StoredRow> {
    let record_id = record.get("id")?.as_str()?.to_string();
    let op = record
        .get("op")
        .and_then(Value::as_str)
        .and_then(parse_op)?;
    let entity = record.get("entity")?.as_str()?.to_string();
    let entity_id = record.get("entityId")?.as_str()?.to_string();
    let scope_id = record.get("scopeId")?.as_str()?.to_string();
    let created_at = record.get("createdAt").and_then(Value::as_u64).unwrap_or(0);
    let data = record.get("data").and_then(|v| match v {
        Value::Object(m) => Some(m.clone()),
        _ => None,
    });
    Some(StoredRow {
        record_id,
        op,
        entity,
        entity_id,
        scope_id,
        data,
        created_at,
    })
}

fn pending_from_row(row: StoredRow) -> PendingMutation {
    PendingMutation {
        op: row.op,
        entity: row.entity,
        id: row.entity_id,
        scope_id: row.scope_id,
        data: row.data,
        created_at: row.created_at,
    }
}

struct FlushGuard<'a> {
    flag: &'a Mutex<bool>,
}

impl<'a> FlushGuard<'a> {
    fn try_acquire(flag: &'a Mutex<bool>) -> Option<Self> {
        let mut guard = flag.lock().expect("flushing lock");
        if *guard {
            return None;
        }
        *guard = true;
        Some(Self { flag })
    }
}

impl Drop for FlushGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut g) = self.flag.lock() {
            *g = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct FakeSender {
        creates: Mutex<Vec<(String, Record)>>,
        updates: AtomicUsize,
        fail_update_not_found: bool,
        fail_create_transient: bool,
        read_result: Mutex<Option<Record>>,
    }

    #[async_trait]
    impl MutationSender for FakeSender {
        async fn sync_create(&self, entity: &str, _scope_id: &str, data: Record) -> Result<()> {
            if self.fail_create_transient {
                return Err(Error::Timeout(10));
            }
            self.creates
                .lock()
                .unwrap()
                .push((entity.to_string(), data));
            Ok(())
        }

        async fn sync_update(
            &self,
            entity: &str,
            _scope_id: &str,
            id: &str,
            _data: Record,
        ) -> Result<()> {
            self.updates.fetch_add(1, Ordering::SeqCst);
            if self.fail_update_not_found {
                return Err(Error::NotFound {
                    entity: entity.to_string(),
                    id: id.to_string(),
                });
            }
            Ok(())
        }

        async fn sync_delete(&self, _entity: &str, _scope_id: &str, _id: &str) -> Result<()> {
            Ok(())
        }

        async fn read_entity(&self, _entity: &str, _id: &str) -> Result<Option<Record>> {
            Ok(self.read_result.lock().unwrap().clone())
        }

        async fn delete_entity(&self, _entity: &str, _id: &str) -> Result<()> {
            Ok(())
        }
    }

    fn record(pairs: &[(&str, &str)]) -> Record {
        let mut r = Record::new();
        for (k, v) in pairs {
            r.insert((*k).into(), Value::String((*v).into()));
        }
        r
    }

    fn pending(op: Operation, id: &str, data: Option<Record>, created_at: u64) -> PendingMutation {
        PendingMutation {
            op,
            entity: "task".into(),
            id: id.into(),
            scope_id: "s1".into(),
            data,
            created_at,
        }
    }

    #[tokio::test]
    async fn flush_retains_transient_then_drains_on_recovery() {
        let queue = InMemoryOfflineQueue::new("workspace".into());
        queue
            .queue(pending(
                Operation::Insert,
                "t1",
                Some(record(&[("id", "t1"), ("status", "pending")])),
                1,
            ))
            .await
            .unwrap();

        let failing = FakeSender {
            fail_create_transient: true,
            ..Default::default()
        };
        assert_eq!(queue.flush(&failing).await.unwrap(), 1);

        let recovered = FakeSender::default();
        assert_eq!(queue.flush(&recovered).await.unwrap(), 0);
        assert_eq!(recovered.creates.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn flush_update_not_found_recreates_from_local_snapshot() {
        let queue = InMemoryOfflineQueue::new("workspace".into());
        queue
            .queue(pending(
                Operation::Update,
                "t1",
                Some(record(&[("status", "running")])),
                1,
            ))
            .await
            .unwrap();

        let sender = FakeSender {
            fail_update_not_found: true,
            read_result: Mutex::new(Some(record(&[("id", "t1"), ("status", "running")]))),
            ..Default::default()
        };
        assert_eq!(queue.flush(&sender).await.unwrap(), 0);
        assert_eq!(sender.updates.load(Ordering::SeqCst), 1);
        assert_eq!(sender.creates.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn flush_consolidates_insert_and_update_into_single_create() {
        let queue = InMemoryOfflineQueue::new("workspace".into());
        queue
            .queue(pending(
                Operation::Insert,
                "t1",
                Some(record(&[("id", "t1"), ("status", "pending")])),
                1,
            ))
            .await
            .unwrap();
        queue
            .queue(pending(
                Operation::Update,
                "t1",
                Some(record(&[("status", "running")])),
                2,
            ))
            .await
            .unwrap();

        let sender = FakeSender::default();
        assert_eq!(queue.flush(&sender).await.unwrap(), 0);
        assert_eq!(sender.updates.load(Ordering::SeqCst), 0);
        let creates = sender.creates.lock().unwrap();
        assert_eq!(creates.len(), 1);
        assert_eq!(
            creates[0].1.get("status").and_then(Value::as_str),
            Some("running")
        );
    }
}
