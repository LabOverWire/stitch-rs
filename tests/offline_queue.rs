use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use stitch::config::{EntityDefinition, FieldType, PersistenceConfig, SchemaField, ScopeConfig};
use stitch::offline_queue::{
    InMemoryOfflineQueue, MutationSender, OfflineQueue, PersistentOfflineQueue,
};
use stitch::persistence::PersistenceLayer;
use stitch::types::{Operation, PendingMutation};
use stitch::{Error, StoreConfig};
use tempfile::TempDir;

fn fixture_config() -> Arc<StoreConfig> {
    let mut entities = HashMap::new();
    entities.insert(
        "project".to_string(),
        EntityDefinition {
            fields: vec![SchemaField {
                name: "id".to_string(),
                r#type: FieldType::String,
                required: true,
                default: None,
            }],
            ..EntityDefinition::default()
        },
    );
    entities.insert(
        "task".to_string(),
        EntityDefinition {
            fields: vec![
                SchemaField {
                    name: "id".to_string(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
                SchemaField {
                    name: "title".to_string(),
                    r#type: FieldType::String,
                    required: false,
                    default: None,
                },
                SchemaField {
                    name: "projectId".to_string(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
            ],
            ..EntityDefinition::default()
        },
    );

    Arc::new(StoreConfig::new(
        entities,
        ScopeConfig {
            root_entity: "project".to_string(),
            child_entities: vec!["task".to_string()],
            scope_field: "projectId".to_string(),
        },
    ))
}

fn make_record(pairs: &[(&str, Value)]) -> Map<String, Value> {
    let mut map = Map::new();
    for (k, v) in pairs {
        map.insert((*k).to_string(), v.clone());
    }
    map
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Behavior {
    Ok,
    Transient,
    NotFound,
    Conflict,
    Ownership,
    Permanent,
    Unknown,
}

struct MockSender {
    behavior: Mutex<HashMap<String, Behavior>>,
    default: Mutex<Behavior>,
    log: Mutex<Vec<String>>,
    read_returns: Mutex<HashMap<(String, String), Map<String, Value>>>,
}

impl MockSender {
    fn new(default: Behavior) -> Arc<Self> {
        Arc::new(Self {
            behavior: Mutex::new(HashMap::new()),
            default: Mutex::new(default),
            log: Mutex::new(Vec::new()),
            read_returns: Mutex::new(HashMap::new()),
        })
    }

    fn set(&self, op_label: &str, behavior: Behavior) {
        self.behavior
            .lock()
            .unwrap()
            .insert(op_label.to_string(), behavior);
    }

    fn set_read(&self, entity: &str, id: &str, record: Map<String, Value>) {
        self.read_returns
            .lock()
            .unwrap()
            .insert((entity.into(), id.into()), record);
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    fn record(&self, label: &str) {
        self.log.lock().unwrap().push(label.to_string());
    }

    fn resolve(&self, op_label: &str) -> Behavior {
        self.behavior
            .lock()
            .unwrap()
            .get(op_label)
            .copied()
            .unwrap_or_else(|| *self.default.lock().unwrap())
    }
}

fn behavior_to_result(b: Behavior, entity: &str, id: &str) -> Result<(), Error> {
    match b {
        Behavior::Ok => Ok(()),
        Behavior::Transient => Err(Error::ConnectionClosed),
        Behavior::NotFound => Err(Error::NotFound {
            entity: entity.into(),
            id: id.into(),
        }),
        Behavior::Conflict => Err(Error::Conflict {
            entity: entity.into(),
            id: id.into(),
        }),
        Behavior::Ownership => Err(Error::Ownership {
            entity: entity.into(),
            id: id.into(),
        }),
        Behavior::Permanent => Err(Error::mqdb(
            "test",
            mqdb_core::error::Error::ConstraintViolation("unique violation".into()),
        )),
        Behavior::Unknown => Err(Error::Config("synthetic unknown error".into())),
    }
}

#[async_trait]
impl MutationSender for MockSender {
    async fn sync_create(
        &self,
        entity: &str,
        scope_id: &str,
        _data: Map<String, Value>,
    ) -> Result<(), Error> {
        self.record(&format!("create:{entity}:{scope_id}"));
        behavior_to_result(self.resolve("create"), entity, scope_id)
    }

    async fn sync_update(
        &self,
        entity: &str,
        scope_id: &str,
        id: &str,
        _data: Map<String, Value>,
    ) -> Result<(), Error> {
        self.record(&format!("update:{entity}:{scope_id}:{id}"));
        behavior_to_result(self.resolve("update"), entity, id)
    }

    async fn sync_delete(&self, entity: &str, scope_id: &str, id: &str) -> Result<(), Error> {
        self.record(&format!("delete:{entity}:{scope_id}:{id}"));
        behavior_to_result(self.resolve("delete"), entity, id)
    }

    async fn read_entity(
        &self,
        entity: &str,
        id: &str,
    ) -> Result<Option<Map<String, Value>>, Error> {
        self.record(&format!("read:{entity}:{id}"));
        Ok(self
            .read_returns
            .lock()
            .unwrap()
            .get(&(entity.to_string(), id.to_string()))
            .cloned())
    }

    async fn delete_entity(&self, entity: &str, id: &str) -> Result<(), Error> {
        self.record(&format!("hard_delete:{entity}:{id}"));
        Ok(())
    }
}

async fn open_persistent_queue() -> (TempDir, Arc<PersistenceLayer>, PersistentOfflineQueue) {
    let dir = TempDir::new().unwrap();
    let persistence = PersistenceConfig {
        db_path: dir.path().join("db"),
        passphrase: None,
    };
    let layer = Arc::new(
        PersistenceLayer::open(&persistence, fixture_config())
            .await
            .unwrap(),
    );
    let queue = PersistentOfflineQueue::new(Arc::clone(&layer), "project".to_string())
        .await
        .unwrap();
    queue.set_authenticated_user(Some("user-1".into()));
    (dir, layer, queue)
}

fn pending(
    op: Operation,
    entity: &str,
    id: &str,
    scope_id: &str,
    data: Map<String, Value>,
) -> PendingMutation {
    PendingMutation {
        op,
        entity: entity.into(),
        id: id.into(),
        scope_id: scope_id.into(),
        data: if data.is_empty() { None } else { Some(data) },
        created_at: 0,
    }
}

#[tokio::test]
async fn persistent_queue_drops_row_after_successful_flush() {
    let (_dir, _persistence, queue) = open_persistent_queue().await;
    queue
        .queue(pending(
            Operation::Insert,
            "task",
            "t1",
            "p1",
            make_record(&[("id", json!("t1")), ("title", json!("a"))]),
        ))
        .await
        .unwrap();

    let sender = MockSender::new(Behavior::Ok);
    queue.flush(sender.as_ref()).await.unwrap();
    assert_eq!(sender.log(), vec!["create:task:p1"]);
    let remaining = queue.pending_for_scope("p1").await.unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn persistent_queue_keeps_row_on_transient_error() {
    let (_dir, _persistence, queue) = open_persistent_queue().await;
    queue
        .queue(pending(
            Operation::Insert,
            "task",
            "t1",
            "p1",
            make_record(&[("id", json!("t1"))]),
        ))
        .await
        .unwrap();

    let sender = MockSender::new(Behavior::Transient);
    queue.flush(sender.as_ref()).await.unwrap();
    let remaining = queue.pending_for_scope("p1").await.unwrap();
    assert_eq!(remaining.len(), 1);
}

#[tokio::test]
async fn ownership_error_drops_row_silently() {
    let (_dir, _persistence, queue) = open_persistent_queue().await;
    queue
        .queue(pending(
            Operation::Update,
            "task",
            "t1",
            "p1",
            make_record(&[("title", json!("x"))]),
        ))
        .await
        .unwrap();

    let sender = MockSender::new(Behavior::Ownership);
    queue.flush(sender.as_ref()).await.unwrap();
    let remaining = queue.pending_for_scope("p1").await.unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn consolidates_insert_plus_updates_into_single_insert() {
    let queue = InMemoryOfflineQueue::new("project".to_string());
    queue
        .queue(pending(
            Operation::Insert,
            "task",
            "t1",
            "p1",
            make_record(&[("id", json!("t1")), ("title", json!("a"))]),
        ))
        .await
        .unwrap();
    queue
        .queue(pending(
            Operation::Update,
            "task",
            "t1",
            "p1",
            make_record(&[("title", json!("b"))]),
        ))
        .await
        .unwrap();
    queue
        .queue(pending(
            Operation::Update,
            "task",
            "t1",
            "p1",
            make_record(&[("title", json!("c"))]),
        ))
        .await
        .unwrap();

    let sender = MockSender::new(Behavior::Ok);
    queue.flush(sender.as_ref()).await.unwrap();
    let log = sender.log();
    assert_eq!(log.len(), 1, "expected single create call, got {log:?}");
    assert_eq!(log[0], "create:task:p1");
}

#[tokio::test]
async fn consolidates_insert_plus_delete_to_no_op() {
    let queue = InMemoryOfflineQueue::new("project".to_string());
    queue
        .queue(pending(
            Operation::Insert,
            "task",
            "t1",
            "p1",
            make_record(&[("id", json!("t1"))]),
        ))
        .await
        .unwrap();
    queue
        .queue(pending(Operation::Delete, "task", "t1", "p1", Map::new()))
        .await
        .unwrap();

    let sender = MockSender::new(Behavior::NotFound);
    queue.flush(sender.as_ref()).await.unwrap();
    let log = sender.log();
    assert_eq!(log.len(), 1, "expected single delete call, got {log:?}");
    assert!(log[0].starts_with("delete:"));
    assert!(queue.pending_for_scope("p1").await.unwrap().is_empty());
}

#[tokio::test]
async fn update_on_child_with_not_found_triggers_upsert() {
    let queue = InMemoryOfflineQueue::new("project".to_string());
    queue
        .queue(pending(
            Operation::Update,
            "task",
            "t1",
            "p1",
            make_record(&[("title", json!("x"))]),
        ))
        .await
        .unwrap();

    let sender = MockSender::new(Behavior::Ok);
    sender.set("update", Behavior::NotFound);
    sender.set_read(
        "task",
        "t1",
        make_record(&[
            ("id", json!("t1")),
            ("title", json!("x")),
            ("projectId", json!("p1")),
        ]),
    );

    queue.flush(sender.as_ref()).await.unwrap();
    let log = sender.log();
    let actions: HashSet<&str> = log.iter().map(String::as_str).collect();
    assert!(actions.contains("update:task:p1:t1"), "log: {log:?}");
    assert!(actions.contains("read:task:t1"), "log: {log:?}");
    assert!(actions.contains("create:task:p1"), "log: {log:?}");
}

#[tokio::test]
async fn update_on_root_with_not_found_triggers_hard_delete() {
    let queue = InMemoryOfflineQueue::new("project".to_string());
    queue
        .queue(pending(
            Operation::Update,
            "project",
            "p1",
            "p1",
            make_record(&[("name", json!("x"))]),
        ))
        .await
        .unwrap();

    let sender = MockSender::new(Behavior::NotFound);
    queue.flush(sender.as_ref()).await.unwrap();
    let log = sender.log();
    assert!(
        log.iter().any(|e| e == "hard_delete:project:p1"),
        "log: {log:?}"
    );
}

#[tokio::test]
async fn conflict_on_insert_switches_to_update() {
    let queue = InMemoryOfflineQueue::new("project".to_string());
    queue
        .queue(pending(
            Operation::Insert,
            "task",
            "t1",
            "p1",
            make_record(&[("id", json!("t1")), ("title", json!("x"))]),
        ))
        .await
        .unwrap();

    let sender = MockSender::new(Behavior::Ok);
    sender.set("create", Behavior::Conflict);
    queue.flush(sender.as_ref()).await.unwrap();
    let log = sender.log();
    assert!(log.iter().any(|e| e.starts_with("create:")), "log: {log:?}");
    assert!(log.iter().any(|e| e == "update:task:p1:t1"), "log: {log:?}");
}

#[tokio::test]
async fn delete_not_found_drops_silently() {
    let queue = InMemoryOfflineQueue::new("project".to_string());
    queue
        .queue(pending(Operation::Delete, "task", "t1", "p1", Map::new()))
        .await
        .unwrap();

    let sender = MockSender::new(Behavior::NotFound);
    queue.flush(sender.as_ref()).await.unwrap();
    assert!(queue.pending_for_scope("p1").await.unwrap().is_empty());
}

#[tokio::test]
async fn has_pending_insert_returns_true_for_queued_insert() {
    let queue = InMemoryOfflineQueue::new("project".to_string());
    queue
        .queue(pending(
            Operation::Insert,
            "task",
            "t1",
            "p1",
            make_record(&[("id", json!("t1"))]),
        ))
        .await
        .unwrap();
    assert!(queue.has_pending_insert("task", "t1").await.unwrap());
    assert!(!queue.has_pending_insert("task", "other").await.unwrap());
}

#[tokio::test]
async fn clear_removes_all_rows() {
    let (_dir, _persistence, queue) = open_persistent_queue().await;
    queue
        .queue(pending(
            Operation::Insert,
            "task",
            "t1",
            "p1",
            make_record(&[("id", json!("t1"))]),
        ))
        .await
        .unwrap();
    queue.clear().await.unwrap();
    assert!(queue.pending_for_scope("p1").await.unwrap().is_empty());
}

#[tokio::test]
async fn persistent_queue_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let persistence_cfg = PersistenceConfig {
        db_path: dir.path().join("db"),
        passphrase: None,
    };
    {
        let layer = Arc::new(
            PersistenceLayer::open(&persistence_cfg, fixture_config())
                .await
                .unwrap(),
        );
        let queue = PersistentOfflineQueue::new(layer.clone(), "project".to_string())
            .await
            .unwrap();
        queue.set_authenticated_user(Some("user-1".into()));
        queue
            .queue(pending(
                Operation::Insert,
                "task",
                "t1",
                "p1",
                make_record(&[("id", json!("t1"))]),
            ))
            .await
            .unwrap();
        layer.close();
    }

    let layer2 = Arc::new(
        PersistenceLayer::open(&persistence_cfg, fixture_config())
            .await
            .unwrap(),
    );
    let queue2 = PersistentOfflineQueue::new(layer2, "project".to_string())
        .await
        .unwrap();
    queue2.set_authenticated_user(Some("user-1".into()));
    let pending = queue2.pending_for_scope("p1").await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].entity, "task");
}

#[tokio::test]
async fn permanent_mutation_error_drops_row() {
    let queue = InMemoryOfflineQueue::new("project".to_string());
    queue
        .queue(pending(
            Operation::Insert,
            "task",
            "t-permanent",
            "p1",
            make_record(&[("id", json!("t-permanent"))]),
        ))
        .await
        .unwrap();

    let sender = MockSender::new(Behavior::Permanent);
    queue.flush(sender.as_ref()).await.unwrap();
    let remaining = queue.pending_for_scope("p1").await.unwrap();
    assert!(
        remaining.is_empty(),
        "permanent constraint violation should drop the row"
    );
}

#[tokio::test]
async fn unknown_error_drops_row() {
    let queue = InMemoryOfflineQueue::new("project".to_string());
    queue
        .queue(pending(
            Operation::Insert,
            "task",
            "t-unknown",
            "p1",
            make_record(&[("id", json!("t-unknown"))]),
        ))
        .await
        .unwrap();

    let sender = MockSender::new(Behavior::Unknown);
    queue.flush(sender.as_ref()).await.unwrap();
    let remaining = queue.pending_for_scope("p1").await.unwrap();
    assert!(remaining.is_empty(), "unknown error should drop the row");
}

#[tokio::test]
async fn user_scoped_isolation() {
    let (_dir, persistence, queue_a) = open_persistent_queue().await;
    queue_a
        .queue(pending(
            Operation::Insert,
            "task",
            "t-a",
            "p1",
            make_record(&[("id", json!("t-a"))]),
        ))
        .await
        .unwrap();

    let queue_b = PersistentOfflineQueue::new(Arc::clone(&persistence), "project".to_string())
        .await
        .unwrap();
    queue_b.set_authenticated_user(Some("user-2".into()));
    queue_b
        .queue(pending(
            Operation::Insert,
            "task",
            "t-b",
            "p1",
            make_record(&[("id", json!("t-b"))]),
        ))
        .await
        .unwrap();

    assert_eq!(queue_a.pending_for_scope("p1").await.unwrap().len(), 1);
    assert_eq!(queue_b.pending_for_scope("p1").await.unwrap().len(), 1);

    queue_a.clear().await.unwrap();
    assert!(queue_a.pending_for_scope("p1").await.unwrap().is_empty());
    assert_eq!(queue_b.pending_for_scope("p1").await.unwrap().len(), 1);
}
