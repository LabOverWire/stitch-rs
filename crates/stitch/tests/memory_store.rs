use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use stitch::config::{EntityDefinition, FieldType, SchemaField, ScopeConfig};
use stitch::memory_store::MemoryStore;
use stitch::types::{Operation, StoreEvent};
use stitch::{Origin, ScopeBundle, StoreConfig};

fn fixture_config() -> Arc<StoreConfig> {
    let mut entities = HashMap::new();
    entities.insert(
        "project".to_string(),
        EntityDefinition {
            fields: vec![
                SchemaField {
                    name: "id".to_string(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
                SchemaField {
                    name: "name".to_string(),
                    r#type: FieldType::String,
                    required: false,
                    default: None,
                },
            ],
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

#[tokio::test]
async fn create_then_read_returns_record() {
    let store = MemoryStore::new(fixture_config()).await.unwrap();
    store
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .unwrap();

    let got = store.read("project", "p1").await.unwrap().unwrap();
    assert_eq!(got.get("name").and_then(Value::as_str), Some("Alpha"));
}

#[tokio::test]
async fn list_filters_children_by_scope() {
    let store = MemoryStore::new(fixture_config()).await.unwrap();
    for project_id in ["p1", "p2"] {
        store
            .create(
                "project",
                project_id,
                make_record(&[("id", json!(project_id))]),
                Origin::Local,
            )
            .await
            .unwrap();
    }
    store
        .create(
            "task",
            "p1",
            make_record(&[("id", json!("t1")), ("title", json!("a"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    store
        .create(
            "task",
            "p2",
            make_record(&[("id", json!("t2")), ("title", json!("b"))]),
            Origin::Local,
        )
        .await
        .unwrap();

    let tasks_p1 = store.list("task", "p1").await.unwrap();
    assert_eq!(tasks_p1.len(), 1);
    assert_eq!(tasks_p1[0].get("id").and_then(Value::as_str), Some("t1"));
}

#[tokio::test]
async fn create_emits_mutation_event_with_origin() {
    let store = MemoryStore::new(fixture_config()).await.unwrap();
    let mut rx = store.subscribe();

    store
        .create(
            "task",
            "p1",
            make_record(&[
                ("id", json!("t1")),
                ("title", json!("a")),
                ("projectId", json!("p1")),
            ]),
            Origin::Remote,
        )
        .await
        .unwrap();

    let StoreEvent::Mutation(event) = rx.recv().await.unwrap() else {
        panic!("expected Mutation event");
    };
    assert_eq!(event.operation, Operation::Insert);
    assert_eq!(event.entity, "task");
    assert_eq!(event.id, "t1");
    assert_eq!(event.scope_id, "p1");
    assert_eq!(event.origin, Origin::Remote);
}

#[tokio::test]
async fn update_returns_merged_record() {
    let store = MemoryStore::new(fixture_config()).await.unwrap();
    store
        .create(
            "task",
            "p1",
            make_record(&[
                ("id", json!("t1")),
                ("title", json!("first")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();

    let updated = store
        .update(
            "task",
            "t1",
            make_record(&[("title", json!("second"))]),
            Origin::Local,
        )
        .await
        .unwrap();

    assert_eq!(updated.get("title").and_then(Value::as_str), Some("second"));
    assert_eq!(updated.get("projectId").and_then(Value::as_str), Some("p1"));
}

#[tokio::test]
async fn delete_removes_record() {
    let store = MemoryStore::new(fixture_config()).await.unwrap();
    store
        .create(
            "task",
            "p1",
            make_record(&[("id", json!("t1")), ("projectId", json!("p1"))]),
            Origin::Local,
        )
        .await
        .unwrap();

    store.delete("task", "t1", Origin::Local).await.unwrap();
    assert!(store.read("task", "t1").await.unwrap().is_none());
}

#[tokio::test]
async fn read_unknown_returns_none() {
    let store = MemoryStore::new(fixture_config()).await.unwrap();
    assert!(store.read("task", "nope").await.unwrap().is_none());
}

#[tokio::test]
async fn delete_unknown_returns_not_found_error() {
    let store = MemoryStore::new(fixture_config()).await.unwrap();
    let err = store
        .delete("task", "nope", Origin::Local)
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "expected NotFound error, got {err:?}");
}

#[tokio::test]
async fn load_scope_replaces_contents_and_emits_scope_loaded() {
    let store = MemoryStore::new(fixture_config()).await.unwrap();
    let mut rx = store.subscribe();

    store
        .create(
            "task",
            "old",
            make_record(&[("id", json!("t-old")), ("projectId", json!("old"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    let _ = rx.recv().await;

    let mut children = BTreeMap::new();
    children.insert(
        "task".to_string(),
        vec![make_record(&[
            ("id", json!("t-new")),
            ("title", json!("hello")),
            ("projectId", json!("p1")),
        ])],
    );
    let bundle = ScopeBundle {
        root: Some(make_record(&[
            ("id", json!("p1")),
            ("name", json!("Alpha")),
        ])),
        children,
    };

    store.load_scope("p1", bundle).await.unwrap();

    assert!(store.read("task", "t-old").await.unwrap().is_none());
    let tasks = store.list("task", "p1").await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].get("id").and_then(Value::as_str), Some("t-new"));

    let loaded = loop {
        match rx.recv().await.unwrap() {
            StoreEvent::ScopeLoaded { scope_id, entities } => break (scope_id, entities),
            _ => continue,
        }
    };
    assert_eq!(loaded.0, "p1");
    assert!(loaded.1.contains(&"project".to_string()));
    assert!(loaded.1.contains(&"task".to_string()));
}

#[tokio::test]
async fn clear_scope_emits_scope_cleared() {
    let store = MemoryStore::new(fixture_config()).await.unwrap();
    let mut rx = store.subscribe();

    store
        .load_scope("p1", ScopeBundle::default())
        .await
        .unwrap();
    let _ = rx.recv().await;

    store.clear_scope("p1").await.unwrap();
    let cleared = loop {
        match rx.recv().await.unwrap() {
            StoreEvent::ScopeCleared { scope_id, entities } => break (scope_id, entities),
            _ => continue,
        }
    };
    assert_eq!(cleared.0, "p1");
    assert!(cleared.1.contains(&"task".to_string()));
    assert!(store.current_scope().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_updates_to_same_record_converge_without_conflict() {
    let store = Arc::new(MemoryStore::new(fixture_config()).await.unwrap());
    store
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    store
        .create(
            "task",
            "p1",
            make_record(&[
                ("id", json!("t1")),
                ("title", json!("init")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..16 {
        let store = Arc::clone(&store);
        handles.push(tokio::spawn(async move {
            store
                .update(
                    "task",
                    "t1",
                    make_record(&[("title", json!(format!("title-{i}")))]),
                    Origin::Local,
                )
                .await
        }));
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "concurrent same-key update surfaced a conflict: {result:?}"
        );
    }

    let final_task = store.read("task", "t1").await.unwrap().unwrap();
    let title = final_task.get("title").and_then(Value::as_str).unwrap();
    assert!(
        title.starts_with("title-"),
        "unexpected final title: {title}"
    );
    assert_eq!(
        final_task.get("projectId").and_then(Value::as_str),
        Some("p1"),
        "field-level merge dropped the untouched projectId field"
    );
}
