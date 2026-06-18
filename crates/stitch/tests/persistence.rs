use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use stitch::config::{EntityDefinition, FieldType, PersistenceConfig, SchemaField, ScopeConfig};
use stitch::persistence::PersistenceLayer;
use stitch::types::{Operation, StoreEvent};
use stitch::{Origin, StoreConfig};
use tempfile::TempDir;

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
            indexes: vec!["projectId".to_string()],
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

async fn open_layer(dir: &TempDir) -> PersistenceLayer {
    let persistence = PersistenceConfig {
        db_path: dir.path().join("db"),
        passphrase: None,
    };
    PersistenceLayer::open(&persistence, fixture_config())
        .await
        .unwrap()
}

#[tokio::test]
async fn create_then_read_returns_record() {
    let dir = TempDir::new().unwrap();
    let layer = open_layer(&dir).await;
    layer
        .create(
            "project",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .unwrap();

    let got = layer.read("project", "p1").await.unwrap().unwrap();
    assert_eq!(got.get("name").and_then(Value::as_str), Some("Alpha"));
}

#[tokio::test]
async fn data_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let layer = open_layer(&dir).await;
        layer
            .create(
                "project",
                make_record(&[("id", json!("p1")), ("name", json!("first"))]),
                Origin::Local,
            )
            .await
            .unwrap();
        layer.close();
    }

    let layer2 = open_layer(&dir).await;
    let got = layer2.read("project", "p1").await.unwrap().unwrap();
    assert_eq!(got.get("name").and_then(Value::as_str), Some("first"));
}

#[tokio::test]
async fn create_emits_mutation_event_with_origin() {
    let dir = TempDir::new().unwrap();
    let layer = open_layer(&dir).await;
    let mut rx = layer.subscribe();

    layer
        .create(
            "task",
            make_record(&[
                ("id", json!("t1")),
                ("title", json!("hello")),
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
async fn suppressed_creates_emit_no_events() {
    let dir = TempDir::new().unwrap();
    let layer = open_layer(&dir).await;
    let mut rx = layer.subscribe();

    layer.set_suppress_notifications(true);
    layer
        .create(
            "task",
            make_record(&[
                ("id", json!("t1")),
                ("title", json!("hello")),
                ("projectId", json!("p1")),
            ]),
            Origin::Load,
        )
        .await
        .unwrap();
    layer.set_suppress_notifications(false);

    let recv = tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await;
    assert!(recv.is_err(), "expected no event during suppression");
}

#[tokio::test]
async fn list_by_scope_filters_children() {
    let dir = TempDir::new().unwrap();
    let layer = open_layer(&dir).await;
    layer
        .create(
            "task",
            make_record(&[("id", json!("t1")), ("projectId", json!("p1"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    layer
        .create(
            "task",
            make_record(&[("id", json!("t2")), ("projectId", json!("p2"))]),
            Origin::Local,
        )
        .await
        .unwrap();

    let p1 = layer.list_by_scope("task", "p1").await.unwrap();
    assert_eq!(p1.len(), 1);
    assert_eq!(p1[0].get("id").and_then(Value::as_str), Some("t1"));
}

#[tokio::test]
async fn list_root_returns_all_roots() {
    let dir = TempDir::new().unwrap();
    let layer = open_layer(&dir).await;
    layer
        .create(
            "project",
            make_record(&[("id", json!("p1"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    layer
        .create(
            "project",
            make_record(&[("id", json!("p2"))]),
            Origin::Local,
        )
        .await
        .unwrap();

    let all = layer.list_root().await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn update_then_delete_emits_correct_events() {
    let dir = TempDir::new().unwrap();
    let layer = open_layer(&dir).await;
    layer
        .create(
            "task",
            make_record(&[
                ("id", json!("t1")),
                ("title", json!("first")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();

    let updated = layer
        .update(
            "task",
            "t1",
            make_record(&[("title", json!("second"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    assert_eq!(updated.get("title").and_then(Value::as_str), Some("second"));

    layer.delete("task", "t1", Origin::Local).await.unwrap();
    assert!(layer.read("task", "t1").await.unwrap().is_none());
}

#[tokio::test]
async fn recover_reopens_db_and_preserves_data() {
    let dir = TempDir::new().unwrap();
    let layer = open_layer(&dir).await;

    layer
        .create(
            "project",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .unwrap();

    layer.recover().await.unwrap();

    let got = layer.read("project", "p1").await.unwrap().unwrap();
    assert_eq!(got.get("name").and_then(Value::as_str), Some("Alpha"));

    layer
        .create(
            "project",
            make_record(&[("id", json!("p2")), ("name", json!("Beta"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    let beta = layer.read("project", "p2").await.unwrap().unwrap();
    assert_eq!(beta.get("name").and_then(Value::as_str), Some("Beta"));
}

#[tokio::test]
async fn delete_unknown_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let layer = open_layer(&dir).await;
    let err = layer
        .delete("task", "nope", Origin::Local)
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "expected NotFound, got {err:?}");
}

#[tokio::test]
async fn create_accepts_null_optional_field_and_lists_record() {
    // Regression for chorale M-FS1 V1 (2026-06-17): a record with
    // `null` set on an optional Number field used to fail the mqdb
    // schema validator inside persistence.create with
    // `expected type Number, got null`. `Store::create` discarded
    // that error (memory still had the record), so subsequent
    // `Store::list` returned empty until restart while `Store::read`
    // succeeded. Fix: mirror `memory_store::strip_nulls` in
    // `persistence::create`.
    use std::collections::HashMap;
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
        "note".to_string(),
        EntityDefinition {
            fields: vec![
                SchemaField {
                    name: "id".to_string(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
                SchemaField {
                    name: "projectId".to_string(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
                SchemaField {
                    name: "body".to_string(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
                SchemaField {
                    name: "read_at".to_string(),
                    r#type: FieldType::Number,
                    required: false,
                    default: None,
                },
            ],
            ..EntityDefinition::default()
        },
    );
    let cfg = Arc::new(StoreConfig::new(
        entities,
        ScopeConfig {
            root_entity: "project".to_string(),
            child_entities: vec!["note".to_string()],
            scope_field: "projectId".to_string(),
        },
    ));
    let dir = TempDir::new().unwrap();
    let persistence = PersistenceConfig {
        db_path: dir.path().join("db"),
        passphrase: None,
    };
    let layer = PersistenceLayer::open(&persistence, cfg).await.unwrap();

    layer
        .create(
            "project",
            make_record(&[("id", json!("p1"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    layer
        .create(
            "note",
            make_record(&[
                ("id", json!("n1")),
                ("projectId", json!("p1")),
                ("body", json!("hello")),
                ("read_at", Value::Null),
            ]),
            Origin::Local,
        )
        .await
        .expect("create with null optional field must succeed");

    let got = layer.read("note", "n1").await.unwrap();
    assert!(got.is_some(), "read after create must return the record");

    let listed = layer
        .list_by_scope("note", "p1")
        .await
        .expect("list_by_scope must succeed");
    assert_eq!(
        listed.len(),
        1,
        "list_by_scope must return the created record (regression: was 0 before strip_nulls)",
    );
    assert_eq!(
        listed[0].get("id").and_then(Value::as_str),
        Some("n1"),
    );
}
