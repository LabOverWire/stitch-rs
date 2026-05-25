use serde_json::{Map, Value, json};
use std::collections::HashMap;
use stitch::config::{EntityDefinition, FieldType, PersistenceConfig, SchemaField, ScopeConfig};
use stitch::types::{Operation, StoreEvent};
use stitch::{Origin, Store, StoreConfig, StoreOptions};
use tempfile::TempDir;

fn fixture_config() -> StoreConfig {
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

    StoreConfig::new(
        entities,
        ScopeConfig {
            root_entity: "project".to_string(),
            child_entities: vec!["task".to_string()],
            scope_field: "projectId".to_string(),
        },
    )
}

fn make_record(pairs: &[(&str, Value)]) -> Map<String, Value> {
    let mut map = Map::new();
    for (k, v) in pairs {
        map.insert((*k).to_string(), v.clone());
    }
    map
}

#[tokio::test]
async fn read_before_initialize_returns_not_initialized() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    let err = store.read("project", "p1").await.unwrap_err();
    assert!(matches!(err, stitch::Error::NotInitialized));
}

#[tokio::test]
async fn memory_only_round_trip() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();

    let id = store
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    assert_eq!(id, "p1");

    let got = store.read("project", "p1").await.unwrap().unwrap();
    assert_eq!(got.get("name").and_then(Value::as_str), Some("Alpha"));
}

#[tokio::test]
async fn create_generates_id_when_absent() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
    let id = store
        .create(
            "project",
            "",
            make_record(&[("name", json!("Auto"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    let project = store.read("project", &id).await.unwrap().unwrap();
    assert_eq!(project.get("name").and_then(Value::as_str), Some("Auto"));
}

#[tokio::test]
async fn update_and_delete_in_memory_only_mode() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
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

    store
        .update(
            "task",
            "t1",
            make_record(&[("title", json!("second"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    let updated = store.read("task", "t1").await.unwrap().unwrap();
    assert_eq!(updated.get("title").and_then(Value::as_str), Some("second"));

    store.delete("task", "t1", Origin::Local).await.unwrap();
    assert!(store.read("task", "t1").await.unwrap().is_none());
}

#[tokio::test]
async fn persistence_outlives_store_instance() {
    let dir = TempDir::new().unwrap();
    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: dir.path().join("db"),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };
    {
        let store = Store::new(fixture_config(), options.clone());
        store.initialize().await.unwrap();
        store
            .create(
                "project",
                "p1",
                make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
                Origin::Local,
            )
            .await
            .unwrap();
        store.shutdown().await.unwrap();
    }

    let store2 = Store::new(fixture_config(), options);
    store2.initialize().await.unwrap();
    let project = store2.read("project", "p1").await.unwrap();
    assert!(
        project.is_none(),
        "memory is empty after restart; need replace_scope to repopulate"
    );

    let list = store2.list("project", None).await.unwrap();
    assert_eq!(list.len(), 1, "persistence reload should expose root");
    assert_eq!(list[0].get("id").and_then(Value::as_str), Some("p1"));
}

#[tokio::test]
async fn replace_scope_loads_from_persistence_when_offline() {
    let dir = TempDir::new().unwrap();
    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: dir.path().join("db"),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };

    let store = Store::new(fixture_config(), options);
    store.initialize().await.unwrap();
    store
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
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
                ("title", json!("hello")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();

    store.replace_scope("p1").await.unwrap();
    let in_memory = store.read("task", "t1").await.unwrap().unwrap();
    assert_eq!(
        in_memory.get("title").and_then(Value::as_str),
        Some("hello")
    );
    assert_eq!(store.current_scope().unwrap(), Some("p1".to_string()));
}

#[tokio::test]
async fn subscribe_receives_mutation_events() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
    let mut rx = store.subscribe().unwrap();

    store
        .create(
            "task",
            "p1",
            make_record(&[
                ("id", json!("t1")),
                ("title", json!("a")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();

    let StoreEvent::Mutation(event) = rx.recv().await.unwrap() else {
        panic!("expected Mutation event");
    };
    assert_eq!(event.operation, Operation::Insert);
    assert_eq!(event.entity, "task");
    assert_eq!(event.id, "t1");
}

#[tokio::test]
async fn close_scope_clears_current_scope() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
    store.replace_scope("p1").await.unwrap();
    assert_eq!(store.current_scope().unwrap(), Some("p1".to_string()));
    store.close_scope("p1").await.unwrap();
    assert_eq!(store.current_scope().unwrap(), None);
}

#[tokio::test]
async fn origin_load_skips_persistence_and_remote() {
    let dir = TempDir::new().unwrap();
    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: dir.path().join("db"),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };
    let store = Store::new(fixture_config(), options);
    store.initialize().await.unwrap();

    store
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1")), ("name", json!("Loaded"))]),
            Origin::Load,
        )
        .await
        .unwrap();

    let in_memory = store.read("project", "p1").await.unwrap().unwrap();
    assert_eq!(
        in_memory.get("name").and_then(Value::as_str),
        Some("Loaded")
    );

    let listed_from_persistence = store.list("project", None).await.unwrap();
    assert!(
        listed_from_persistence.is_empty(),
        "Origin::Load should bypass persistence; got rows: {listed_from_persistence:?}"
    );
}

#[tokio::test]
async fn origin_clear_skips_persistence_and_remote() {
    let dir = TempDir::new().unwrap();
    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: dir.path().join("db"),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };
    let store = Store::new(fixture_config(), options);
    store.initialize().await.unwrap();

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
                ("projectId", json!("p1")),
                ("title", json!("hello")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();

    store.delete("task", "t1", Origin::Clear).await.unwrap();

    assert!(
        store.read("task", "t1").await.unwrap().is_none(),
        "memory should reflect the Clear delete"
    );
    let in_persistence = store
        .list(
            "task",
            Some(stitch::ListFilter {
                scope_id: Some("p1".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        in_persistence.len(),
        1,
        "Origin::Clear should NOT propagate to persistence; persisted row should remain"
    );
}

#[tokio::test]
async fn subscribe_persistence_sees_writes_outside_current_scope() {
    let dir = TempDir::new().unwrap();
    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: dir.path().join("db"),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };
    let store = Store::new(fixture_config(), options);
    store.initialize().await.unwrap();
    let mut persistence_rx = store.subscribe_persistence().unwrap().unwrap();

    store
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1")), ("name", json!("Other"))]),
            Origin::Local,
        )
        .await
        .unwrap();

    let StoreEvent::Mutation(event) = persistence_rx.recv().await.unwrap() else {
        panic!("expected Mutation event on persistence bus");
    };
    assert_eq!(event.entity, "project");
    assert_eq!(event.id, "p1");
}

#[tokio::test]
async fn subscribe_persistence_returns_none_without_persistence() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
    assert!(store.subscribe_persistence().unwrap().is_none());
}

#[tokio::test]
async fn batch_dedupes_events_per_scope_entity() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
    let mut rx = store.subscribe().unwrap();

    store.begin_batch().unwrap();
    for i in 0..5 {
        store
            .create(
                "task",
                "p1",
                make_record(&[
                    ("id", json!(format!("t{i}"))),
                    ("title", json!(format!("task-{i}"))),
                    ("projectId", json!("p1")),
                ]),
                Origin::Local,
            )
            .await
            .unwrap();
    }
    store.end_batch().unwrap();

    let mut events: Vec<stitch::types::MutationEvent> = Vec::new();
    while let Ok(Ok(event)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await
    {
        if let StoreEvent::Mutation(m) = event {
            events.push(m);
        }
    }
    assert_eq!(
        events.len(),
        1,
        "expected one consolidated event per (scope, entity); got {events:?}"
    );
    assert_eq!(events[0].entity, "task");
    assert_eq!(events[0].scope_id, "p1");
}

#[tokio::test]
async fn batch_with_no_mutations_is_harmless() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
    store.begin_batch().unwrap();
    store.end_batch().unwrap();
    store.end_batch().unwrap();
}

#[tokio::test]
async fn origin_remote_writes_to_memory_only_when_no_persistence() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();

    store
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1")), ("name", json!("server-pushed"))]),
            Origin::Remote,
        )
        .await
        .unwrap();

    let got = store.read("project", "p1").await.unwrap().unwrap();
    assert_eq!(
        got.get("name").and_then(Value::as_str),
        Some("server-pushed")
    );
}

#[tokio::test]
async fn list_with_scope_filter_returns_matching_children() {
    let dir = TempDir::new().unwrap();
    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: dir.path().join("db"),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };
    let store = Store::new(fixture_config(), options);
    store.initialize().await.unwrap();

    store
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1")), ("name", json!("A"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    store
        .create(
            "project",
            "p2",
            make_record(&[("id", json!("p2")), ("name", json!("B"))]),
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
                ("title", json!("p1-task")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();
    store
        .create(
            "task",
            "p2",
            make_record(&[
                ("id", json!("t2")),
                ("title", json!("p2-task")),
                ("projectId", json!("p2")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();

    let tasks_p1 = store
        .list(
            "task",
            Some(stitch::ListFilter {
                scope_id: Some("p1".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(tasks_p1.len(), 1);
    assert_eq!(tasks_p1[0].get("id").and_then(Value::as_str), Some("t1"));
}

#[tokio::test]
async fn list_root_entities_honors_sort() {
    let dir = TempDir::new().unwrap();
    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: dir.path().join("db"),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };
    let store = Store::new(fixture_config(), options);
    store.initialize().await.unwrap();

    for (id, name) in [("p3", "Charlie"), ("p1", "Alpha"), ("p2", "Bravo")] {
        store
            .create(
                "project",
                id,
                make_record(&[("id", json!(id)), ("name", json!(name))]),
                Origin::Local,
            )
            .await
            .unwrap();
    }

    let sorted = store
        .list_root_entities(vec![stitch::SortField {
            field: "name".into(),
            direction: stitch::SortDirection::Asc,
        }])
        .await
        .unwrap();
    let names: Vec<&str> = sorted
        .iter()
        .filter_map(|r| r.get("name").and_then(Value::as_str))
        .collect();
    assert_eq!(names, vec!["Alpha", "Bravo", "Charlie"]);
}

#[tokio::test]
async fn child_count_returns_matching_rows() {
    let dir = TempDir::new().unwrap();
    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: dir.path().join("db"),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };
    let store = Store::new(fixture_config(), options);
    store.initialize().await.unwrap();

    store
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    for i in 0..3 {
        store
            .create(
                "task",
                "p1",
                make_record(&[
                    ("id", json!(format!("t{i}"))),
                    ("title", json!("x")),
                    ("projectId", json!("p1")),
                ]),
                Origin::Local,
            )
            .await
            .unwrap();
    }

    assert_eq!(store.child_count("task", "p1").await.unwrap(), 3);
    assert_eq!(store.child_count("task", "p2").await.unwrap(), 0);
}

#[tokio::test]
async fn subscribe_entity_filters_by_entity_name() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();

    let mut rx = store.subscribe_entity("task").unwrap();

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
                ("title", json!("hi")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
        .await
        .expect("subscriber should receive within timeout")
        .expect("channel should be open");
    assert_eq!(event.entity, "task");
    assert_eq!(event.id, "t1");
    assert_eq!(event.operation, Operation::Insert);
}

#[tokio::test]
async fn subscribe_entity_receives_remote_origin_events_when_no_persistence() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();

    let mut rx = store.subscribe_entity("project").unwrap();

    store
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1")), ("name", json!("inbound"))]),
            Origin::Remote,
        )
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
        .await
        .expect("subscriber should receive within timeout")
        .expect("channel should be open");
    assert_eq!(event.entity, "project");
    assert_eq!(event.id, "p1");
}

#[tokio::test]
async fn subscribe_entity_with_persistence_delivers_exactly_one_event_per_local_create() {
    let dir = TempDir::new().unwrap();
    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: dir.path().join("db"),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };
    let store = Store::new(fixture_config(), options);
    store.initialize().await.unwrap();

    let mut rx = store.subscribe_entity("project").unwrap();

    store
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .unwrap();

    let mut events = Vec::new();
    for _ in 0..3 {
        match tokio::time::timeout(std::time::Duration::from_millis(150), rx.recv()).await {
            Ok(Some(event)) => events.push(event),
            _ => break,
        }
    }
    assert_eq!(
        events.len(),
        1,
        "memory-bus Local events should be filtered out; only persistence-bus event should remain. got: {events:?}"
    );
    assert_eq!(events[0].entity, "project");
    assert_eq!(events[0].id, "p1");
}

#[tokio::test]
async fn subscribe_scope_entity_filters_by_scope_and_entity() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();

    let mut rx = store.subscribe_scope_entity("p1", "task").unwrap();

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
            "project",
            "p2",
            make_record(&[("id", json!("p2"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    store
        .create(
            "task",
            "p2",
            make_record(&[
                ("id", json!("t_other")),
                ("title", json!("nope")),
                ("projectId", json!("p2")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();
    store
        .create(
            "task",
            "p1",
            make_record(&[
                ("id", json!("t_match")),
                ("title", json!("yes")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
        .await
        .expect("subscriber should receive within timeout")
        .expect("channel should be open");
    assert_eq!(event.entity, "task");
    assert_eq!(event.id, "t_match");
    assert_eq!(event.scope_id, "p1");
}

#[tokio::test]
async fn ready_is_false_before_initialize_and_true_after() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    assert!(!store.ready());
    store.initialize().await.unwrap();
    assert!(store.ready());
}

#[tokio::test]
async fn initial_sync_done_is_true_without_remote() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
    assert!(store.initial_sync_done().unwrap());
}

#[tokio::test]
async fn is_reconnecting_is_false_before_first_connect() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
    assert!(!store.is_reconnecting().unwrap());
}

#[tokio::test]
async fn request_without_remote_returns_config_error() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
    let err = store
        .request("anything", Value::Object(Map::new()))
        .await
        .unwrap_err();
    assert!(matches!(err, stitch::Error::Config(_)));
}

#[tokio::test]
async fn memory_create_strips_null_scope_field_then_injects_argument() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
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
                ("title", json!("body")),
                ("projectId", Value::Null),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();
    let task = store.read("task", "t1").await.unwrap().unwrap();
    assert_eq!(
        task.get("projectId").and_then(Value::as_str),
        Some("p1"),
        "explicit null scope_field should be stripped and re-injected from the scope_id arg"
    );
}

#[tokio::test]
async fn memory_create_strips_null_fields() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();
    store
        .create(
            "project",
            "p1",
            make_record(&[
                ("id", json!("p1")),
                ("name", json!("Alpha")),
                ("missing", Value::Null),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();
    let got = store.read("project", "p1").await.unwrap().unwrap();
    assert!(
        !got.contains_key("missing"),
        "null-valued field should be stripped before insert"
    );
}

#[tokio::test]
async fn update_local_state_upserts_when_missing() {
    let dir = TempDir::new().unwrap();
    let options = StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: dir.path().join("db"),
            passphrase: None,
        }),
        ..StoreOptions::default()
    };
    let store = Store::new(fixture_config(), options);
    store.initialize().await.unwrap();

    store
        .update_local_state("project", "p1", make_record(&[("name", json!("created"))]))
        .await
        .unwrap();
    let got = store
        .read_local_state("project", "p1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.get("name").and_then(Value::as_str), Some("created"));

    store
        .update_local_state("project", "p1", make_record(&[("name", json!("updated"))]))
        .await
        .unwrap();
    let got = store
        .read_local_state("project", "p1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.get("name").and_then(Value::as_str), Some("updated"));
}

#[tokio::test]
async fn reset_for_logout_clears_auth_and_status() {
    let store = Store::new(fixture_config(), StoreOptions::default());
    store.initialize().await.unwrap();

    store
        .set_authenticated_user(Some("user-1".into()))
        .unwrap();
    store.reset_for_logout().await.unwrap();

    assert!(store.current_scope().unwrap().is_none());
    assert_eq!(
        store.connection_status().unwrap(),
        stitch::ConnectionStatus::Offline
    );
}
