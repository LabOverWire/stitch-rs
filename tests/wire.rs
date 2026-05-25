mod common;

use common::{BrokerFixture, fixture_config, init_tracing, make_record};
use serde_json::{Value, json};
use std::time::Duration;
use stitch::config::{
    EntityDefinition, FieldType, PersistenceConfig, RemoteConfig, SchemaField, ScopeConfig,
    StoreConfig, TopLevelEntity,
};
use stitch::types::StoreEvent;
use stitch::{Origin, Store, StoreOptions};
use tempfile::TempDir;
use tokio::time::sleep;

fn options_with_remote(server_url: String, dir: &TempDir) -> StoreOptions {
    StoreOptions {
        persistence: Some(PersistenceConfig {
            db_path: dir.path().join("db"),
            passphrase: None,
        }),
        remote: Some(RemoteConfig::new(server_url)),
    }
}

async fn wait_for_connected(store: &Store) {
    for _ in 0..50 {
        if matches!(
            store.connection_status(),
            Ok(stitch::ConnectionStatus::Connected)
        ) {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "store never reached Connected, last status: {:?}",
        store.connection_status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_stores_sync_creates_via_broker() {
    init_tracing();
    let broker = BrokerFixture::start().await;
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let store_a = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir_a),
        "client-a".into(),
    );
    let store_b = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir_b),
        "client-b".into(),
    );

    store_a.initialize().await.unwrap();
    store_b.initialize().await.unwrap();
    wait_for_connected(&store_a).await;
    wait_for_connected(&store_b).await;

    store_b.replace_scope("p1").await.unwrap();
    let mut rx = store_b.subscribe().unwrap();

    store_a
        .create(
            "project",
            "",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    store_a
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_task = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
            && let StoreEvent::Mutation(m) = event
            && m.entity == "task"
            && m.id == "t1"
        {
            saw_task = true;
            break;
        }
    }
    assert!(saw_task, "store_b never observed the remote task insert");

    let local = store_b.read("task", "t1").await.unwrap();
    assert!(local.is_some(), "store_b should hold the task locally");

    store_a.shutdown().await.unwrap();
    store_b.shutdown().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_create_flushes_after_connect() {
    init_tracing();
    let broker = BrokerFixture::start().await;
    let dir = TempDir::new().unwrap();

    let store_a = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir),
        "client-a-offline".into(),
    );
    store_a.initialize().await.unwrap();
    wait_for_connected(&store_a).await;
    store_a
        .set_authenticated_user(Some("user-1".into()))
        .unwrap();

    let dir_b = TempDir::new().unwrap();
    let store_b = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir_b),
        "client-b-observer".into(),
    );
    store_b.initialize().await.unwrap();
    wait_for_connected(&store_b).await;
    store_b.replace_scope("p1").await.unwrap();

    store_a.disconnect().await.unwrap();
    sleep(Duration::from_millis(100)).await;

    store_a
        .create(
            "project",
            "",
            make_record(&[("id", json!("p1")), ("name", json!("Offline"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    store_a
        .create(
            "task",
            "p1",
            make_record(&[
                ("id", json!("queued")),
                ("title", json!("offline-write")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();

    store_a.reconnect(&broker.url(), None).await.unwrap();
    wait_for_connected(&store_a).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        if let Some(record) = store_b.read("task", "queued").await.unwrap()
            && record.get("title").and_then(Value::as_str) == Some("offline-write")
        {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let mirrored = store_b.read("task", "queued").await.unwrap();
    assert!(
        mirrored.is_some(),
        "queued task should have flushed and been mirrored on store_b"
    );

    store_a.shutdown().await.unwrap();
    store_b.shutdown().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_sync_populates_local_state_from_server() {
    init_tracing();
    let broker = BrokerFixture::start().await;
    let dir_a = TempDir::new().unwrap();

    let store_a = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir_a),
        "client-a-seed".into(),
    );
    store_a.initialize().await.unwrap();
    wait_for_connected(&store_a).await;
    store_a
        .set_authenticated_user(Some("user-1".into()))
        .unwrap();

    store_a
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1")), ("name", json!("Seeded"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    store_a
        .create(
            "task",
            "p1",
            make_record(&[
                ("id", json!("t-seed")),
                ("title", json!("from-server")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();
    sleep(Duration::from_millis(150)).await;

    let dir_b = TempDir::new().unwrap();
    let store_b = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir_b),
        "client-b-fresh".into(),
    );
    store_b.initialize().await.unwrap();
    wait_for_connected(&store_b).await;
    store_b
        .set_authenticated_user(Some("user-2".into()))
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let roots = store_b.list_root_entities(Vec::new()).await.unwrap();
        if roots
            .iter()
            .any(|r| r.get("id").and_then(Value::as_str) == Some("p1"))
        {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let roots = store_b.list_root_entities(Vec::new()).await.unwrap();
    assert!(
        roots
            .iter()
            .any(|r| r.get("id").and_then(Value::as_str) == Some("p1")),
        "store_b should have synced project p1 from server during initial sync; got: {roots:?}"
    );

    store_b.replace_scope("p1").await.unwrap();
    let task = store_b.read("task", "t-seed").await.unwrap();
    assert!(
        task.is_some(),
        "store_b should see the seeded task after replace_scope"
    );

    store_a.shutdown().await.unwrap();
    store_b.shutdown().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn higher_version_remote_update_wins() {
    init_tracing();
    let broker = BrokerFixture::start().await;
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let store_a = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir_a),
        "client-a-conflict".into(),
    );
    let store_b = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir_b),
        "client-b-conflict".into(),
    );
    store_a.initialize().await.unwrap();
    store_b.initialize().await.unwrap();
    wait_for_connected(&store_a).await;
    wait_for_connected(&store_b).await;
    store_a
        .set_authenticated_user(Some("user-a".into()))
        .unwrap();
    store_b
        .set_authenticated_user(Some("user-b".into()))
        .unwrap();

    store_a.replace_scope("p1").await.unwrap();
    store_b.replace_scope("p1").await.unwrap();

    store_a
        .create(
            "project",
            "p1",
            make_record(&[("id", json!("p1")), ("name", json!("base"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    store_a
        .create(
            "task",
            "p1",
            make_record(&[
                ("id", json!("t1")),
                ("title", json!("base")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();
    sleep(Duration::from_millis(200)).await;

    store_b
        .update(
            "task",
            "t1",
            make_record(&[("title", json!("winner"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    sleep(Duration::from_millis(200)).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        if let Some(record) = store_a.read("task", "t1").await.unwrap()
            && record.get("title").and_then(Value::as_str) == Some("winner")
        {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let a_record = store_a.read("task", "t1").await.unwrap().unwrap();
    assert_eq!(
        a_record.get("title").and_then(Value::as_str),
        Some("winner"),
        "store_a should converge to store_b's winning update"
    );

    store_a.shutdown().await.unwrap();
    store_b.shutdown().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_mutation_bumps_root_scope_version() {
    init_tracing();
    let broker = BrokerFixture::start().await;
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let store_a = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir_a),
        "client-a-bump".into(),
    );
    let store_b = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir_b),
        "client-b-bump".into(),
    );
    store_a.initialize().await.unwrap();
    store_b.initialize().await.unwrap();
    wait_for_connected(&store_a).await;
    wait_for_connected(&store_b).await;

    store_b.replace_scope("p1").await.unwrap();
    let mut rx = store_b.subscribe().unwrap();

    store_a
        .create(
            "project",
            "",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    store_a
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_project_update = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
            && let StoreEvent::Mutation(m) = event
            && m.entity == "project"
            && m.id == "p1"
            && matches!(m.operation, stitch::Operation::Update)
        {
            saw_project_update = true;
            break;
        }
    }
    assert!(
        saw_project_update,
        "store_b never observed the bumped project update following a child mutation"
    );

    let project = store_b
        .read("project", "p1")
        .await
        .unwrap()
        .expect("store_b should hold the project locally");
    let version = project
        .get("version")
        .and_then(Value::as_i64)
        .expect("project should carry a numeric version after the bump");
    assert!(version > 0, "version should be a positive ms timestamp");

    store_a.shutdown().await.unwrap();
    store_b.shutdown().await.unwrap();
    broker.shutdown().await;
}

fn config_with_top_level_notification() -> StoreConfig {
    let mut entities = HashMap::new();
    entities.insert(
        "project".to_string(),
        EntityDefinition {
            fields: vec![SchemaField {
                name: "id".into(),
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
                    name: "id".into(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
                SchemaField {
                    name: "projectId".into(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
            ],
            ..EntityDefinition::default()
        },
    );
    entities.insert(
        "notification".to_string(),
        EntityDefinition {
            fields: vec![
                SchemaField {
                    name: "id".into(),
                    r#type: FieldType::String,
                    required: true,
                    default: None,
                },
                SchemaField {
                    name: "body".into(),
                    r#type: FieldType::String,
                    required: false,
                    default: None,
                },
            ],
            ..EntityDefinition::default()
        },
    );

    let mut config = StoreConfig::new(
        entities,
        ScopeConfig {
            root_entity: "project".to_string(),
            child_entities: vec!["task".to_string()],
            scope_field: "projectId".to_string(),
        },
    );
    config.top_level_entities = vec![TopLevelEntity {
        entity: "notification".to_string(),
        subscription_pattern: "$DB/notification/events/#".to_string(),
    }];
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn top_level_entity_propagates_via_wildcard() {
    init_tracing();
    let broker = BrokerFixture::start().await;
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let store_a = Store::with_client_id(
        config_with_top_level_notification(),
        options_with_remote(broker.url(), &dir_a),
        "client-a-tl".into(),
    );
    let store_b = Store::with_client_id(
        config_with_top_level_notification(),
        options_with_remote(broker.url(), &dir_b),
        "client-b-tl".into(),
    );
    store_a.initialize().await.unwrap();
    store_b.initialize().await.unwrap();
    wait_for_connected(&store_a).await;
    wait_for_connected(&store_b).await;

    let mut rx = store_b.subscribe_persistence().unwrap().unwrap();

    sleep(Duration::from_millis(800)).await;

    store_a
        .create(
            "notification",
            "",
            make_record(&[("id", json!("n1")), ("body", json!("hello"))]),
            Origin::Local,
        )
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(StoreEvent::Mutation(m))) =
            tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
            && m.entity == "notification"
            && m.id == "n1"
        {
            saw = true;
            break;
        }
    }
    assert!(
        saw,
        "store_b should observe the top-level notification via wildcard"
    );

    let read_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut local = None;
    while tokio::time::Instant::now() < read_deadline {
        local = store_b.read("notification", "n1").await.unwrap();
        if local.is_some() {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        local.is_some(),
        "notification should be persisted locally on store_b"
    );

    store_a.shutdown().await.unwrap();
    store_b.shutdown().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn applied_version_seeded_from_open_scope_and_cleared_on_close() {
    init_tracing();
    let broker = BrokerFixture::start().await;
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let store_a = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir_a),
        "client-a-applied".into(),
    );
    let store_b = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir_b),
        "client-b-applied".into(),
    );
    store_a.initialize().await.unwrap();
    store_b.initialize().await.unwrap();
    wait_for_connected(&store_a).await;
    wait_for_connected(&store_b).await;

    store_a.replace_scope("p1").await.unwrap();
    store_a
        .create(
            "project",
            "",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .unwrap();
    store_a
        .create(
            "task",
            "p1",
            make_record(&[
                ("id", json!("t1")),
                ("title", json!("seed")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .unwrap();
    sleep(Duration::from_millis(200)).await;

    store_b.replace_scope("p1").await.unwrap();

    let seeded = store_b.applied_version("p1").unwrap();
    assert!(
        seeded.is_some_and(|v| v > 0),
        "store_b should see applied_version after replace_scope; got {seeded:?}"
    );

    store_b.close_scope("p1").await.unwrap();
    assert!(
        store_b.applied_version("p1").unwrap().is_none(),
        "applied_version should be cleared after close_scope"
    );

    store_a.shutdown().await.unwrap();
    store_b.shutdown().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_for_logout_disconnects_and_clears_handlers() {
    init_tracing();
    let broker = BrokerFixture::start().await;
    let dir = TempDir::new().unwrap();
    let store = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir),
        "client-logout".into(),
    );
    store.initialize().await.unwrap();
    wait_for_connected(&store).await;
    store
        .set_authenticated_user(Some("user-1".into()))
        .unwrap();

    let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter_clone = counter.clone();
    store
        .set_session_invalid_handler(move || {
            counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })
        .unwrap();

    store.reset_for_logout().await.unwrap();

    assert_eq!(
        store.connection_status().unwrap(),
        stitch::ConnectionStatus::Offline
    );
    assert!(store.current_scope().unwrap().is_none());
    assert!(!store.is_reconnecting().unwrap());
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "session_invalid handler must not fire from a clean reset_for_logout"
    );

    store.shutdown().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_validator_fires_on_connected() {
    init_tracing();
    let broker = BrokerFixture::start().await;
    let dir = TempDir::new().unwrap();
    let store = Store::with_client_id(
        fixture_config(),
        options_with_remote(broker.url(), &dir),
        "client-validator".into(),
    );
    store.initialize().await.unwrap();
    wait_for_connected(&store).await;

    let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter_clone = counter.clone();
    let validator: stitch::ReconnectValidator = std::sync::Arc::new(move || {
        let c = counter_clone.clone();
        Box::pin(async move {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
    });
    store.set_reconnect_validator(validator).unwrap();

    store.disconnect().await.unwrap();
    sleep(Duration::from_millis(100)).await;
    store.reconnect(&broker.url(), None).await.unwrap();
    wait_for_connected(&store).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if counter.load(std::sync::atomic::Ordering::SeqCst) > 0 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        counter.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "validator should fire on reconnect"
    );

    store.shutdown().await.unwrap();
    broker.shutdown().await;
}

use std::collections::HashMap;
