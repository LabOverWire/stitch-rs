mod common;

use common::{fixture_config, init_tracing, make_record};
use serde_json::json;
use std::time::Duration;
use stitch::config::RemoteConfig;
use stitch::{ConnectionStatus, Origin, Store, StoreOptions};
use stitch_harness::{Access, BrokerHarness};
use tokio::time::sleep;

async fn reaches_connected(store: &Store) -> bool {
    for _ in 0..50 {
        if matches!(store.connection_status(), Ok(ConnectionStatus::Connected)) {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

fn authed_options(url: String, user: &str, pass: &str) -> StoreOptions {
    let mut remote = RemoteConfig::new(url);
    remote.username = Some(user.to_string());
    remote.password = Some(pass.to_string());
    StoreOptions {
        persistence: None,
        remote: Some(remote),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_store_crud_round_trips_through_broker() {
    init_tracing();
    let broker = BrokerHarness::new()
        .scope("project", "projectId")
        .user("app", "secret")
        .allow("app", "$DB/#", Access::ReadWrite)
        .start()
        .await
        .expect("start broker");

    let store = Store::with_client_id(
        fixture_config(),
        authed_options(broker.tcp_url(), "app", "secret"),
        "app-client".into(),
    );
    store.initialize().await.expect("initialize");
    assert!(
        reaches_connected(&store).await,
        "an authorized user must reach Connected against the broker"
    );

    store
        .create(
            "project",
            "",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .expect("create project through broker");
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
        .expect("create task through broker");

    let task = store.read("task", "t1").await.expect("read task");
    assert!(
        task.is_some(),
        "a task created through the broker must be readable back"
    );

    store.shutdown().await.expect("shutdown store");
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_store_with_the_same_client_id_resumes_its_session() {
    init_tracing();
    let broker = BrokerHarness::new()
        .scope("project", "projectId")
        .user("app", "secret")
        .allow("app", "$DB/#", Access::ReadWrite)
        .start()
        .await
        .expect("start broker");

    for run in 0..2 {
        let store = Store::with_client_id(
            fixture_config(),
            authed_options(broker.tcp_url(), "app", "secret"),
            "stable-client".into(),
        );
        store.initialize().await.expect("initialize");
        assert!(
            reaches_connected(&store).await,
            "run {run}: a store reusing its client id must reconnect to its stored session"
        );
        store.shutdown().await.expect("shutdown store");
    }

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_password_never_reaches_connected() {
    init_tracing();
    let broker = BrokerHarness::new()
        .user("app", "secret")
        .allow("app", "$DB/#", Access::ReadWrite)
        .start()
        .await
        .expect("start broker");

    let store = Store::with_client_id(
        fixture_config(),
        authed_options(broker.tcp_url(), "app", "wrong-password"),
        "bad-client".into(),
    );

    if store.initialize().await.is_err() {
        broker.shutdown().await;
        return;
    }
    assert!(
        !reaches_connected(&store).await,
        "a wrong password must never reach Connected"
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_create_on_unique_conflict_returns_err_and_leaves_no_phantom() {
    init_tracing();
    let broker = BrokerHarness::new()
        .scope("project", "projectId")
        .unique_constraint("task", ["title"])
        .start()
        .await
        .expect("start broker");

    let store = Store::with_client_id(
        fixture_config(),
        StoreOptions {
            persistence: None,
            remote: Some(RemoteConfig::new(broker.tcp_url())),
        },
        "app-client".into(),
    );
    store.initialize().await.expect("initialize");
    assert!(reaches_connected(&store).await, "must reach Connected");

    store
        .create(
            "project",
            "",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .expect("create project");

    store
        .create(
            "task",
            "p1",
            make_record(&[
                ("id", json!("t1")),
                ("title", json!("seatA")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .expect("first claim wins");

    let err = store
        .create(
            "task",
            "p1",
            make_record(&[
                ("id", json!("t2")),
                ("title", json!("seatA")),
                ("projectId", json!("p1")),
            ]),
            Origin::Local,
        )
        .await
        .expect_err("a second claim on the same unique key must be rejected");
    assert!(err.is_conflict(), "expected Conflict, got {err:?}");

    assert!(
        store.read("task", "t2").await.expect("read t2").is_none(),
        "the losing create must leave no phantom local row"
    );
    assert!(
        store.read("task", "t1").await.expect("read t1").is_some(),
        "the winning row must remain"
    );

    store.shutdown().await.expect("shutdown store");
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queued_create_the_broker_already_holds_is_kept_after_flush() {
    init_tracing();
    let broker = BrokerHarness::new()
        .scope("project", "projectId")
        .unique_constraint("task", ["title"])
        .start()
        .await
        .expect("start broker");
    let task = || {
        make_record(&[
            ("id", json!("t1")),
            ("title", json!("seatA")),
            ("projectId", json!("p1")),
        ])
    };

    let online = Store::with_client_id(
        fixture_config(),
        StoreOptions {
            persistence: None,
            remote: Some(RemoteConfig::new(broker.tcp_url())),
        },
        "first-sender".into(),
    );
    online.initialize().await.expect("initialize");
    assert!(reaches_connected(&online).await);
    online
        .create(
            "project",
            "",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .expect("create project");
    online
        .create("task", "p1", task(), Origin::Local)
        .await
        .expect("create applied by the broker");

    let mut remote = RemoteConfig::new(broker.tcp_url());
    remote.auto_connect = false;
    let replaying = Store::with_client_id(
        fixture_config(),
        StoreOptions {
            persistence: None,
            remote: Some(remote),
        },
        "replaying-sender".into(),
    );
    replaying.initialize().await.expect("initialize");
    replaying
        .create("task", "p1", task(), Origin::Local)
        .await
        .expect("queued while disconnected");
    replaying
        .reconnect(&broker.tcp_url(), None, None, None)
        .await
        .expect("reconnect");
    assert!(reaches_connected(&replaying).await);

    for _ in 0..200 {
        if replaying.pending_count("p1").await.expect("pending") == 0 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        replaying.pending_count("p1").await.expect("pending"),
        0,
        "the queued create must be flushed"
    );
    assert!(
        replaying.read("task", "t1").await.expect("read").is_some(),
        "a queued create the broker already holds must not be rolled back on flush"
    );

    online.shutdown().await.expect("shutdown");
    replaying.shutdown().await.expect("shutdown");
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_if_and_delete_if_fence_on_server_version() {
    init_tracing();
    let broker = BrokerHarness::new()
        .scope("project", "projectId")
        .start()
        .await
        .expect("start broker");

    let store = Store::with_client_id(
        fixture_config(),
        StoreOptions {
            persistence: None,
            remote: Some(RemoteConfig::new(broker.tcp_url())),
        },
        "cas-client".into(),
    );
    store.initialize().await.expect("initialize");
    assert!(reaches_connected(&store).await, "must reach Connected");

    store
        .create(
            "project",
            "",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .expect("create project");

    let v2 = store
        .update_if("project", "p1", make_record(&[("name", json!("Beta"))]), 1)
        .await
        .expect("CAS update with the current version succeeds");
    assert_eq!(v2, 2, "update_if returns the bumped server version");

    let cached = store
        .read("project", "p1")
        .await
        .expect("read p1")
        .expect("row present after CAS update");
    assert_eq!(
        Store::row_version(&cached),
        Some(v2),
        "update_if must refresh the cached _version so a read-modify-write loop sees it"
    );

    let stale = store
        .update_if("project", "p1", make_record(&[("name", json!("Gamma"))]), 1)
        .await
        .expect_err("a stale expected_version must be rejected");
    assert!(
        stale.is_precondition_failed(),
        "expected PreconditionFailed, got {stale:?}"
    );

    let stale_delete = store
        .delete_if("project", "p1", 1)
        .await
        .expect_err("a stale delete must be rejected");
    assert!(
        stale_delete.is_precondition_failed(),
        "expected PreconditionFailed, got {stale_delete:?}"
    );

    store
        .delete_if(
            "project",
            "p1",
            Store::row_version(&cached).expect("cached version"),
        )
        .await
        .expect("CAS delete with the cached version succeeds");

    store.shutdown().await.expect("shutdown store");
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_of_a_queued_update_does_not_resurrect_a_remotely_deleted_record() {
    init_tracing();
    let broker = BrokerHarness::new()
        .scope("project", "projectId")
        .start()
        .await
        .expect("start broker");

    let remote = || Some(RemoteConfig::new(broker.tcp_url()));
    let editor = Store::with_client_id(
        fixture_config(),
        StoreOptions {
            persistence: None,
            remote: remote(),
        },
        "editor".into(),
    );
    let deleter = Store::with_client_id(
        fixture_config(),
        StoreOptions {
            persistence: None,
            remote: remote(),
        },
        "deleter".into(),
    );
    editor.initialize().await.expect("initialize editor");
    deleter.initialize().await.expect("initialize deleter");
    assert!(reaches_connected(&editor).await, "editor must connect");
    assert!(reaches_connected(&deleter).await, "deleter must connect");
    deleter
        .replace_scope("p1")
        .await
        .expect("deleter opens scope");

    editor
        .create(
            "project",
            "",
            make_record(&[("id", json!("p1")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .expect("create project");
    editor
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
        .expect("create task");

    let mut saw = false;
    for _ in 0..200 {
        if deleter.read("task", "t1").await.expect("read").is_some() {
            saw = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(saw, "deleter must observe t1 synced from the broker");

    editor.disconnect().await.expect("disconnect editor");
    sleep(Duration::from_millis(100)).await;
    editor
        .update(
            "task",
            "t1",
            make_record(&[("title", json!("edited-offline"))]),
            Origin::Local,
        )
        .await
        .expect("queue offline update");
    assert!(
        editor.pending_count("p1").await.expect("pending") > 0,
        "the offline update must be queued"
    );

    deleter
        .delete("task", "t1", Origin::Local)
        .await
        .expect("delete t1 on the broker");
    let mut gone = false;
    for _ in 0..200 {
        if deleter.read("task", "t1").await.expect("read").is_none() {
            gone = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(gone, "t1 must be gone from the broker before the flush");

    editor
        .reconnect(&broker.tcp_url(), None, None, None)
        .await
        .expect("reconnect editor");
    assert!(reaches_connected(&editor).await, "editor must reconnect");
    let mut drained = false;
    for _ in 0..200 {
        if editor.pending_count("p1").await.expect("pending") == 0 {
            drained = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(drained, "the flush must drain the queued update");

    sleep(Duration::from_millis(500)).await;

    let on_broker = deleter.read("task", "t1").await.expect("deleter read");
    assert!(
        on_broker.is_none(),
        "a queued update flushed after a remote delete must not resurrect the record on the broker, but it reappeared as {on_broker:?}"
    );

    let on_editor = editor.read("task", "t1").await.expect("editor read");
    assert!(
        on_editor.is_none(),
        "the flushing store must not keep a local copy of a record the broker reports as deleted, but it still holds {on_editor:?}"
    );

    editor.shutdown().await.expect("shutdown editor");
    deleter.shutdown().await.expect("shutdown deleter");
    broker.shutdown().await;
}
