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
