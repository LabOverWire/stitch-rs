mod common;

use common::{fixture_config, init_tracing, make_record};
use mqtt5::types::{Message, SubscribeOptions};
use mqtt5::{MqttClient, QoS};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use stitch::config::{RemoteConfig, WillConfig};
use stitch::{ConnectionStatus, Origin, Store, StoreConfig, StoreOptions};
use stitch_harness::BrokerHarness;
use tokio::time::sleep;

type Seen = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

fn config_with_will(topic: &str, payload: &[u8]) -> StoreConfig {
    let mut cfg = fixture_config();
    cfg.keep_alive_secs = 2;
    let mut will = WillConfig::new(topic, payload.to_vec());
    will.qos = 1;
    cfg.will = Some(will);
    cfg
}

fn options(url: String) -> StoreOptions {
    StoreOptions {
        persistence: None,
        remote: Some(RemoteConfig::new(url)),
    }
}

async fn wait_connected(store: &Store) {
    for _ in 0..50 {
        if matches!(store.connection_status(), Ok(ConnectionStatus::Connected)) {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("store never reached Connected");
}

async fn spawn_observer(url: &str, pattern: &str) -> (MqttClient, Seen) {
    let client = MqttClient::new("will-observer");
    client.connect(url).await.expect("observer connect");
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    client
        .subscribe_with_options(
            pattern.to_string(),
            SubscribeOptions {
                qos: QoS::AtLeastOnce,
                ..SubscribeOptions::default()
            },
            move |msg: Message| {
                sink.lock().unwrap().push((msg.topic, msg.payload));
            },
        )
        .await
        .expect("observer subscribe");
    (client, seen)
}

fn observed(seen: &Seen, topic: &str, payload: &[u8]) -> bool {
    seen.lock()
        .unwrap()
        .iter()
        .any(|(t, p)| t == topic && p.as_slice() == payload)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn will_fires_on_ungraceful_disconnect() {
    init_tracing();
    let broker = BrokerHarness::new().start().await.expect("start broker");
    let (_observer, seen) = spawn_observer(&broker.tcp_url(), "presence/#").await;

    let store = Store::with_client_id(
        config_with_will("presence/client-a", b"gone"),
        options(broker.tcp_url()),
        "client-a".into(),
    );
    store.initialize().await.expect("initialize");
    wait_connected(&store).await;

    store
        .create(
            "project",
            "",
            make_record(&[("id", json!("p-a")), ("name", json!("Alpha"))]),
            Origin::Local,
        )
        .await
        .expect("a live session must serve CRUD before it dies");

    store
        .disconnect_abnormally()
        .await
        .expect("abnormal disconnect");

    let mut fired = false;
    for _ in 0..50 {
        if observed(&seen, "presence/client-a", b"gone") {
            fired = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(
        fired,
        "broker must publish the registered will on an ungraceful disconnect"
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn will_does_not_fire_on_graceful_disconnect() {
    init_tracing();
    let broker = BrokerHarness::new().start().await.expect("start broker");
    let (_observer, seen) = spawn_observer(&broker.tcp_url(), "presence/#").await;

    let store = Store::with_client_id(
        config_with_will("presence/client-b", b"gone"),
        options(broker.tcp_url()),
        "client-b".into(),
    );
    store.initialize().await.expect("initialize");
    wait_connected(&store).await;

    store.disconnect().await.expect("graceful disconnect");

    for _ in 0..20 {
        assert!(
            !observed(&seen, "presence/client-b", b"gone"),
            "a graceful disconnect must not fire the will"
        );
        sleep(Duration::from_millis(100)).await;
    }

    broker.shutdown().await;
}
