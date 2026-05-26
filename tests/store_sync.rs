//! App-level convergence: two `Store`s exchanging JSON documents over a pipe
//! via the session protocol. Exercises the full stack — Store → SyncNode →
//! session — with real serialized records.

use serde_json::{Map, Value, json};
use std::time::Duration;
use stitch_p2p::{PEER_ID_LEN, PeerId, Store, session};
use tokio::io::{DuplexStream, split};
use tokio::task::JoinHandle;

fn peer(n: u8) -> PeerId {
    let mut id = [0u8; PEER_ID_LEN];
    id[0] = n;
    id
}

const PULL: Duration = Duration::from_millis(20);

fn connect(a: &Store, a_io: DuplexStream, b: &Store, b_io: DuplexStream) -> Vec<JoinHandle<()>> {
    let rx_a = a.node().register_session();
    let (ar, aw) = split(a_io);
    let sa = a.node().state();
    let rx_b = b.node().register_session();
    let (br, bw) = split(b_io);
    let sb = b.node().state();
    vec![
        tokio::spawn(async move {
            let _ = session::run(sa, ar, aw, rx_a, PULL).await;
        }),
        tokio::spawn(async move {
            let _ = session::run(sb, br, bw, rx_b, PULL).await;
        }),
    ]
}

async fn wait_until<F, Fut>(timeout: Duration, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_stores_converge_on_documents() {
    let a = Store::new(peer(1));
    let b = Store::new(peer(2));

    a.create("task", "t1", json!({"title": "alpha"}))
        .await
        .unwrap();
    b.create("task", "t2", json!({"title": "beta"}))
        .await
        .unwrap();

    let (a_io, b_io) = tokio::io::duplex(64 * 1024);
    let handles = connect(&a, a_io, &b, b_io);

    let ok = wait_until(Duration::from_secs(3), || async {
        a.read("task", "t2").await.is_some() && b.read("task", "t1").await.is_some()
    })
    .await;
    for h in &handles {
        h.abort();
    }
    assert!(ok, "stores did not converge");
    assert_eq!(a.read("task", "t2").await, Some(json!({"title": "beta"})));
    assert_eq!(b.read("task", "t1").await, Some(json!({"title": "alpha"})));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_edits_resolve_identically() {
    let a = Store::new(peer(1));
    let b = Store::new(peer(2));

    // Both edit the same record before connecting — a conflict.
    a.create("doc", "d1", json!({"body": "from-a"}))
        .await
        .unwrap();
    b.create("doc", "d1", json!({"body": "from-b"}))
        .await
        .unwrap();

    let (a_io, b_io) = tokio::io::duplex(64 * 1024);
    let handles = connect(&a, a_io, &b, b_io);

    let ok = wait_until(Duration::from_secs(3), || async {
        a.read("doc", "d1").await == b.read("doc", "d1").await
            && a.read("doc", "d1").await.is_some()
    })
    .await;
    for h in &handles {
        h.abort();
    }
    assert!(ok, "concurrent edit did not converge");
    // Deterministic winner (peer-id tiebreak), identical on both.
    assert_eq!(a.read("doc", "d1").await, b.read("doc", "d1").await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_then_sync_propagates_merge() {
    let a = Store::new(peer(1));
    let b = Store::new(peer(2));

    a.create("task", "t1", json!({"title": "x", "done": false}))
        .await
        .unwrap();
    let mut fields = Map::new();
    fields.insert("done".into(), Value::Bool(true));
    a.update("task", "t1", fields).await.unwrap();

    let (a_io, b_io) = tokio::io::duplex(64 * 1024);
    let handles = connect(&a, a_io, &b, b_io);

    let ok = wait_until(Duration::from_secs(3), || async {
        b.read("task", "t1").await == Some(json!({"title": "x", "done": true}))
    })
    .await;
    for h in &handles {
        h.abort();
    }
    assert!(ok, "merged update did not propagate");
}
