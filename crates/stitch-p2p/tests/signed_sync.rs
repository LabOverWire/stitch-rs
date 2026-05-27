#![cfg(all(feature = "store", feature = "membership"))]
//! Signed writes end to end: two stores with distinct Ed25519 identities sync
//! over a pipe, each verifying the other's signatures (peer id == public key),
//! and converge.

use serde_json::json;
use std::time::Duration;
use stitch_p2p::{Identity, Store, session};
use tokio::io::{DuplexStream, split};
use tokio::task::JoinHandle;

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
async fn signed_stores_converge() {
    let a = Store::with_identity(Identity::from_seed([1u8; 32]));
    let b = Store::with_identity(Identity::from_seed([2u8; 32]));

    a.create("task", "t1", json!({"by": "a"})).await.unwrap();
    b.create("task", "t2", json!({"by": "b"})).await.unwrap();

    let (a_io, b_io) = tokio::io::duplex(64 * 1024);
    let handles = connect(&a, a_io, &b, b_io);

    let ok = wait_until(Duration::from_secs(3), || async {
        a.read("task", "t2").await.is_some() && b.read("task", "t1").await.is_some()
    })
    .await;
    for h in &handles {
        h.abort();
    }
    assert!(ok, "signed stores did not converge");
    assert_eq!(a.read("task", "t2").await, Some(json!({"by": "b"})));
    assert_eq!(b.read("task", "t1").await, Some(json!({"by": "a"})));
}
