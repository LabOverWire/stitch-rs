//! Multi-peer convergence through the fan-out node, over in-memory pipes.
//! A node with two sessions (the hub B in a line A—B—C) propagates each end's
//! writes to the other purely through its shared `SyncState` + periodic pull —
//! the executable analog of `spec/StitchP2PTransitive.tla`, now over the real
//! session protocol.

use std::time::Duration;
use stitch_p2p::{Op, PEER_ID_LEN, PeerId, SyncNode, session};
use tokio::io::{DuplexStream, split};
use tokio::task::JoinHandle;

fn peer(n: u8) -> PeerId {
    let mut id = [0u8; PEER_ID_LEN];
    id[0] = n;
    id
}

const PULL: Duration = Duration::from_millis(20);

fn link(x: &SyncNode, x_io: DuplexStream, y: &SyncNode, y_io: DuplexStream) -> Vec<JoinHandle<()>> {
    let rx_x = x.register_session();
    let (xr, xw) = split(x_io);
    let sx = x.state();
    let rx_y = y.register_session();
    let (yr, yw) = split(y_io);
    let sy = y.state();
    vec![
        tokio::spawn(async move {
            let _ = session::run(sx, xr, xw, rx_x, PULL).await;
        }),
        tokio::spawn(async move {
            let _ = session::run(sy, yr, yw, rx_y, PULL).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn line_topology_converges_through_hub() {
    let a = SyncNode::new(peer(1));
    let b = SyncNode::new(peer(2));
    let c = SyncNode::new(peer(3));

    let (ab_a, ab_b) = tokio::io::duplex(64 * 1024);
    let (bc_b, bc_c) = tokio::io::duplex(64 * 1024);
    let mut handles = link(&a, ab_a, &b, ab_b);
    handles.extend(link(&b, bc_b, &c, bc_c));
    assert_eq!(b.session_count(), 2, "hub B should hold two sessions");

    a.local_write(Op::Insert, "task", "t1", b"from-a".to_vec()).await;
    b.local_write(Op::Insert, "task", "t2", b"from-b".to_vec()).await;
    c.local_write(Op::Insert, "task", "t3", b"from-c".to_vec()).await;

    let converged = wait_until(Duration::from_secs(5), || async {
        a.visible("task", "t3").await.is_some() && c.visible("task", "t1").await.is_some()
    })
    .await;

    for h in handles {
        h.abort();
    }
    assert!(converged, "line topology did not converge through the hub");

    // A and C exchanged data only through B.
    assert_eq!(a.visible("task", "t3").await, Some(b"from-c".to_vec()));
    assert_eq!(c.visible("task", "t1").await, Some(b"from-a".to_vec()));
    for node in [&a, &b, &c] {
        assert_eq!(node.visible("task", "t1").await, Some(b"from-a".to_vec()));
        assert_eq!(node.visible("task", "t2").await, Some(b"from-b".to_vec()));
        assert_eq!(node.visible("task", "t3").await, Some(b"from-c".to_vec()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_after_connect_reaches_far_end() {
    let a = SyncNode::new(peer(1));
    let b = SyncNode::new(peer(2));
    let c = SyncNode::new(peer(3));

    let (ab_a, ab_b) = tokio::io::duplex(64 * 1024);
    let (bc_b, bc_c) = tokio::io::duplex(64 * 1024);
    let mut handles = link(&a, ab_a, &b, ab_b);
    handles.extend(link(&b, bc_b, &c, bc_c));

    // Write at A only after the mesh is live; it must reach the far end C.
    a.local_write(Op::Insert, "task", "late", b"v".to_vec()).await;

    let reached = wait_until(Duration::from_secs(5), || async {
        c.visible("task", "late").await.is_some()
    })
    .await;

    for h in handles {
        h.abort();
    }
    assert!(reached, "post-connect write did not reach the far end");
}
