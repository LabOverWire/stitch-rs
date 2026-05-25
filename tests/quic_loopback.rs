//! Proves the sync session works over a real QUIC connection (not just an
//! in-memory pipe), using mqp2p's `QuicEndpoint` for cert/fingerprint mTLS.
//! No broker and no STUN — peers connect directly over localhost, which is all
//! the transport proof needs. Discovery (broker) and NAT traversal (STUN) are
//! orthogonal mqp2p concerns layered on top in production.

use mqp2p::quic::{QuicEndpoint, generate_self_signed_cert};
use std::net::UdpSocket;
use std::sync::Arc;
use std::time::Duration;
use stitch_p2p::{Op, PEER_ID_LEN, PeerId, SyncState, session};
use tokio::sync::{Mutex, mpsc};

fn peer_id(n: u8) -> PeerId {
    let mut id = [0u8; PEER_ID_LEN];
    id[0] = n;
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_peers_converge_over_real_quic() {
    let id_a = generate_self_signed_cert().unwrap();
    let id_b = generate_self_signed_cert().unwrap();
    let fp_a = id_a.fingerprint.clone();
    let fp_b = id_b.fingerprint.clone();

    let ep_a = QuicEndpoint::bind(UdpSocket::bind("127.0.0.1:0").unwrap(), id_a).unwrap();
    let ep_b = QuicEndpoint::bind(UdpSocket::bind("127.0.0.1:0").unwrap(), id_b).unwrap();
    let addr_b = ep_b.local_addr().unwrap();

    let a_state = Arc::new(Mutex::new(SyncState::new(peer_id(1))));
    let b_state = Arc::new(Mutex::new(SyncState::new(peer_id(2))));

    a_state
        .lock()
        .await
        .local_write(10, Op::Insert, "task", "t1", b"from-a".to_vec());
    b_state
        .lock()
        .await
        .local_write(11, Op::Insert, "task", "t2", b"from-b".to_vec());

    // Acceptor B: accept connection (verifying A's fingerprint), then the bidi
    // stream A opens once it writes its Hello.
    let b_run_state = Arc::clone(&b_state);
    let b_task = tokio::spawn(async move {
        let conn = ep_b.accept_with_fingerprint(&fp_a).await.unwrap();
        let (send, recv) = conn.accept_bi().await.unwrap();
        let (_tx, rx) = mpsc::unbounded_channel();
        let _ = session::run(b_run_state, recv, send, rx, Duration::from_millis(50)).await;
    });

    // Connector A.
    let conn = ep_a.connect(addr_b, &fp_b).await.unwrap();
    let (send, recv) = conn.open_bi().await.unwrap();
    let a_run_state = Arc::clone(&a_state);
    let a_task = tokio::spawn(async move {
        let (_tx, rx) = mpsc::unbounded_channel();
        let _ = session::run(a_run_state, recv, send, rx, Duration::from_millis(50)).await;
    });

    let converged = wait_until(Duration::from_secs(5), || async {
        let a = a_state.lock().await;
        let b = b_state.lock().await;
        a.visible("task", "t1").is_some()
            && a.visible("task", "t2").is_some()
            && b.visible("task", "t1").is_some()
            && b.visible("task", "t2").is_some()
    })
    .await;

    a_task.abort();
    b_task.abort();
    ep_a.close();

    assert!(converged, "peers did not converge over QUIC within timeout");
    let a = a_state.lock().await;
    let b = b_state.lock().await;
    assert_eq!(a.visible("task", "t2"), Some(&b"from-b"[..]));
    assert_eq!(b.visible("task", "t1"), Some(&b"from-a"[..]));
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
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}
