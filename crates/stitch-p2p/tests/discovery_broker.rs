#![cfg(feature = "discovery")]
//! End-to-end discovery wiring over a real MQDB broker: two peers register,
//! discover each other, NAT-traverse to a direct QUIC connection, and run the
//! sync protocol to convergence — all driven by `Swarm`. Requires the `mqdb`
//! binary on PATH (the test skips with a message if absent).

use mqp2p::{Peer, PeerConfig};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;
use stitch_p2p::{Op, Swarm, SyncNode, peer_id_from_fingerprint};

struct BrokerGuard(Child);

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn mqdb_available() -> bool {
    Command::new("mqdb")
        .arg("--help")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn pick_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn create_passwd_file(dir: &std::path::Path) -> std::path::PathBuf {
    let passwd_path = dir.join("passwd.txt");
    let output = Command::new("mqdb")
        .args(["passwd", "testuser", "-b", "testpass", "-n"])
        .output()
        .expect("run mqdb passwd");
    assert!(output.status.success(), "mqdb passwd failed");
    std::fs::write(&passwd_path, &output.stdout).expect("write passwd file");
    passwd_path
}

fn start_broker(port: u16, db_dir: &std::path::Path, passwd_file: &std::path::Path) -> BrokerGuard {
    let child = Command::new("mqdb")
        .args([
            "agent",
            "start",
            "--bind",
            &format!("127.0.0.1:{port}"),
            "--db",
            &db_dir.to_string_lossy(),
            "--passwd",
            &passwd_file.to_string_lossy(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn mqdb broker");
    BrokerGuard(child)
}

fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..100 {
        if TcpStream::connect(&addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("broker did not start on {port} within 10s");
}

async fn make_peer(name: &str, broker: &str) -> (Arc<Peer>, SyncNode) {
    let config = PeerConfig::new(name, broker)
        .with_bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .without_stun()
        .with_credentials("testuser", "testpass");
    let mut peer = Peer::new(config).await.expect("peer creation");
    peer.register().await.expect("peer registration");
    let node = SyncNode::new(
        peer_id_from_fingerprint(peer.fingerprint()).expect("fingerprint -> peer id"),
    );
    (Arc::new(peer), node)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_peers_discover_and_sync_over_broker() {
    if !mqdb_available() {
        eprintln!("skipping: `mqdb` binary not found on PATH");
        return;
    }

    let tmp = tempfile::tempdir().expect("tmpdir");
    let db_dir = tmp.path().join("db");
    std::fs::create_dir_all(&db_dir).expect("db dir");
    let passwd = create_passwd_file(tmp.path());
    let port = pick_free_port();
    let _broker = start_broker(port, &db_dir, &passwd);
    wait_for_port(port);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr = format!("127.0.0.1:{port}");

    let (peer_a, node_a) = make_peer("alice", &broker_addr).await;
    let (peer_b, node_b) = make_peer("bob", &broker_addr).await;

    let pull = Duration::from_millis(500);
    let swarm_a = Swarm::spawn(peer_a, node_a.clone(), pull);
    let swarm_b = Swarm::spawn(peer_b, node_b.clone(), pull);

    node_a
        .local_write(Op::Insert, "task", "t1", b"from-alice".to_vec())
        .await;
    node_b
        .local_write(Op::Insert, "task", "t2", b"from-bob".to_vec())
        .await;

    let converged = wait_until(Duration::from_secs(30), || async {
        node_a.visible("task", "t2").await.is_some() && node_b.visible("task", "t1").await.is_some()
    })
    .await;

    swarm_a.abort();
    swarm_b.abort();

    assert!(converged, "peers did not converge over broker within 30s");
    assert_eq!(
        node_a.visible("task", "t2").await,
        Some(b"from-bob".to_vec())
    );
    assert_eq!(
        node_b.visible("task", "t1").await,
        Some(b"from-alice".to_vec())
    );
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}
