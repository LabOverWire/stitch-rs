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

#[cfg(all(feature = "membership", feature = "store"))]
use stitch_p2p::membership::Role;
#[cfg(all(feature = "membership", feature = "store"))]
use stitch_p2p::{Identity, MemberGate, Store};

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

async fn make_peer(name: &str, broker: &str, seed: Option<[u8; 32]>) -> (Arc<Peer>, SyncNode) {
    let mut config = PeerConfig::new(name, broker)
        .with_bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .without_stun()
        .with_credentials("testuser", "testpass");
    if let Some(seed) = seed {
        config = config.with_ed25519_identity(seed);
    }
    let mut peer = Peer::new(config).await.expect("peer creation");
    peer.register().await.expect("peer registration");
    let node = SyncNode::new(
        peer_id_from_fingerprint(peer.fingerprint()).expect("fingerprint -> peer id"),
    );
    (Arc::new(peer), node)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_peers_discover_and_sync_over_broker() {
    let pull = Duration::from_millis(500);
    discover_and_sync(|peer, node| Swarm::spawn(peer, node, pull)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_polls_on_its_own_interval() {
    let pull = Duration::from_millis(500);
    let discovery = Duration::from_secs(3);
    discover_and_sync(|peer, node| {
        Swarm::spawn_with_discovery_interval(peer, node, pull, discovery)
    })
    .await;
}

async fn discover_and_sync(spawn: impl Fn(Arc<Peer>, SyncNode) -> Swarm) {
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

    let (peer_a, node_a) = make_peer("alice", &broker_addr, None).await;
    let (peer_b, node_b) = make_peer("bob", &broker_addr, None).await;

    let swarm_a = spawn(peer_a, node_a.clone());
    let swarm_b = spawn(peer_b, node_b.clone());

    node_a
        .local_write(Op::Insert, "task", "t1", b"from-alice".to_vec())
        .await
        .expect("write");
    node_b
        .local_write(Op::Insert, "task", "t2", b"from-bob".to_vec())
        .await
        .expect("write");

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

#[cfg(all(feature = "membership", feature = "store"))]
async fn start_test_broker() -> (BrokerGuard, String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let db_dir = tmp.path().join("db");
    std::fs::create_dir_all(&db_dir).expect("db dir");
    let passwd = create_passwd_file(tmp.path());
    let port = pick_free_port();
    let broker = start_broker(port, &db_dir, &passwd);
    wait_for_port(port);
    tokio::time::sleep(Duration::from_secs(1)).await;
    (broker, format!("127.0.0.1:{port}"), tmp)
}

#[cfg(all(feature = "membership", feature = "store"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gated_swarm_admits_an_invited_member() {
    if !mqdb_available() {
        eprintln!("skipping: `mqdb` binary not found on PATH");
        return;
    }
    let (_broker, broker_addr, _tmp) = start_test_broker().await;

    let owner_id = Identity::from_seed([20u8; 32]);
    let owner_pid = owner_id.peer_id();
    let owner_seed = owner_id.seed();
    let member_id = Identity::from_seed([21u8; 32]);
    let member_pid = member_id.peer_id();
    let member_seed = member_id.seed();

    let owner = Store::with_owner(owner_id);
    let member = Store::join(member_id, owner_pid);
    owner
        .invite(member_pid, Role::Member)
        .await
        .expect("invite");

    let (owner_peer, _) = make_peer("gated-owner", &broker_addr, Some(owner_seed)).await;
    let (member_peer, _) = make_peer("gated-member", &broker_addr, Some(member_seed)).await;

    let pull = Duration::from_millis(300);
    let gate: Arc<dyn MemberGate> = Arc::new(owner.membership().expect("owner membership"));
    let swarm_owner = Swarm::spawn_gated(owner_peer, owner.node().clone(), pull, gate);
    let swarm_member = Swarm::spawn(member_peer, member.node().clone(), pull);

    member
        .create("task", "m", serde_json::json!({"by": "member"}))
        .await
        .expect("member write");

    let admitted = wait_until(Duration::from_secs(30), || async {
        owner.read("task", "m").await.is_some()
    })
    .await;

    swarm_owner.abort();
    swarm_member.abort();

    assert!(
        admitted,
        "a gated swarm did not admit and sync an invited member"
    );
    assert_eq!(
        owner.read("task", "m").await,
        Some(serde_json::json!({"by": "member"}))
    );
}

#[cfg(all(feature = "membership", feature = "store"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gated_swarm_rejects_an_uninvited_outsider() {
    if !mqdb_available() {
        eprintln!("skipping: `mqdb` binary not found on PATH");
        return;
    }
    let (_broker, broker_addr, _tmp) = start_test_broker().await;

    let owner_id = Identity::from_seed([22u8; 32]);
    let owner_pid = owner_id.peer_id();
    let owner_seed = owner_id.seed();
    let outsider_id = Identity::from_seed([23u8; 32]);
    let outsider_seed = outsider_id.seed();

    let owner = Store::with_owner(owner_id);
    let outsider = Store::join(outsider_id, owner_pid);

    let (owner_peer, _) = make_peer("gated-owner", &broker_addr, Some(owner_seed)).await;
    let (outsider_peer, _) = make_peer("gated-outsider", &broker_addr, Some(outsider_seed)).await;

    let pull = Duration::from_millis(300);
    let gate: Arc<dyn MemberGate> = Arc::new(owner.membership().expect("owner membership"));
    let swarm_owner = Swarm::spawn_gated(owner_peer, owner.node().clone(), pull, gate);
    let swarm_outsider = Swarm::spawn(outsider_peer, outsider.node().clone(), pull);

    outsider
        .create("task", "o", serde_json::json!({"by": "outsider"}))
        .await
        .expect("outsider write");

    let leaked = wait_until(Duration::from_secs(10), || async {
        owner.node().visible("task", "o").await.is_some()
    })
    .await;

    swarm_owner.abort();
    swarm_outsider.abort();

    assert!(
        !leaked,
        "a gated swarm admitted an uninvited outsider; its record reached the owner"
    );
}
