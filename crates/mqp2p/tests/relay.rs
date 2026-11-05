use mqp2p::quic::{QuicEndpoint, generate_self_signed_cert};
use mqp2p::turn_client::{TurnClient, TurnConfig};
use mqp2p::{Peer, PeerConfig};
use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use zeroize::Zeroizing;

const REALM: &str = "stitch.test";
const TURN_USER: &str = "turnuser";
const TURN_PASS: &str = "turnpass";

struct TurnServerGuard(Child);

impl Drop for TurnServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn pick_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("failed to bind ephemeral port")
        .local_addr()
        .expect("failed to get local addr")
        .port()
}

fn start_turn_server(port: u16) -> Option<TurnServerGuard> {
    Command::new("turnserver")
        .args([
            "-n",
            "--log-file=stdout",
            "--listening-ip=127.0.0.1",
            &format!("--listening-port={port}"),
            "--relay-ip=127.0.0.1",
            &format!("--realm={REALM}"),
            "--lt-cred-mech",
            &format!("--user={TURN_USER}:{TURN_PASS}"),
            "--no-tls",
            "--no-cli",
            "--allow-loopback-peers",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
        .map(TurnServerGuard)
}

fn turn_config(server: SocketAddr) -> TurnConfig {
    TurnConfig {
        server,
        username: TURN_USER.to_string(),
        password: Zeroizing::new(TURN_PASS.to_string()),
        lifetime_secs: 600,
    }
}

async fn allocate_with_retry(server: SocketAddr) -> TurnClient {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match TurnClient::connect(turn_config(server)).await {
            Ok(client) => return client,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("turn allocation never succeeded: {e}");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relayed_quic_handshake_transfers_data_over_coturn() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("warn")
        .with_test_writer()
        .try_init();

    let turn_port = pick_free_port();
    let Some(_turn) = start_turn_server(turn_port) else {
        eprintln!("skipping relay test: coturn `turnserver` not found on PATH");
        return;
    };
    let server: SocketAddr = format!("127.0.0.1:{turn_port}").parse().unwrap();

    let client_a = allocate_with_retry(server).await;
    let client_b = allocate_with_retry(server).await;
    let relayed_a = client_a.relayed_addr();
    let relayed_b = client_b.relayed_addr();

    let socket_a = client_a
        .relay_socket(relayed_b)
        .await
        .expect("bind channel a->b");
    let socket_b = client_b
        .relay_socket(relayed_a)
        .await
        .expect("bind channel b->a");

    let id_a = generate_self_signed_cert().expect("cert a");
    let id_b = generate_self_signed_cert().expect("cert b");
    let fp_a = id_a.fingerprint.clone();
    let fp_b = id_b.fingerprint.clone();
    let transport = Arc::new(quinn::TransportConfig::default());

    let ep_a = QuicEndpoint::bind_abstract(socket_a, id_a, Arc::clone(&transport)).expect("ep a");
    let ep_b = QuicEndpoint::bind_abstract(socket_b, id_b, transport).expect("ep b");

    let payload = vec![0x5a_u8; 64 * 1024];
    let expected = payload.clone();

    let receiver = tokio::spawn(async move {
        let conn = ep_b.accept_with_fingerprint(&fp_a).await.expect("accept");
        let mut recv = conn.accept_uni().await.expect("stream");
        let received = recv.read_to_end(1024 * 1024).await.expect("read");
        conn.close(0u32.into(), b"done");
        received
    });

    let conn = tokio::time::timeout(Duration::from_secs(15), ep_a.connect(relayed_b, &fp_b))
        .await
        .expect("relay handshake timed out")
        .expect("connect");
    let mut send = conn.open_uni().await.expect("open");
    send.write_all(&payload).await.expect("write");
    send.finish().expect("finish");

    let received = tokio::time::timeout(Duration::from_secs(15), receiver)
        .await
        .expect("receiver timed out")
        .expect("receiver task");
    assert_eq!(received, expected, "relayed payload mismatch");

    conn.close(0u32.into(), b"done");
}

struct BrokerGuard(Child);

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_broker(port: u16, dir: &std::path::Path) -> Option<BrokerGuard> {
    let passwd = dir.join("passwd");
    let output = Command::new("mqdb")
        .args(["passwd", "peer", "-b", "peer", "-n"])
        .output()
        .ok()?;
    assert!(output.status.success(), "mqdb passwd failed");
    std::fs::write(&passwd, &output.stdout).expect("write passwd");
    Command::new("mqdb")
        .args(["agent", "start", "--bind", &format!("127.0.0.1:{port}")])
        .args(["--db", &dir.join("db").to_string_lossy()])
        .args(["--passwd", &passwd.to_string_lossy()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
        .map(BrokerGuard)
}

async fn registered_peer(name: &str, broker: &str, turn: &str) -> Peer {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let config = PeerConfig::new(name, broker)
            .without_stun()
            .with_credentials("peer", "peer")
            .with_turn(turn, TURN_USER, TURN_PASS);
        let registered = match Peer::new(config).await {
            Ok(mut peer) => peer.register().await.map(|_| peer),
            Err(e) => Err(e),
        };
        match registered {
            Ok(peer) => return peer,
            Err(e) if tokio::time::Instant::now() >= deadline => {
                panic!("peer never registered: {e}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_responder_with_turn_accepts_one_initiator_after_another_without_delay() {
    let turn_port = pick_free_port();
    let Some(_turn) = start_turn_server(turn_port) else {
        eprintln!("skipping relay test: coturn `turnserver` not found on PATH");
        return;
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let broker_port = pick_free_port();
    let Some(_broker) = start_broker(broker_port, dir.path()) else {
        eprintln!("skipping relay test: `mqdb` not found on PATH");
        return;
    };
    let broker = format!("127.0.0.1:{broker_port}");
    let turn = format!("127.0.0.1:{turn_port}");

    let responder = Arc::new(registered_peer("responder", &broker, &turn).await);
    for name in ["first", "second"] {
        let initiator = registered_peer(name, &broker, &turn).await;
        let peers = initiator.discover_peers().await.expect("discover");
        let target = peers
            .iter()
            .find(|peer| peer.name == "responder")
            .expect("responder registered")
            .clone();

        let accepting = Arc::clone(&responder);
        let accept = tokio::spawn(async move { accepting.accept_connection().await });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let started = tokio::time::Instant::now();
        let connected = initiator.connect_to(&target).await.expect("connect");
        let accepted = tokio::time::timeout(Duration::from_secs(30), accept)
            .await
            .expect("accept timed out")
            .expect("accept task")
            .expect("accept");
        let elapsed = started.elapsed();

        assert!(!connected.is_relayed(), "{name} should connect directly");
        assert!(!accepted.is_relayed(), "{name} should be accepted directly");
        assert!(
            elapsed < Duration::from_secs(5),
            "{name} took {elapsed:?} to connect and be accepted"
        );
    }
}
