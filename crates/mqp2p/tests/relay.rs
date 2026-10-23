use mqp2p::quic::{QuicEndpoint, generate_self_signed_cert};
use mqp2p::turn_client::{TurnClient, TurnConfig};
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
