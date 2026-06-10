use mqp2p::{Peer, PeerConfig, TransferProgress};
use rand::RngExt;
use ring::digest;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::Duration;

struct BrokerGuard(Child);

impl Drop for BrokerGuard {
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
        .expect("failed to start mqdb broker");
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
    panic!("broker did not start on port {port} within 10s");
}

fn create_passwd_file(dir: &std::path::Path) -> std::path::PathBuf {
    let passwd_path = dir.join("passwd.txt");
    let output = Command::new("mqdb")
        .args(["passwd", "testuser", "-b", "testpass", "-n"])
        .output()
        .expect("failed to run mqdb passwd");
    assert!(output.status.success(), "mqdb passwd failed");
    std::fs::write(&passwd_path, &output.stdout).expect("failed to write passwd file");
    passwd_path
}

fn sha256_file(path: &std::path::Path) -> String {
    let data = std::fs::read(path).expect("failed to read file for hashing");
    let d = digest::digest(&digest::SHA256, &data);
    d.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test]
async fn test_full_file_transfer() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();

    let tmpdir = tempfile::tempdir().expect("failed to create tmpdir");
    let db_dir = tmpdir.path().join("db");
    std::fs::create_dir_all(&db_dir).expect("failed to create db dir");

    let passwd_file = create_passwd_file(tmpdir.path());

    let broker_port = pick_free_port();
    let _broker = start_broker(broker_port, &db_dir, &passwd_file);
    wait_for_port(broker_port);

    let broker_addr = format!("127.0.0.1:{broker_port}");
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let config_a = PeerConfig::new("sender", &broker_addr)
        .with_bind_addr(bind_addr)
        .without_stun()
        .with_credentials("testuser", "testpass");
    let mut peer_a = Peer::new(config_a).await.expect("peer A creation failed");
    let _id_a = peer_a.register().await.expect("peer A registration failed");

    let config_b = PeerConfig::new("receiver", &broker_addr)
        .with_bind_addr(bind_addr)
        .without_stun()
        .with_credentials("testuser", "testpass");
    let mut peer_b = Peer::new(config_b).await.expect("peer B creation failed");
    let id_b = peer_b.register().await.expect("peer B registration failed");

    let peers = peer_a
        .discover_peers()
        .await
        .expect("peer discovery failed");
    let target = peers
        .iter()
        .find(|p| p.id == id_b)
        .expect("receiver not found in peer list");

    let recv_dir = tmpdir.path().join("received");
    std::fs::create_dir_all(&recv_dir).expect("failed to create recv dir");

    let accept_handle = tokio::spawn(async move {
        let conn_b = peer_b
            .accept_connection()
            .await
            .expect("accept_connection failed");

        let result = conn_b
            .receive_file(&recv_dir, |_offer| true, |_progress: TransferProgress| {})
            .await
            .expect("receive_file failed");

        tokio::time::sleep(Duration::from_millis(200)).await;
        conn_b.close().expect("close conn_b failed");
        peer_b.shutdown().await.expect("peer B shutdown failed");
        (result, recv_dir)
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    let conn_a = peer_a.connect_to(target).await.expect("connect_to failed");

    let send_file_path = tmpdir.path().join("testfile.bin");
    let file_size = 1024 * 1024;
    {
        let mut f = std::fs::File::create(&send_file_path).expect("failed to create test file");
        let mut rng = rand::rng();
        let mut buf = [0u8; 8192];
        let mut remaining = file_size;
        while remaining > 0 {
            let chunk = remaining.min(buf.len());
            rng.fill(&mut buf[..chunk]);
            f.write_all(&buf[..chunk])
                .expect("failed to write test data");
            remaining -= chunk;
        }
    }

    let send_result = conn_a
        .send_file(&send_file_path, |_progress: TransferProgress| {})
        .await
        .expect("send_file failed");

    conn_a.close().expect("close conn_a failed");

    let (recv_result, recv_dir) = accept_handle.await.expect("receiver task panicked");

    let received_path = recv_dir.join(&send_result.file_name);
    assert!(received_path.exists(), "received file does not exist");

    let received_meta = std::fs::metadata(&received_path).expect("failed to stat received file");
    assert_eq!(
        received_meta.len(),
        file_size as u64,
        "received file size mismatch"
    );

    let send_hash = sha256_file(&send_file_path);
    let recv_hash = sha256_file(&received_path);
    assert_eq!(send_hash, recv_hash, "SHA-256 hash mismatch");
    assert_eq!(send_result.sha256, send_hash);
    assert_eq!(recv_result.sha256, recv_hash);

    peer_a.shutdown().await.expect("peer A shutdown failed");
}
