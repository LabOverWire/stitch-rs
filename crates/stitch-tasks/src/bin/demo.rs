//! Narrated, multi-process peer-to-peer demo over a real MQDB broker and QUIC.
//!
//! Run `cargo run -p stitch-tasks --bin demo` (needs the `mqdb` binary on PATH).
//! The orchestrator starts a broker and three real peer processes — A (project
//! owner), B and C (members) — that discover each other through the broker,
//! connect over QUIC, and sync a shared task board. It then plays out a scripted
//! story: a write propagates; B drops offline and edits while C edits the same
//! task concurrently; B rejoins; the peers reconcile by HLC last-writer-wins and
//! converge. Events are printed as a single timestamped timeline as they happen.
//!
//! Each peer is this same binary re-executed as `demo peer --name <N> ...`.

use std::collections::HashMap;
use std::process::{Child as StdChild, Command as StdCommand, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mqp2p::{Peer, PeerConfig};
use stitch_p2p::membership::Role;
use stitch_p2p::{Identity, Op, Store, Swarm, SyncNode, WriteOrigin};
use stitch_tasks::{TaskBoard, Task};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};
use tokio::sync::mpsc;

const PEERS: [&str; 3] = ["A", "B", "C"];
const OWNER: &str = "A";
const PULL: Duration = Duration::from_millis(300);

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("peer") {
        run_peer(&args).await;
    } else {
        run_orchestrator().await;
    }
}

fn seed_from_name(name: &str) -> [u8; 32] {
    let mut seed = [0u8; 32];
    let bytes = name.as_bytes();
    let n = bytes.len().min(32);
    seed[..n].copy_from_slice(&bytes[..n]);
    seed
}

fn flag<'a>(args: &'a [String], key: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

fn emit(line: &str) {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

async fn disconnect(peer: Arc<Peer>, swarm: Swarm) {
    drop(swarm);
    let mut peer = peer;
    for _ in 0..40 {
        match Arc::try_unwrap(peer) {
            Ok(owned) => {
                let _ = owned.shutdown().await;
                return;
            }
            Err(shared) => {
                peer = shared;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
}

async fn link_up(name: &str, broker: &str, node: &SyncNode) -> (Arc<Peer>, Swarm) {
    let config = PeerConfig::new(name, broker)
        .with_bind_addr("127.0.0.1:0".parse().expect("bind addr"))
        .without_stun()
        .with_credentials("testuser", "testpass");
    let mut peer = Peer::new(config).await.expect("peer creation");
    peer.register().await.expect("peer registration");
    let peer = Arc::new(peer);
    let swarm = Swarm::spawn(Arc::clone(&peer), node.clone(), PULL);
    (peer, swarm)
}

fn task_detail(data: &Option<Vec<u8>>) -> String {
    match data {
        Some(bytes) => serde_json::from_slice::<Task>(bytes)
            .map(|t| format!("\"{}\" done={}", t.title, t.done))
            .unwrap_or_default(),
        None => String::new(),
    }
}

fn op_symbol(op: Op) -> &'static str {
    match op {
        Op::Insert => "+",
        Op::Update => "~",
        Op::Delete => "x",
    }
}

async fn run_peer(args: &[String]) {
    let name = flag(args, "--name").expect("--name").to_string();
    let broker = flag(args, "--broker").expect("--broker").to_string();
    let owner_id = Identity::from_seed(seed_from_name(OWNER)).peer_id();
    let identity = Identity::from_seed(seed_from_name(&name));

    let store = if name == OWNER {
        Store::with_owner(identity)
    } else {
        Store::join(identity, owner_id)
    };
    let board = TaskBoard::new(store);

    if name == OWNER {
        for member in PEERS {
            if member != OWNER {
                let id = Identity::from_seed(seed_from_name(member)).peer_id();
                board.store().invite(id, Role::Member).await;
            }
        }
    }

    let node = board.store().node().clone();
    let mut link: Option<(Arc<Peer>, Swarm)> = Some(link_up(&name, &broker, &node).await);

    let mut events = board.store().subscribe().await;
    let watch_name = name.clone();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            if event.entity != stitch_tasks::ENTITY {
                continue;
            }
            let detail = task_detail(&event.data);
            let line = match event.origin {
                WriteOrigin::Local => {
                    format!("EVT {watch_name}  {} {}  {detail}", op_symbol(event.op), event.id)
                }
                WriteOrigin::Remote => {
                    format!("EVT {watch_name}  <- {}  {detail}  (synced from peer)", event.id)
                }
            };
            emit(line.trim_end());
        }
    });

    emit(&format!("READY {name}"));

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let mut parts = line.splitn(3, ' ');
        let cmd = parts.next().unwrap_or("");
        let id = parts.next().unwrap_or("");
        let rest = parts.next().unwrap_or("");
        match cmd {
            "add" => board.add(id, rest).await,
            "rename" => board.rename(id, rest).await,
            "done" => board.set_done(id, rest == "true").await,
            "remove" => board.remove(id).await,
            "offline" => {
                if let Some((peer, swarm)) = link.take() {
                    disconnect(peer, swarm).await;
                }
            }
            "online" if link.is_none() => {
                link = Some(link_up(&name, &broker, &node).await);
            }
            "snapshot" => {
                let snap = board.snapshot().await;
                let json = serde_json::to_string(&snap).unwrap_or_default();
                emit(&format!("SNAP {name} {json}"));
            }
            "quit" => break,
            _ => {}
        }
    }
}

enum Ctl {
    Ready(String),
    Snap(String, String),
}

struct PeerProc {
    name: &'static str,
    child: tokio::process::Child,
    stdin: ChildStdin,
}

impl PeerProc {
    async fn send(&mut self, command: &str) {
        let _ = self.stdin.write_all(command.as_bytes()).await;
        let _ = self.stdin.write_all(b"\n").await;
        let _ = self.stdin.flush().await;
    }
}

struct BrokerGuard(StdChild);

impl Drop for BrokerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn mqdb_available() -> bool {
    StdCommand::new("mqdb")
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn pick_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

async fn wait_for_port(port: u16) -> bool {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

async fn collect_snapshots(
    peers: &mut [PeerProc],
    ctl: &mut mpsc::UnboundedReceiver<Ctl>,
) -> Option<HashMap<String, String>> {
    for peer in peers.iter_mut() {
        peer.send("snapshot").await;
    }
    let mut snaps = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(3);
    while snaps.len() < peers.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match tokio::time::timeout(remaining, ctl.recv()).await {
            Ok(Some(Ctl::Snap(name, json))) => {
                snaps.insert(name, json);
            }
            Ok(Some(Ctl::Ready(_))) => {}
            _ => return None,
        }
    }
    Some(snaps)
}

async fn await_convergence(
    peers: &mut [PeerProc],
    ctl: &mut mpsc::UnboundedReceiver<Ctl>,
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(snaps) = collect_snapshots(peers, ctl).await {
            let mut values = snaps.values();
            if let Some(first) = values.next()
                && values.all(|v| v == first)
            {
                return Some(first.clone());
            }
        }
        tokio::time::sleep(Duration::from_millis(600)).await;
    }
    None
}

async fn run_orchestrator() {
    if !mqdb_available() {
        eprintln!("skipping demo: `mqdb` binary not found on PATH");
        return;
    }

    let work = std::env::temp_dir().join(format!("stitch-demo-{}", std::process::id()));
    let db_dir = work.join("db");
    std::fs::create_dir_all(&db_dir).expect("create work dir");

    let passwd_out = StdCommand::new("mqdb")
        .args(["passwd", "testuser", "-b", "testpass", "-n"])
        .output()
        .expect("run mqdb passwd");
    assert!(passwd_out.status.success(), "mqdb passwd failed");
    let passwd_file = work.join("passwd.txt");
    std::fs::write(&passwd_file, &passwd_out.stdout).expect("write passwd");

    let port = pick_free_port();
    let broker_child = StdCommand::new("mqdb")
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
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn broker");
    let _broker = BrokerGuard(broker_child);

    if !wait_for_port(port).await {
        eprintln!("broker did not start on {port}");
        return;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let broker_addr = format!("127.0.0.1:{port}");

    let exe = std::env::current_exe().expect("current exe");
    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<String>();
    let (ctl_tx, mut ctl_rx) = mpsc::unbounded_channel::<Ctl>();

    let start = Instant::now();
    tokio::spawn(async move {
        while let Some(line) = ev_rx.recv().await {
            println!("[{:>6.1}s] {line}", start.elapsed().as_secs_f64());
            use std::io::Write;
            let _ = std::io::stdout().flush();
        }
    });

    let mut peers = Vec::new();
    for name in PEERS {
        let mut child = Command::new(&exe)
            .args(["peer", "--name", name, "--broker", &broker_addr])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn peer");
        let stdout = child.stdout.take().expect("peer stdout");
        let stdin = child.stdin.take().expect("peer stdin");
        let ev = ev_tx.clone();
        let ctl = ctl_tx.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(rest) = line.strip_prefix("READY ") {
                    let _ = ctl.send(Ctl::Ready(rest.to_string()));
                } else if let Some(rest) = line.strip_prefix("SNAP ") {
                    if let Some((peer, json)) = rest.split_once(' ') {
                        let _ = ctl.send(Ctl::Snap(peer.to_string(), json.to_string()));
                    }
                } else if let Some(rest) = line.strip_prefix("EVT ") {
                    let _ = ev.send(rest.to_string());
                }
            }
        });
        peers.push(PeerProc { name, child, stdin });
    }

    let mut ready = 0;
    let ready_deadline = Instant::now() + Duration::from_secs(20);
    while ready < peers.len() {
        let remaining = ready_deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, ctl_rx.recv()).await {
            Ok(Some(Ctl::Ready(name))) => {
                ready += 1;
                let _ = ev_tx.send(format!("· {name} ready"));
            }
            _ => break,
        }
    }
    if ready < peers.len() {
        eprintln!("peers did not all come up");
        return;
    }

    let _ = ev_tx.send("== peers up; discovering over broker, connecting via QUIC ==".into());
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let _ = ev_tx.send("-- A creates t1 --".into());
    peer_mut(&mut peers, "A").send("add t1 ship release").await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let _ = ev_tx.send("-- partition: B drops offline --".into());
    peer_mut(&mut peers, "B").send("offline").await;
    tokio::time::sleep(Duration::from_millis(800)).await;

    let _ = ev_tx.send("-- B edits t1 offline; meanwhile C edits t1 and adds t2 --".into());
    peer_mut(&mut peers, "B").send("done t1 true").await;
    peer_mut(&mut peers, "C").send("rename t1 ship v2").await;
    peer_mut(&mut peers, "C").send("add t2 write changelog").await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let _ = ev_tx.send("-- heal: B comes back online and re-syncs --".into());
    peer_mut(&mut peers, "B").send("online").await;

    let result = await_convergence(&mut peers, &mut ctl_rx, Duration::from_secs(25)).await;
    match &result {
        Some(state) => {
            let _ = ev_tx.send(format!("== converged: all peers agree on {state} =="));
        }
        None => {
            let _ = ev_tx.send("== DID NOT CONVERGE ==".into());
        }
    }

    for peer in &mut peers {
        peer.send("quit").await;
    }
    for peer in &mut peers {
        let _ = peer.child.wait().await;
    }
    drop(ev_tx);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = std::fs::remove_dir_all(&work);

    if result.is_none() {
        std::process::exit(1);
    }
}

fn peer_mut<'a>(peers: &'a mut [PeerProc], name: &str) -> &'a mut PeerProc {
    peers
        .iter_mut()
        .find(|p| p.name == name)
        .expect("known peer")
}
