use crate::hlc::{PEER_ID_LEN, PeerId};
use crate::node::SyncNode;
use crate::session;
use mqp2p::{P2pConnection, Peer};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;

/// Derive a sync [`PeerId`] (the 32-byte writer identity used in HLC stamps)
/// from an mqp2p certificate fingerprint of the form `sha256:aa:bb:...`. Ties
/// the sync identity to the cryptographic identity. Returns `None` if the
/// fingerprint isn't 32 colon-separated hex bytes after the `sha256:` prefix.
#[must_use]
pub fn peer_id_from_fingerprint(fingerprint: &str) -> Option<PeerId> {
    let hex = fingerprint.strip_prefix("sha256:")?;
    let mut out = [0u8; PEER_ID_LEN];
    let mut count = 0;
    for byte_hex in hex.split(':') {
        if count >= PEER_ID_LEN {
            return None;
        }
        out[count] = u8::from_str_radix(byte_hex, 16).ok()?;
        count += 1;
    }
    if count == PEER_ID_LEN {
        Some(out)
    } else {
        None
    }
}

#[derive(Clone, Copy)]
enum Role {
    Initiator,
    Responder,
}

/// Drives a [`SyncNode`] from an mqp2p [`Peer`]: an accept loop for inbound
/// connections and a connect loop that dials discovered peers. Connection
/// roles are broken by mqp2p peer-id order — the lexicographically larger id
/// dials, the smaller accepts — so each pair forms exactly one connection.
/// Each established connection opens a sync stream and runs [`session::run`]
/// against the node's shared state.
pub struct Swarm {
    handles: Vec<JoinHandle<()>>,
    bridges: Bridges,
}

/// Live per-connection session tasks, tracked so [`Swarm::abort`] can tear them
/// down (dropping their connections) rather than leaving them detached. Finished
/// handles are pruned on insert, so the vector stays bounded by live connections.
type Bridges = Arc<Mutex<Vec<JoinHandle<()>>>>;

fn track(bridges: &Bridges, handle: JoinHandle<()>) {
    let mut guard = bridges.lock().expect("bridges lock");
    guard.retain(|h| !h.is_finished());
    guard.push(handle);
}

impl Swarm {
    #[must_use]
    pub fn spawn(peer: Arc<Peer>, node: SyncNode, pull_interval: Duration) -> Self {
        let connected: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let bridges: Bridges = Arc::new(Mutex::new(Vec::new()));
        let accept = tokio::spawn(accept_loop(
            Arc::clone(&peer),
            node.clone(),
            Arc::clone(&connected),
            Arc::clone(&bridges),
            pull_interval,
        ));
        let connect = tokio::spawn(connect_loop(
            peer,
            node,
            connected,
            Arc::clone(&bridges),
            pull_interval,
        ));
        Self {
            handles: vec![accept, connect],
            bridges,
        }
    }

    /// Stop discovery and tear down every live session, closing their
    /// connections. After this the peer no longer syncs until a new `Swarm` is
    /// spawned.
    pub fn abort(&self) {
        for handle in &self.handles {
            handle.abort();
        }
        for handle in self.bridges.lock().expect("bridges lock").drain(..) {
            handle.abort();
        }
    }
}

impl Drop for Swarm {
    fn drop(&mut self) {
        self.abort();
    }
}

async fn accept_loop(
    peer: Arc<Peer>,
    node: SyncNode,
    connected: Arc<Mutex<HashSet<String>>>,
    bridges: Bridges,
    pull_interval: Duration,
) {
    loop {
        match peer.accept_connection().await {
            Ok(conn) => {
                let id = conn.remote_peer().id.clone();
                connected.lock().expect("connected lock").insert(id.clone());
                let handle = tokio::spawn(bridge(
                    conn,
                    node.clone(),
                    Role::Responder,
                    pull_interval,
                    Arc::clone(&connected),
                    id,
                ));
                track(&bridges, handle);
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

async fn connect_loop(
    peer: Arc<Peer>,
    node: SyncNode,
    connected: Arc<Mutex<HashSet<String>>>,
    bridges: Bridges,
    pull_interval: Duration,
) {
    let my_id = match peer.peer_id() {
        Some(id) => id.to_string(),
        None => return,
    };
    loop {
        if let Ok(targets) = peer.discover_peers().await {
            for target in targets {
                if target.id <= my_id {
                    continue;
                }
                {
                    let mut guard = connected.lock().expect("connected lock");
                    if guard.contains(&target.id) {
                        continue;
                    }
                    guard.insert(target.id.clone());
                }
                match peer.connect_to(&target).await {
                    Ok(conn) => {
                        let handle = tokio::spawn(bridge(
                            conn,
                            node.clone(),
                            Role::Initiator,
                            pull_interval,
                            Arc::clone(&connected),
                            target.id.clone(),
                        ));
                        track(&bridges, handle);
                    }
                    Err(_) => {
                        connected.lock().expect("connected lock").remove(&target.id);
                    }
                }
            }
        }
        tokio::time::sleep(pull_interval).await;
    }
}

async fn bridge(
    conn: P2pConnection,
    node: SyncNode,
    role: Role,
    pull_interval: Duration,
    connected: Arc<Mutex<HashSet<String>>>,
    remote_id: String,
) {
    let streams = match role {
        Role::Initiator => conn.open_stream().await,
        Role::Responder => conn.accept_stream().await,
    };
    if let Ok((send, recv)) = streams {
        let rx = node.register_session();
        let _ = session::run(node.state(), recv, send, rx, pull_interval).await;
    }
    connected.lock().expect("connected lock").remove(&remote_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_parses_to_peer_id() {
        let hex: Vec<String> = (0..32).map(|i| format!("{i:02x}")).collect();
        let fp = format!("sha256:{}", hex.join(":"));
        let id = peer_id_from_fingerprint(&fp).expect("valid fingerprint");
        assert_eq!(id[0], 0);
        assert_eq!(id[31], 31);
    }

    #[test]
    fn fingerprint_without_prefix_is_rejected() {
        assert!(peer_id_from_fingerprint("00:01:02").is_none());
    }

    #[test]
    fn fingerprint_wrong_length_is_rejected() {
        assert!(peer_id_from_fingerprint("sha256:00:01:02").is_none());
    }

    #[test]
    fn fingerprint_non_hex_is_rejected() {
        let parts: Vec<&str> = std::iter::repeat_n("zz", 32).collect();
        let fp = format!("sha256:{}", parts.join(":"));
        assert!(peer_id_from_fingerprint(&fp).is_none());
    }
}
