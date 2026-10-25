use crate::hlc::{PEER_ID_LEN, PeerId};
use crate::node::SyncNode;
use crate::protocol::{SyncMessage, read_message, write_message};
use crate::session;
use crate::wire::WriteFrame;
use mqp2p::{P2pConnection, Peer};
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinHandle;

/// How long the membership-proof exchange may take before a gated peer gives up
/// and decides on local membership alone. Bounds the whole handshake, so a peer
/// that never participates (e.g. an ungated swarm) costs at most this.
const PROOF_WAIT: Duration = Duration::from_secs(3);

/// Decides whether a connected peer, identified by its verified transport
/// public key, may join the swarm. Implemented by
/// `stitch_p2p::membership::Membership` so a swarm accepts and dials only
/// authorized members; without a gate every peer is admitted.
pub trait MemberGate: Send + Sync + 'static {
    fn admits(&self, key: [u8; 32]) -> Pin<Box<dyn Future<Output = bool> + Send + '_>>;

    /// This node's own membership proof — the signed records it offers a peer
    /// that may not yet have synced this node's membership. Default: nothing to
    /// offer, so gates without a bootstrap story behave exactly as before.
    fn local_proof(&self) -> Pin<Box<dyn Future<Output = Vec<WriteFrame>> + Send + '_>> {
        Box::pin(async { Vec::new() })
    }

    /// Decide admission for `key` given the membership proof it presented.
    /// Default: a gate with no membership model cannot verify a chain, so it
    /// records how many frames it is setting aside and falls back to
    /// [`MemberGate::admits`].
    fn admits_with_proof(
        &self,
        key: [u8; 32],
        proof: Vec<WriteFrame>,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        tracing::trace!(
            frames = proof.len(),
            "gate has no proof model; deciding on membership alone"
        );
        self.admits(key)
    }
}

async fn gate_admits(gate: &Option<Arc<dyn MemberGate>>, key: Option<[u8; 32]>) -> bool {
    match gate {
        None => true,
        Some(gate) => match key {
            Some(key) => gate.admits(key).await,
            None => false,
        },
    }
}

async fn write_want<W: AsyncWrite + Unpin>(stream: &mut W, want: bool) -> std::io::Result<()> {
    stream.write_all(&[u8::from(want)]).await
}

async fn read_want<R: AsyncRead + Unpin>(stream: &mut R) -> std::io::Result<bool> {
    let mut byte = [0u8; 1];
    stream.read_exact(&mut byte).await?;
    Ok(byte[0] != 0)
}

/// Run the request-driven membership-proof exchange on a gated connection and
/// return whether a presented proof admits `key`. Each side opens one uni
/// stream carrying a `want` byte (set only when it could not admit the peer
/// locally); it then writes a `Delta` with its proof only if the peer asked. A
/// peer's proof is therefore disclosed only to a peer that requested it, never
/// pushed, and a converged pair exchanges only the two `want` bytes. The whole
/// exchange is bounded by [`PROOF_WAIT`], so a peer that never participates
/// falls back to the local decision instead of hanging. A node must still run
/// the exchange even when it admits the peer locally, in order to serve the
/// peer's own request; against a peer that never opens its stream (e.g. an
/// ungated swarm) this costs up to [`PROOF_WAIT`] before admission, which only
/// affects mixed gated/ungated topologies — an all-gated pair resolves at once.
async fn proof_exchange(
    conn: &P2pConnection,
    gate: &Arc<dyn MemberGate>,
    key: [u8; 32],
    local_ok: bool,
) -> bool {
    let exchange = async {
        let mut out = conn.open_uni().await.ok()?;
        write_want(&mut out, !local_ok).await.ok()?;
        let mut inbound = conn.accept_uni().await.ok()?;
        let peer_wants = read_want(&mut inbound).await.ok()?;
        if peer_wants {
            let offered = gate.local_proof().await;
            write_message(&mut out, &SyncMessage::Delta(offered))
                .await
                .ok()?;
        }
        let _ = out.finish();
        if local_ok {
            return Some(false);
        }
        match read_message(&mut inbound).await.ok()? {
            SyncMessage::Delta(frames) => Some(gate.admits_with_proof(key, frames).await),
            _ => Some(false),
        }
    };
    match tokio::time::timeout(PROOF_WAIT, exchange).await {
        Ok(Some(result)) => result,
        _ => false,
    }
}

/// Decide whether a connected peer may join. On a gated connection both sides
/// run a request-driven proof exchange: a side asks for the peer's proof only
/// when it cannot admit the peer from local membership, and serves its own
/// proof only when asked. A peer is admitted on local membership or on a proof
/// that resolves it to the genesis owner — breaking the member-to-member
/// bootstrap deadlock without pushing membership to peers that never needed it.
async fn admitted(
    conn: P2pConnection,
    gate: &Option<Arc<dyn MemberGate>>,
) -> Option<P2pConnection> {
    let key = conn.remote_public_key();
    let local_ok = gate_admits(gate, key).await;
    let Some(gate) = gate else {
        return Some(conn);
    };
    let Some(key) = key else {
        let _ = conn.close();
        return None;
    };
    let proof_ok = proof_exchange(&conn, gate, key, local_ok).await;
    if local_ok || proof_ok {
        Some(conn)
    } else {
        let _ = conn.close();
        None
    }
}

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
        Self::spawn_with_discovery_interval(peer, node, pull_interval, pull_interval)
    }

    #[must_use]
    pub fn spawn_with_discovery_interval(
        peer: Arc<Peer>,
        node: SyncNode,
        pull_interval: Duration,
        discovery_interval: Duration,
    ) -> Self {
        Self::spawn_inner(peer, node, pull_interval, discovery_interval, None)
    }

    /// Like [`Swarm::spawn`], but only peers whose verified transport key the
    /// `gate` admits are connected; others are closed immediately. Used for
    /// membership-gated pairing (see `stitch_p2p::membership::Membership`).
    #[must_use]
    pub fn spawn_gated(
        peer: Arc<Peer>,
        node: SyncNode,
        pull_interval: Duration,
        gate: Arc<dyn MemberGate>,
    ) -> Self {
        Self::spawn_inner(peer, node, pull_interval, pull_interval, Some(gate))
    }

    fn spawn_inner(
        peer: Arc<Peer>,
        node: SyncNode,
        pull_interval: Duration,
        discovery_interval: Duration,
        gate: Option<Arc<dyn MemberGate>>,
    ) -> Self {
        let connected: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let bridges: Bridges = Arc::new(Mutex::new(Vec::new()));
        let accept = tokio::spawn(accept_loop(
            Arc::clone(&peer),
            node.clone(),
            Arc::clone(&connected),
            Arc::clone(&bridges),
            pull_interval,
            gate.clone(),
        ));
        let connect = tokio::spawn(connect_loop(
            peer,
            node,
            connected,
            Arc::clone(&bridges),
            pull_interval,
            discovery_interval,
            gate,
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
    gate: Option<Arc<dyn MemberGate>>,
) {
    loop {
        match peer.accept_connection().await {
            Ok(conn) => {
                let id = conn.remote_peer().id.clone();
                connected.lock().expect("connected lock").insert(id.clone());
                let handle = tokio::spawn(admit_and_bridge(
                    conn,
                    node.clone(),
                    Role::Responder,
                    pull_interval,
                    Arc::clone(&connected),
                    id,
                    gate.clone(),
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
    discovery_interval: Duration,
    gate: Option<Arc<dyn MemberGate>>,
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
                        let handle = tokio::spawn(admit_and_bridge(
                            conn,
                            node.clone(),
                            Role::Initiator,
                            pull_interval,
                            Arc::clone(&connected),
                            target.id.clone(),
                            gate.clone(),
                        ));
                        track(&bridges, handle);
                    }
                    Err(_) => {
                        connected.lock().expect("connected lock").remove(&target.id);
                    }
                }
            }
        }
        tokio::time::sleep(discovery_interval).await;
    }
}

async fn admit_and_bridge(
    conn: P2pConnection,
    node: SyncNode,
    role: Role,
    pull_interval: Duration,
    connected: Arc<Mutex<HashSet<String>>>,
    remote_id: String,
    gate: Option<Arc<dyn MemberGate>>,
) {
    match admitted(conn, &gate).await {
        Some(conn) => bridge(conn, node, role, pull_interval, connected, remote_id).await,
        None => {
            connected.lock().expect("connected lock").remove(&remote_id);
        }
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

    struct FixedGate(HashSet<[u8; 32]>);

    impl MemberGate for FixedGate {
        fn admits(&self, key: [u8; 32]) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
            let admitted = self.0.contains(&key);
            Box::pin(async move { admitted })
        }
    }

    #[tokio::test]
    async fn without_a_gate_every_peer_is_admitted() {
        assert!(gate_admits(&None, Some([1u8; 32])).await);
        assert!(gate_admits(&None, None).await);
    }

    #[tokio::test]
    async fn a_gate_admits_members_and_rejects_the_rest() {
        let member = [7u8; 32];
        let outsider = [8u8; 32];
        let gate: Option<Arc<dyn MemberGate>> = Some(Arc::new(FixedGate(HashSet::from([member]))));
        assert!(gate_admits(&gate, Some(member)).await);
        assert!(!gate_admits(&gate, Some(outsider)).await);
        assert!(
            !gate_admits(&gate, None).await,
            "a peer with no readable transport key is rejected"
        );
    }
}
