use crate::hlc::PeerId;
use crate::protocol::{ProtocolError, SyncMessage, read_message, write_message};
use crate::replog::{Cursors, RecordOutcome};
use crate::sync_state::SyncState;
use crate::wire::WriteFrame;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, mpsc};

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Drive the sync protocol over one connection until the peer disconnects or the
/// outbound channel closes. Symmetric — both peers run this.
///
/// The protocol is periodic pull-based anti-entropy, matching the verified
/// `initiator` mode of `spec/SessionPull.tla`:
/// - send a `Hello` carrying our cursors immediately on connect; thereafter,
///   once it has learned the peer's id, only the lower-id peer of the session
///   sends the periodic `Hello`
/// - on an inbound `Hello`, reply with a `Delta` only when it is non-empty, and
///   send a `Hello` back when the peer's cursors show it is ahead of us — the
///   pull-back that keeps both directions moving with one side ticking
/// - the higher-id (non-ticking) peer also answers every `Hello` with its
///   cursors so the ticking peer's reclaim low-water-mark stays current; that
///   reply carries no frames and is not part of the convergence model (it is a
///   reclamation aid — see `spec/README.md`)
/// - on an inbound `Delta`, apply each frame (LWW + GC)
/// - on a local write (via `outbound`), push it immediately as a one-frame
///   `Delta` — a latency optimization; correctness rests on the periodic pull
///
/// Transitive forwarding is not handled here: a node shares one [`SyncState`]
/// across all its sessions, so writes pulled in from one peer are served to
/// others on their next pull. Gaps and lost frames self-heal on the next pull.
///
/// A clean peer disconnect (EOF) returns `Ok(())`.
pub async fn run<R, W>(
    state: Arc<Mutex<SyncState>>,
    mut reader: R,
    mut writer: W,
    mut outbound: mpsc::UnboundedReceiver<WriteFrame>,
    pull_interval: Duration,
) -> Result<(), ProtocolError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let self_id = state.lock().await.self_id();
    let mut remote_id: Option<PeerId> = None;

    send_hello(&state, &mut writer).await?;

    let mut ticker = tokio::time::interval(pull_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        tokio::select! {
            inbound = read_message(&mut reader) => {
                match inbound {
                    Ok(SyncMessage::Hello { from, cursors }) => {
                        remote_id = Some(from);
                        let (catch_up, reply_hello) = {
                            let mut guard = state.lock().await;
                            guard.note_peer_cursors(from, cursors.clone());
                            let catch_up = guard.delta_since(&cursors);
                            let reply_hello = peer_ahead(&cursors, &guard.cursors())
                                || !should_initiate(self_id, remote_id);
                            (catch_up, reply_hello)
                        };
                        if !catch_up.is_empty() {
                            write_message(&mut writer, &SyncMessage::Delta(catch_up)).await?;
                        }
                        if reply_hello {
                            send_hello(&state, &mut writer).await?;
                        }
                    }
                    Ok(SyncMessage::Delta(frames)) => apply(&state, frames).await,
                    Err(e) if is_eof(&e) => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
            local = outbound.recv() => {
                match local {
                    Some(frame) => {
                        write_message(&mut writer, &SyncMessage::Delta(vec![frame])).await?;
                    }
                    None => return Ok(()),
                }
            }
            _ = ticker.tick() => {
                if should_initiate(self_id, remote_id) {
                    send_hello(&state, &mut writer).await?;
                }
            }
        }
    }
}

fn should_initiate(self_id: PeerId, remote_id: Option<PeerId>) -> bool {
    match remote_id {
        Some(remote) => self_id < remote,
        None => true,
    }
}

fn peer_ahead(theirs: &Cursors, ours: &Cursors) -> bool {
    theirs
        .iter()
        .any(|(origin, seq)| *seq > ours.get(origin).copied().unwrap_or(0))
}

async fn send_hello<W>(state: &Mutex<SyncState>, writer: &mut W) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    let (from, cursors) = {
        let guard = state.lock().await;
        (guard.self_id(), guard.cursors())
    };
    write_message(writer, &SyncMessage::Hello { from, cursors }).await
}

async fn apply(state: &Mutex<SyncState>, frames: Vec<WriteFrame>) {
    let now = now_millis();
    let mut guard = state.lock().await;
    for frame in frames {
        let origin = frame.stamp.peer;
        let seq = frame.seq;
        match guard.receive(frame, now) {
            RecordOutcome::Appended | RecordOutcome::Duplicate | RecordOutcome::Unpersisted => {}
            RecordOutcome::Gap { expected, got } => {
                tracing::debug!(origin = ?&origin[..4], expected, got, "frame ahead of the log, waiting for the missing ones");
            }
            RecordOutcome::Rejected => {
                tracing::warn!(origin = ?&origin[..4], seq, "frame failed verification and was dropped");
            }
        }
    }
}

fn is_eof(err: &ProtocolError) -> bool {
    matches!(err, ProtocolError::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::{PEER_ID_LEN, PeerId};
    use crate::lww::Op;
    use std::time::Duration;
    use tokio::io::split;

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; PEER_ID_LEN];
        id[0] = n;
        id
    }

    const FAST_PULL: Duration = Duration::from_millis(20);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_peers_converge_over_pipe() {
        let a_state = Arc::new(Mutex::new(SyncState::new(peer(1))));
        let b_state = Arc::new(Mutex::new(SyncState::new(peer(2))));

        a_state
            .lock()
            .await
            .local_write(10, Op::Insert, "task", "t1", b"from-a".to_vec())
            .expect("write");
        b_state
            .lock()
            .await
            .local_write(11, Op::Insert, "task", "t2", b"from-b".to_vec())
            .expect("write");

        let (a_io, b_io) = tokio::io::duplex(16 * 1024);
        let (a_r, a_w) = split(a_io);
        let (b_r, b_w) = split(b_io);
        let (a_tx, a_rx) = mpsc::unbounded_channel();
        let (b_tx, b_rx) = mpsc::unbounded_channel();

        let a = tokio::spawn(run(a_state.clone(), a_r, a_w, a_rx, FAST_PULL));
        let b = tokio::spawn(run(b_state.clone(), b_r, b_w, b_rx, FAST_PULL));

        let ok = wait_until(Duration::from_secs(2), || async {
            let a = a_state.lock().await;
            let b = b_state.lock().await;
            a.visible("task", "t2").is_some() && b.visible("task", "t1").is_some()
        })
        .await;

        drop(a_tx);
        drop(b_tx);
        a.abort();
        b.abort();

        assert!(ok, "peers did not converge");
        assert_eq!(
            a_state.lock().await.visible("task", "t2"),
            Some(&b"from-b"[..])
        );
        assert_eq!(
            b_state.lock().await.visible("task", "t1"),
            Some(&b"from-a"[..])
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_after_connect_propagates() {
        let a_state = Arc::new(Mutex::new(SyncState::new(peer(1))));
        let b_state = Arc::new(Mutex::new(SyncState::new(peer(2))));

        let (a_io, b_io) = tokio::io::duplex(16 * 1024);
        let (a_r, a_w) = split(a_io);
        let (b_r, b_w) = split(b_io);
        let (a_tx, a_rx) = mpsc::unbounded_channel();
        let (b_tx, b_rx) = mpsc::unbounded_channel();

        let a = tokio::spawn(run(a_state.clone(), a_r, a_w, a_rx, FAST_PULL));
        let b = tokio::spawn(run(b_state.clone(), b_r, b_w, b_rx, FAST_PULL));

        // Write on A *after* the session is live; push it through A's outbound.
        let frame = a_state
            .lock()
            .await
            .local_write(now_millis(), Op::Insert, "task", "late", b"v".to_vec())
            .expect("write");
        a_tx.send(frame).unwrap();

        let ok = wait_until(Duration::from_secs(2), || async {
            b_state.lock().await.visible("task", "late").is_some()
        })
        .await;

        drop(a_tx);
        drop(b_tx);
        a.abort();
        b.abort();

        assert!(ok, "post-connect write did not propagate");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lower_peer_pull_drains_higher_peer_write() {
        let a_state = Arc::new(Mutex::new(SyncState::new(peer(1))));
        let b_state = Arc::new(Mutex::new(SyncState::new(peer(2))));

        let (a_io, b_io) = tokio::io::duplex(16 * 1024);
        let (a_r, a_w) = split(a_io);
        let (b_r, b_w) = split(b_io);
        let (a_tx, a_rx) = mpsc::unbounded_channel();
        let (b_tx, b_rx) = mpsc::unbounded_channel();

        let a = tokio::spawn(run(a_state.clone(), a_r, a_w, a_rx, FAST_PULL));
        let b = tokio::spawn(run(b_state.clone(), b_r, b_w, b_rx, FAST_PULL));

        b_state
            .lock()
            .await
            .local_write(12, Op::Insert, "task", "b-late", b"from-b".to_vec())
            .expect("write");

        let ok = wait_until(Duration::from_secs(2), || async {
            a_state.lock().await.visible("task", "b-late").is_some()
        })
        .await;

        drop(a_tx);
        drop(b_tx);
        a.abort();
        b.abort();

        assert!(
            ok,
            "lower peer's periodic pull did not drain the higher peer's write"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lower_peer_cursors_refresh_so_reclaim_advances() {
        let a_state = Arc::new(Mutex::new(SyncState::new(peer(1))));
        let b_state = Arc::new(Mutex::new(SyncState::new(peer(2))));

        a_state
            .lock()
            .await
            .local_write(10, Op::Insert, "task", "t1", b"from-a".to_vec())
            .expect("write");
        b_state
            .lock()
            .await
            .local_write(11, Op::Insert, "task", "t2", b"from-b".to_vec())
            .expect("write");

        let (a_io, b_io) = tokio::io::duplex(16 * 1024);
        let (a_r, a_w) = split(a_io);
        let (b_r, b_w) = split(b_io);
        let (a_tx, a_rx) = mpsc::unbounded_channel();
        let (b_tx, b_rx) = mpsc::unbounded_channel();

        let a = tokio::spawn(run(a_state.clone(), a_r, a_w, a_rx, FAST_PULL));
        let b = tokio::spawn(run(b_state.clone(), b_r, b_w, b_rx, FAST_PULL));

        let fresh = wait_until(Duration::from_secs(2), || async {
            let a = a_state.lock().await;
            a.visible("task", "t2").is_some() && a.peer_cursor(&peer(2), &peer(1)) >= 1
        })
        .await;

        let dropped = a_state.lock().await.reclaim(&[peer(2)]);

        drop(a_tx);
        drop(b_tx);
        a.abort();
        b.abort();

        assert!(
            fresh,
            "lower peer never learned the higher peer caught up on its origin"
        );
        assert_eq!(
            dropped, 2,
            "lower peer could not reclaim both frames; its view of the higher peer's cursors went stale"
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
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }
}
