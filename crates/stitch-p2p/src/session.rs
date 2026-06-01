use crate::protocol::{ProtocolError, SyncMessage, read_message, write_message};
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
/// `Sync` action in `spec/`:
/// - send a `Hello` carrying our cursors immediately, then every `pull_interval`
/// - on an inbound `Hello`, reply with the `Delta` of writes that peer is missing
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
    send_hello(&state, &mut writer).await?;

    let mut ticker = tokio::time::interval(pull_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        tokio::select! {
            inbound = read_message(&mut reader) => {
                match inbound {
                    Ok(SyncMessage::Hello { from, cursors }) => {
                        let catch_up = {
                            let mut guard = state.lock().await;
                            guard.note_peer_cursors(from, cursors.clone());
                            guard.delta_since(&cursors)
                        };
                        write_message(&mut writer, &SyncMessage::Delta(catch_up)).await?;
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
                send_hello(&state, &mut writer).await?;
            }
        }
    }
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
        guard.receive(frame, now);
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
            .local_write(10, Op::Insert, "task", "t1", b"from-a".to_vec());
        b_state
            .lock()
            .await
            .local_write(11, Op::Insert, "task", "t2", b"from-b".to_vec());

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
        let frame = a_state.lock().await.local_write(
            now_millis(),
            Op::Insert,
            "task",
            "late",
            b"v".to_vec(),
        );
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
