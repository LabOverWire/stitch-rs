use crate::lww::Op;
use crate::protocol::{ProtocolError, SyncMessage, read_message, write_message};
use crate::sync_state::SyncState;
use crate::wire::WriteFrame;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, mpsc};

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Originate a local mutation and queue it for the session to forward. Applies
/// to local state immediately; the frame propagates to the connected peer when
/// the session's write loop next runs.
///
/// # Errors
/// Returns the frame back if the outbound channel is closed (session ended).
pub async fn local_write(
    state: &Mutex<SyncState>,
    outbound: &mpsc::UnboundedSender<WriteFrame>,
    op: Op,
    entity: impl Into<String>,
    id: impl Into<String>,
    data: Vec<u8>,
) -> Result<WriteFrame, WriteFrame> {
    let frame = {
        let mut guard = state.lock().await;
        guard.local_write(now_millis(), op, entity, id, data)
    };
    match outbound.send(frame.clone()) {
        Ok(()) => Ok(frame),
        Err(_) => Err(frame),
    }
}

/// Drive the sync protocol over one connection until the peer disconnects or the
/// outbound channel closes. Symmetric: both peers run this. Sequence:
///   1. send `Hello` with our cursors
///   2. read their `Hello`
///   3. send the `Delta` they're missing
///   4. loop: apply inbound deltas, forward local writes
///
/// A clean peer disconnect (EOF) returns `Ok(())`.
pub async fn run<R, W>(
    state: Arc<Mutex<SyncState>>,
    mut reader: R,
    mut writer: W,
    mut outbound: mpsc::UnboundedReceiver<WriteFrame>,
) -> Result<(), ProtocolError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let our_cursors = state.lock().await.cursors();
    write_message(&mut writer, &SyncMessage::Hello(our_cursors)).await?;

    let their_cursors = loop {
        match read_message(&mut reader).await {
            Ok(SyncMessage::Hello(c)) => break c,
            Ok(SyncMessage::Delta(frames)) => apply(&state, frames).await,
            Err(e) if is_eof(&e) => return Ok(()),
            Err(e) => return Err(e),
        }
    };

    let catch_up = state.lock().await.delta_since(&their_cursors);
    write_message(&mut writer, &SyncMessage::Delta(catch_up)).await?;

    loop {
        tokio::select! {
            inbound = read_message(&mut reader) => {
                match inbound {
                    Ok(SyncMessage::Delta(frames)) => apply(&state, frames).await,
                    Ok(SyncMessage::Hello(_)) => {}
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
        }
    }
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
    use std::time::Duration;
    use tokio::io::split;

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; PEER_ID_LEN];
        id[0] = n;
        id
    }

    async fn settle() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

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
        let (_a_tx, a_rx) = mpsc::unbounded_channel();
        let (_b_tx, b_rx) = mpsc::unbounded_channel();

        let a = tokio::spawn(run(a_state.clone(), a_r, a_w, a_rx));
        let b = tokio::spawn(run(b_state.clone(), b_r, b_w, b_rx));

        settle().await;
        drop(_a_tx);
        drop(_b_tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), a).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), b).await;

        let a_final = a_state.lock().await;
        let b_final = b_state.lock().await;
        assert_eq!(a_final.visible("task", "t1"), Some(&b"from-a"[..]));
        assert_eq!(a_final.visible("task", "t2"), Some(&b"from-b"[..]));
        assert_eq!(b_final.visible("task", "t1"), Some(&b"from-a"[..]));
        assert_eq!(b_final.visible("task", "t2"), Some(&b"from-b"[..]));
    }
}
