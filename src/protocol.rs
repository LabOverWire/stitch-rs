use crate::hlc::{PEER_ID_LEN, PeerId};
use crate::replog::Cursors;
use crate::wire::{WireError, WriteFrame};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MSG_HELLO: u8 = 0;
const MSG_DELTA: u8 = 1;

/// Upper bound on a single framed message, rejected before allocation. Guards
/// against a peer announcing an enormous length.
pub const MAX_MESSAGE_LEN: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame: {0}")]
    Wire(#[from] WireError),
    #[error("unknown message type {0}")]
    BadType(u8),
    #[error("message length {0} exceeds {MAX_MESSAGE_LEN}")]
    TooLarge(usize),
    #[error("payload truncated: needed {need}, had {have}")]
    Truncated { need: usize, have: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncMessage {
    Hello(Cursors),
    Delta(Vec<WriteFrame>),
}

fn encode_hello(cursors: &Cursors) -> Vec<u8> {
    let count = u32::try_from(cursors.len()).expect("cursor count exceeds u32");
    let mut payload = Vec::with_capacity(4 + cursors.len() * (PEER_ID_LEN + 8));
    payload.extend_from_slice(&count.to_be_bytes());
    for (peer, cursor) in cursors {
        payload.extend_from_slice(peer);
        payload.extend_from_slice(&cursor.to_be_bytes());
    }
    payload
}

fn decode_hello(payload: &[u8]) -> Result<Cursors, ProtocolError> {
    if payload.len() < 4 {
        return Err(ProtocolError::Truncated {
            need: 4,
            have: payload.len(),
        });
    }
    let count = u32::from_be_bytes(payload[0..4].try_into().expect("4 bytes")) as usize;
    let entry_len = PEER_ID_LEN + 8;
    let need = 4 + count * entry_len;
    if payload.len() < need {
        return Err(ProtocolError::Truncated {
            need,
            have: payload.len(),
        });
    }
    let mut cursors: Cursors = HashMap::with_capacity(count);
    let mut off = 4;
    for _ in 0..count {
        let mut peer: PeerId = [0u8; PEER_ID_LEN];
        peer.copy_from_slice(&payload[off..off + PEER_ID_LEN]);
        off += PEER_ID_LEN;
        let cursor = u64::from_be_bytes(payload[off..off + 8].try_into().expect("8 bytes"));
        off += 8;
        cursors.insert(peer, cursor);
    }
    Ok(cursors)
}

fn encode_delta(frames: &[WriteFrame]) -> Result<Vec<u8>, ProtocolError> {
    let count = u32::try_from(frames.len()).expect("frame count exceeds u32");
    let mut payload = Vec::new();
    payload.extend_from_slice(&count.to_be_bytes());
    for frame in frames {
        let bytes = frame.encode()?;
        let len = u32::try_from(bytes.len()).expect("frame exceeds u32");
        payload.extend_from_slice(&len.to_be_bytes());
        payload.extend_from_slice(&bytes);
    }
    Ok(payload)
}

fn decode_delta(payload: &[u8]) -> Result<Vec<WriteFrame>, ProtocolError> {
    if payload.len() < 4 {
        return Err(ProtocolError::Truncated {
            need: 4,
            have: payload.len(),
        });
    }
    let count = u32::from_be_bytes(payload[0..4].try_into().expect("4 bytes")) as usize;
    let mut frames = Vec::with_capacity(count);
    let mut off = 4;
    for _ in 0..count {
        if payload.len() < off + 4 {
            return Err(ProtocolError::Truncated {
                need: off + 4,
                have: payload.len(),
            });
        }
        let len = u32::from_be_bytes(payload[off..off + 4].try_into().expect("4 bytes")) as usize;
        off += 4;
        if payload.len() < off + len {
            return Err(ProtocolError::Truncated {
                need: off + len,
                have: payload.len(),
            });
        }
        frames.push(WriteFrame::decode(&payload[off..off + len])?);
        off += len;
    }
    Ok(frames)
}

pub async fn write_message<W>(w: &mut W, msg: &SyncMessage) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    let (kind, payload) = match msg {
        SyncMessage::Hello(c) => (MSG_HELLO, encode_hello(c)),
        SyncMessage::Delta(frames) => (MSG_DELTA, encode_delta(frames)?),
    };
    let len = u32::try_from(payload.len()).map_err(|_| ProtocolError::TooLarge(payload.len()))?;
    w.write_all(&[kind]).await?;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(&payload).await?;
    w.flush().await?;
    Ok(())
}

pub async fn read_message<R>(r: &mut R) -> Result<SyncMessage, ProtocolError>
where
    R: AsyncRead + Unpin,
{
    let mut kind = [0u8; 1];
    r.read_exact(&mut kind).await?;
    let mut len_bytes = [0u8; 4];
    r.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > MAX_MESSAGE_LEN {
        return Err(ProtocolError::TooLarge(len));
    }
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    match kind[0] {
        MSG_HELLO => Ok(SyncMessage::Hello(decode_hello(&payload)?)),
        MSG_DELTA => Ok(SyncMessage::Delta(decode_delta(&payload)?)),
        other => Err(ProtocolError::BadType(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::{Hlc, Stamp};
    use crate::lww::Op;

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; PEER_ID_LEN];
        id[0] = n;
        id
    }

    fn frame(origin: u8, seq: u64) -> WriteFrame {
        WriteFrame {
            stamp: Stamp::new(Hlc::new(seq, 0), peer(origin)),
            seq,
            op: Op::Insert,
            entity: "task".into(),
            id: format!("t{seq}"),
            data: vec![origin, 1, 2],
            signature: None,
        }
    }

    #[tokio::test]
    async fn hello_round_trips_over_pipe() {
        let mut cursors = Cursors::new();
        cursors.insert(peer(1), 3);
        cursors.insert(peer(2), 7);
        let msg = SyncMessage::Hello(cursors);

        let (mut client, mut server) = tokio::io::duplex(4096);
        write_message(&mut client, &msg).await.unwrap();
        let got = read_message(&mut server).await.unwrap();
        assert_eq!(got, msg);
    }

    #[tokio::test]
    async fn delta_round_trips_over_pipe() {
        let msg = SyncMessage::Delta(vec![frame(1, 1), frame(2, 1), frame(2, 2)]);
        let (mut client, mut server) = tokio::io::duplex(4096);
        write_message(&mut client, &msg).await.unwrap();
        let got = read_message(&mut server).await.unwrap();
        assert_eq!(got, msg);
    }

    #[tokio::test]
    async fn empty_delta_round_trips() {
        let msg = SyncMessage::Delta(vec![]);
        let (mut client, mut server) = tokio::io::duplex(4096);
        write_message(&mut client, &msg).await.unwrap();
        assert_eq!(read_message(&mut server).await.unwrap(), msg);
    }

    #[tokio::test]
    async fn two_messages_back_to_back() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let m1 = SyncMessage::Hello({
            let mut c = Cursors::new();
            c.insert(peer(9), 1);
            c
        });
        let m2 = SyncMessage::Delta(vec![frame(9, 1)]);
        write_message(&mut client, &m1).await.unwrap();
        write_message(&mut client, &m2).await.unwrap();
        assert_eq!(read_message(&mut server).await.unwrap(), m1);
        assert_eq!(read_message(&mut server).await.unwrap(), m2);
    }
}
