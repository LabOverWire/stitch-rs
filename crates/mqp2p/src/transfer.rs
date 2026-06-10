use crate::error::{Error, Result};
use quinn::{RecvStream, SendStream};
use ring::digest;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, trace};

const FRAME_FILE_OFFER: u8 = 0x01;
const FRAME_FILE_ACCEPT: u8 = 0x02;
const FRAME_FILE_REJECT: u8 = 0x03;
const FRAME_DATA_CHUNK: u8 = 0x10;
const FRAME_TRANSFER_COMPLETE: u8 = 0x20;
const FRAME_ACK: u8 = 0x30;

const CHUNK_SIZE: usize = 64 * 1024;
const MAX_FRAME_PAYLOAD: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOffer {
    pub name: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRejectReason {
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct TransferProgress {
    pub bytes_transferred: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct TransferResult {
    pub file_name: String,
    pub file_size: u64,
    pub sha256: String,
}

async fn write_frame(stream: &mut SendStream, frame_type: u8, payload: &[u8]) -> Result<()> {
    let len = payload.len() as u32;
    let mut header = [0u8; 5];
    header[0] = frame_type;
    header[1..5].copy_from_slice(&len.to_be_bytes());
    stream
        .write_all(&header)
        .await
        .map_err(|e| Error::Transfer(format!("write frame header: {e}")))?;
    if !payload.is_empty() {
        stream
            .write_all(payload)
            .await
            .map_err(|e| Error::Transfer(format!("write frame payload: {e}")))?;
    }
    Ok(())
}

async fn read_frame(stream: &mut RecvStream) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 5];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|e| Error::Transfer(format!("read frame header: {e}")))?;

    let frame_type = header[0];
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

    if len > MAX_FRAME_PAYLOAD {
        return Err(Error::Transfer(format!(
            "frame payload too large: {len} bytes"
        )));
    }

    let mut payload = vec![0u8; len];
    if len > 0 {
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|e| Error::Transfer(format!("read frame payload: {e}")))?;
    }

    Ok((frame_type, payload))
}

fn digest_to_hex(d: &digest::Digest) -> String {
    d.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

pub async fn send_file(
    send: &mut SendStream,
    recv: &mut RecvStream,
    path: &Path,
    mut progress: impl FnMut(TransferProgress),
) -> Result<TransferResult> {
    let metadata = tokio::fs::metadata(path).await?;
    let file_size = metadata.len();
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    let mut hasher = digest::Context::new(&digest::SHA256);
    let mut hash_file = tokio::fs::File::open(path).await?;
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = hash_file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let sha256_digest = hasher.finish();
    let sha256 = digest_to_hex(&sha256_digest);

    let offer = FileOffer {
        name: file_name.clone(),
        size: file_size,
        sha256: sha256.clone(),
    };
    let offer_json = serde_json::to_vec(&offer)?;
    write_frame(send, FRAME_FILE_OFFER, &offer_json).await?;
    debug!(name = file_name, size = file_size, "sent file offer");

    let (frame_type, payload) = read_frame(recv).await?;
    match frame_type {
        FRAME_FILE_ACCEPT => {
            debug!("file offer accepted");
        }
        FRAME_FILE_REJECT => {
            let reason: FileRejectReason =
                serde_json::from_slice(&payload).unwrap_or(FileRejectReason {
                    reason: "unknown".into(),
                });
            return Err(Error::TransferRejected(reason.reason));
        }
        other => return Err(Error::UnexpectedFrame(other)),
    }

    let mut file = tokio::fs::File::open(path).await?;
    let mut offset: u64 = 0;
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        let mut chunk_payload = Vec::with_capacity(8 + n);
        chunk_payload.extend_from_slice(&offset.to_be_bytes());
        chunk_payload.extend_from_slice(&buf[..n]);
        write_frame(send, FRAME_DATA_CHUNK, &chunk_payload).await?;

        offset += n as u64;
        progress(TransferProgress {
            bytes_transferred: offset,
            total_bytes: file_size,
        });

        trace!(offset, total = file_size, "sent data chunk");
    }

    write_frame(send, FRAME_TRANSFER_COMPLETE, sha256_digest.as_ref()).await?;

    let (ack_type, ack_payload) = read_frame(recv).await?;
    if ack_type != FRAME_ACK {
        return Err(Error::UnexpectedFrame(ack_type));
    }
    if ack_payload.len() >= 8 {
        let acked = u64::from_be_bytes([
            ack_payload[0],
            ack_payload[1],
            ack_payload[2],
            ack_payload[3],
            ack_payload[4],
            ack_payload[5],
            ack_payload[6],
            ack_payload[7],
        ]);
        debug!(acked, "transfer acknowledged");
    }

    send.finish()
        .map_err(|e| Error::Transfer(format!("finish send stream: {e}")))?;

    Ok(TransferResult {
        file_name,
        file_size,
        sha256,
    })
}

pub async fn receive_file(
    send: &mut SendStream,
    recv: &mut RecvStream,
    output_dir: &Path,
    accept: impl FnOnce(&FileOffer) -> bool,
    mut progress: impl FnMut(TransferProgress),
) -> Result<TransferResult> {
    let (frame_type, payload) = read_frame(recv).await?;
    if frame_type != FRAME_FILE_OFFER {
        return Err(Error::UnexpectedFrame(frame_type));
    }

    let offer: FileOffer = serde_json::from_slice(&payload)?;
    debug!(name = offer.name, size = offer.size, "received file offer");

    if !accept(&offer) {
        let reject = FileRejectReason {
            reason: "rejected by receiver".into(),
        };
        write_frame(send, FRAME_FILE_REJECT, &serde_json::to_vec(&reject)?).await?;
        return Err(Error::TransferRejected("rejected by receiver".into()));
    }

    write_frame(send, FRAME_FILE_ACCEPT, &[]).await?;

    let temp_path = output_dir.join(format!(".{}.tmp", offer.name));
    let out_path = output_dir.join(&offer.name);
    let mut file = tokio::fs::File::create(&temp_path).await?;
    let mut hasher = Some(digest::Context::new(&digest::SHA256));
    let mut bytes_received: u64 = 0;
    let final_hash;

    loop {
        let (frame_type, payload) = read_frame(recv).await?;

        match frame_type {
            FRAME_DATA_CHUNK => {
                if payload.len() < 8 {
                    return Err(Error::Transfer("data chunk too short".into()));
                }
                let chunk_data = &payload[8..];
                file.write_all(chunk_data).await?;
                if let Some(ref mut ctx) = hasher {
                    ctx.update(chunk_data);
                }
                bytes_received += chunk_data.len() as u64;

                progress(TransferProgress {
                    bytes_transferred: bytes_received,
                    total_bytes: offer.size,
                });

                trace!(
                    received = bytes_received,
                    total = offer.size,
                    "received chunk"
                );
            }
            FRAME_TRANSFER_COMPLETE => {
                if payload.len() < 32 {
                    return Err(Error::Transfer("transfer complete hash too short".into()));
                }

                let received_hash: String =
                    payload[..32].iter().map(|b| format!("{b:02x}")).collect();

                let ctx = hasher
                    .take()
                    .ok_or_else(|| Error::Transfer("hasher already consumed".into()))?;
                let computed = ctx.finish();
                let computed_hash = digest_to_hex(&computed);

                if received_hash != computed_hash {
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    return Err(Error::HashMismatch {
                        expected: received_hash,
                        actual: computed_hash,
                    });
                }

                final_hash = computed_hash;
                debug!(hash = final_hash, "transfer complete, hash verified");
                break;
            }
            other => return Err(Error::UnexpectedFrame(other)),
        }
    }

    file.flush().await?;
    drop(file);
    tokio::fs::rename(&temp_path, &out_path).await?;
    debug!(path = %out_path.display(), "file written to disk");

    let ack_payload = bytes_received.to_be_bytes();
    write_frame(send, FRAME_ACK, &ack_payload).await?;
    send.finish()
        .map_err(|e| Error::Transfer(format!("finish send stream: {e}")))?;

    Ok(TransferResult {
        file_name: offer.name,
        file_size: bytes_received,
        sha256: final_hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_hash_is_deterministic() {
        let data = b"hello world";
        let d1 = digest::digest(&digest::SHA256, data);
        let d2 = digest::digest(&digest::SHA256, data);
        let h1 = digest_to_hex(&d1);
        let h2 = digest_to_hex(&d2);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
    }

    #[test]
    fn compute_hash_differs_for_different_data() {
        let d1 = digest::digest(&digest::SHA256, b"hello");
        let d2 = digest::digest(&digest::SHA256, b"world");
        assert_ne!(digest_to_hex(&d1), digest_to_hex(&d2));
    }

    #[test]
    fn file_offer_serialization_roundtrip() {
        let offer = FileOffer {
            name: "test.txt".into(),
            size: 1024,
            sha256: "abc123".into(),
        };
        let json = serde_json::to_vec(&offer).unwrap();
        let parsed: FileOffer = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed.name, "test.txt");
        assert_eq!(parsed.size, 1024);
        assert_eq!(parsed.sha256, "abc123");
    }

    #[test]
    fn streaming_hash_matches_oneshot() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let oneshot = digest::digest(&digest::SHA256, data);

        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(&data[..10]);
        ctx.update(&data[10..]);
        let streaming = ctx.finish();

        assert_eq!(digest_to_hex(&oneshot), digest_to_hex(&streaming));
    }
}
