use crate::error::{Error, Result};
use ring::rand::SecureRandom;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tracing::{debug, trace};

const STUN_MAGIC: u32 = 0x2112_A442;
const STUN_HEADER_LEN: usize = 20;
const STUN_BINDING_REQUEST: u16 = 0x0001;
const STUN_BINDING_RESPONSE: u16 = 0x0101;
const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;
const ATTR_MAPPED_ADDRESS: u16 = 0x0001;
const HOLE_PUNCH_MAGIC: [u8; 4] = [0x50, 0x32, 0x50, 0x50];

pub const DEFAULT_STUN_SERVER: &str = "stun.l.google.com:19302";

fn build_binding_request(txn_id: &[u8; 12]) -> [u8; STUN_HEADER_LEN] {
    let mut buf = [0u8; STUN_HEADER_LEN];
    let msg_type = STUN_BINDING_REQUEST.to_be_bytes();
    buf[0] = msg_type[0];
    buf[1] = msg_type[1];
    buf[4] = (STUN_MAGIC >> 24) as u8;
    buf[5] = (STUN_MAGIC >> 16) as u8;
    buf[6] = (STUN_MAGIC >> 8) as u8;
    buf[7] = STUN_MAGIC as u8;
    buf[8..20].copy_from_slice(txn_id);
    buf
}

fn parse_xor_mapped_address(
    attr_value: &[u8],
    txn_id: &[u8; 12],
) -> std::result::Result<SocketAddr, String> {
    if attr_value.len() < 8 {
        return Err("XOR-MAPPED-ADDRESS too short".into());
    }
    let family = attr_value[1];
    let xor_port = u16::from_be_bytes([attr_value[2], attr_value[3]]);
    let port = xor_port ^ (STUN_MAGIC >> 16) as u16;

    match family {
        0x01 => {
            let xor_ip =
                u32::from_be_bytes([attr_value[4], attr_value[5], attr_value[6], attr_value[7]]);
            let ip = xor_ip ^ STUN_MAGIC;
            let addr = std::net::Ipv4Addr::from(ip);
            Ok(SocketAddr::new(addr.into(), port))
        }
        0x02 => {
            if attr_value.len() < 20 {
                return Err("XOR-MAPPED-ADDRESS IPv6 too short".into());
            }
            let mut xor_bytes = [0u8; 16];
            xor_bytes.copy_from_slice(&attr_value[4..20]);
            let mut key = [0u8; 16];
            key[0..4].copy_from_slice(&STUN_MAGIC.to_be_bytes());
            key[4..16].copy_from_slice(txn_id);
            for i in 0..16 {
                xor_bytes[i] ^= key[i];
            }
            let addr = std::net::Ipv6Addr::from(xor_bytes);
            Ok(SocketAddr::new(addr.into(), port))
        }
        _ => Err(format!("unknown address family: {family:#04x}")),
    }
}

fn parse_mapped_address(attr_value: &[u8]) -> std::result::Result<SocketAddr, String> {
    if attr_value.len() < 8 {
        return Err("MAPPED-ADDRESS too short".into());
    }
    let family = attr_value[1];
    let port = u16::from_be_bytes([attr_value[2], attr_value[3]]);

    match family {
        0x01 => {
            let ip =
                std::net::Ipv4Addr::new(attr_value[4], attr_value[5], attr_value[6], attr_value[7]);
            Ok(SocketAddr::new(ip.into(), port))
        }
        _ => Err(format!("unsupported MAPPED-ADDRESS family: {family:#04x}")),
    }
}

fn parse_binding_response(
    buf: &[u8],
    txn_id: &[u8; 12],
) -> std::result::Result<SocketAddr, String> {
    if buf.len() < STUN_HEADER_LEN {
        return Err("response too short".into());
    }

    let msg_type = u16::from_be_bytes([buf[0], buf[1]]);
    if msg_type != STUN_BINDING_RESPONSE {
        return Err(format!("unexpected message type: {msg_type:#06x}"));
    }

    let resp_txn = &buf[8..20];
    if resp_txn != txn_id.as_slice() {
        return Err("transaction ID mismatch".into());
    }

    let msg_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    let attrs = &buf[STUN_HEADER_LEN..STUN_HEADER_LEN + msg_len.min(buf.len() - STUN_HEADER_LEN)];

    let mut offset = 0;
    let mut xor_mapped = None;
    let mut mapped = None;

    while offset + 4 <= attrs.len() {
        let attr_type = u16::from_be_bytes([attrs[offset], attrs[offset + 1]]);
        let attr_len = u16::from_be_bytes([attrs[offset + 2], attrs[offset + 3]]) as usize;
        offset += 4;

        if offset + attr_len > attrs.len() {
            break;
        }

        let attr_value = &attrs[offset..offset + attr_len];

        match attr_type {
            ATTR_XOR_MAPPED_ADDRESS => {
                xor_mapped = parse_xor_mapped_address(attr_value, txn_id).ok();
            }
            ATTR_MAPPED_ADDRESS => {
                mapped = parse_mapped_address(attr_value).ok();
            }
            _ => {}
        }

        offset += attr_len;
        let padding = (4 - (attr_len % 4)) % 4;
        offset += padding;
    }

    xor_mapped
        .or(mapped)
        .ok_or_else(|| "no mapped address in response".into())
}

pub async fn discover_external_addr(socket: &UdpSocket, stun_server: &str) -> Result<SocketAddr> {
    let server_addr: SocketAddr = tokio::net::lookup_host(stun_server)
        .await
        .map_err(|e| Error::Stun {
            server: "0.0.0.0:0".parse().unwrap_or_else(|_| unreachable!()),
            reason: format!("DNS lookup failed for {stun_server}: {e}"),
        })?
        .next()
        .ok_or_else(|| Error::Stun {
            server: "0.0.0.0:0".parse().unwrap_or_else(|_| unreachable!()),
            reason: format!("no addresses found for {stun_server}"),
        })?;

    let mut txn_id = [0u8; 12];
    ring::rand::SystemRandom::new()
        .fill(&mut txn_id)
        .map_err(|_| Error::Stun {
            server: server_addr,
            reason: "failed to generate random transaction ID".into(),
        })?;

    let request = build_binding_request(&txn_id);

    socket
        .send_to(&request, server_addr)
        .await
        .map_err(|e| Error::Stun {
            server: server_addr,
            reason: format!("send failed: {e}"),
        })?;

    let mut recv_buf = [0u8; 512];
    let timeout = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        socket.recv_from(&mut recv_buf),
    );

    let (n, _from) = timeout
        .await
        .map_err(|_| Error::Stun {
            server: server_addr,
            reason: "timeout waiting for STUN response".into(),
        })?
        .map_err(|e| Error::Stun {
            server: server_addr,
            reason: format!("recv failed: {e}"),
        })?;

    let srflx = parse_binding_response(&recv_buf[..n], &txn_id).map_err(Error::StunParse)?;

    let local = socket.local_addr()?;
    debug!(local = %local, srflx = %srflx, "STUN binding discovered");

    Ok(srflx)
}

pub async fn send_probes(socket: &UdpSocket, remote_addr: SocketAddr, duration_ms: u64) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(duration_ms);

    trace!(remote = %remote_addr, "sending hole-punch probes");

    while tokio::time::Instant::now() < deadline {
        let _ = socket.send_to(&HOLE_PUNCH_MAGIC, remote_addr).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    debug!(remote = %remote_addr, duration_ms, "hole-punch probes complete");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_request_has_correct_magic() {
        let txn = [1u8; 12];
        let req = build_binding_request(&txn);
        assert_eq!(req[0..2], STUN_BINDING_REQUEST.to_be_bytes());
        assert_eq!(req[2..4], [0, 0]);
        assert_eq!(
            u32::from_be_bytes([req[4], req[5], req[6], req[7]]),
            STUN_MAGIC
        );
        assert_eq!(&req[8..20], &txn);
    }

    #[test]
    fn parse_xor_mapped_ipv4() {
        let txn = [0u8; 12];
        let port: u16 = 12345;
        let ip: u32 = u32::from(std::net::Ipv4Addr::new(203, 0, 113, 5));
        let xor_port = port ^ (STUN_MAGIC >> 16) as u16;
        let xor_ip = ip ^ STUN_MAGIC;

        let mut attr = [0u8; 8];
        attr[1] = 0x01;
        attr[2..4].copy_from_slice(&xor_port.to_be_bytes());
        attr[4..8].copy_from_slice(&xor_ip.to_be_bytes());

        let result = parse_xor_mapped_address(&attr, &txn).unwrap();
        assert_eq!(result.port(), port);
        match result.ip() {
            std::net::IpAddr::V4(v4) => assert_eq!(v4, std::net::Ipv4Addr::new(203, 0, 113, 5)),
            _ => panic!("expected IPv4"),
        }
    }

    #[test]
    fn parse_rejects_short_attr() {
        let txn = [0u8; 12];
        assert!(parse_xor_mapped_address(&[0u8; 4], &txn).is_err());
    }

    #[test]
    fn parse_full_binding_response() {
        let txn = [0xAA; 12];
        let port: u16 = 8080;
        let ip = std::net::Ipv4Addr::new(192, 168, 1, 100);
        let ip_u32 = u32::from(ip);
        let xor_port = port ^ (STUN_MAGIC >> 16) as u16;
        let xor_ip = ip_u32 ^ STUN_MAGIC;

        let mut response = Vec::new();
        response.extend_from_slice(&STUN_BINDING_RESPONSE.to_be_bytes());

        let mut attr_buf = Vec::new();
        attr_buf.extend_from_slice(&ATTR_XOR_MAPPED_ADDRESS.to_be_bytes());
        attr_buf.extend_from_slice(&8u16.to_be_bytes());
        attr_buf.push(0);
        attr_buf.push(0x01);
        attr_buf.extend_from_slice(&xor_port.to_be_bytes());
        attr_buf.extend_from_slice(&xor_ip.to_be_bytes());

        let msg_len = attr_buf.len() as u16;
        response.extend_from_slice(&msg_len.to_be_bytes());
        response.extend_from_slice(&STUN_MAGIC.to_be_bytes());
        response.extend_from_slice(&txn);
        response.extend_from_slice(&attr_buf);

        let result = parse_binding_response(&response, &txn).unwrap();
        assert_eq!(result.port(), port);
        assert_eq!(result.ip(), std::net::IpAddr::V4(ip));
    }
}
