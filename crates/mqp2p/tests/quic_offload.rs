use mqp2p::quic::{QuicEndpoint, SegmentationOffload, generate_self_signed_cert};
use std::net::UdpSocket;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn endpoints_without_segmentation_offload_exchange_data() {
    let id_a = generate_self_signed_cert().expect("cert a");
    let id_b = generate_self_signed_cert().expect("cert b");
    let fp_a = id_a.fingerprint.clone();
    let fp_b = id_b.fingerprint.clone();
    let socket = || UdpSocket::bind("127.0.0.1:0").expect("socket");
    let ep_a = QuicEndpoint::bind_with_offload(socket(), id_a, SegmentationOffload::Disabled)
        .expect("bind a");
    let ep_b = QuicEndpoint::bind_with_offload(socket(), id_b, SegmentationOffload::Disabled)
        .expect("bind b");
    let addr_b = ep_b.local_addr().expect("addr b");
    let payload = vec![0x5a_u8; 64 * 1024];
    let expected = payload.clone();

    let receiver = tokio::spawn(async move {
        let conn = ep_b.accept_with_fingerprint(&fp_a).await.expect("accept");
        let mut recv = conn.accept_uni().await.expect("stream");
        let received = recv.read_to_end(1024 * 1024).await.expect("read");
        conn.close(0u32.into(), b"done");
        received
    });

    let conn = ep_a.connect(addr_b, &fp_b).await.expect("connect");
    let mut send = conn.open_uni().await.expect("open");
    send.write_all(&payload).await.expect("write");
    send.finish().expect("finish");

    assert_eq!(receiver.await.expect("receiver"), expected);
}
