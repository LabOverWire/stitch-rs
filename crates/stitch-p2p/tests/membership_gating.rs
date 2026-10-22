#![cfg(all(feature = "store", feature = "membership", feature = "discovery"))]
//! Membership gating for device pairing (#56): the transport key a device
//! presents equals its membership peer id, so a host can admit only invited
//! devices, reject outsiders, and learn when a connected device is revoked.

use serde_json::json;
use std::time::Duration;
use stitch_p2p::membership::Role;
use stitch_p2p::{Identity, Store, session};
use tokio::io::{DuplexStream, split};
use tokio::task::JoinHandle;

const PULL: Duration = Duration::from_millis(20);

fn connect(a: &Store, a_io: DuplexStream, b: &Store, b_io: DuplexStream) -> Vec<JoinHandle<()>> {
    let rx_a = a.node().register_session();
    let (ar, aw) = split(a_io);
    let sa = a.node().state();
    let rx_b = b.node().register_session();
    let (br, bw) = split(b_io);
    let sb = b.node().state();
    vec![
        tokio::spawn(async move {
            let _ = session::run(sa, ar, aw, rx_a, PULL).await;
        }),
        tokio::spawn(async move {
            let _ = session::run(sb, br, bw, rx_b, PULL).await;
        }),
    ]
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

#[test]
fn a_membership_peer_id_equals_its_transport_key() {
    let identity = Identity::from_seed([42u8; 32]);
    let cert = mqp2p::quic::cert_from_ed25519_seed(&identity.seed()).expect("cert from seed");
    let transport_key = mqp2p::quic::public_key_from_cert(&cert.cert_der).expect("transport key");
    assert_eq!(transport_key, identity.peer_id());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_admits_members_rejects_outsiders_and_sees_revocation() {
    let owner_id = Identity::from_seed([1u8; 32]);
    let owner_pid = owner_id.peer_id();
    let member_id = Identity::from_seed([2u8; 32]);
    let member_pid = member_id.peer_id();
    let outsider_pid = Identity::from_seed([3u8; 32]).peer_id();

    let owner = Store::with_owner(owner_id);
    let member = Store::join(member_id, owner_pid);

    owner.invite(member_pid, Role::Member).await.expect("write");
    member.create("task", "m", json!({"x": 1})).await.unwrap();

    let (a_io, b_io) = tokio::io::duplex(64 * 1024);
    let handles = connect(&owner, a_io, &member, b_io);

    let host = owner.membership().expect("owner has a membership view");

    assert!(
        host.is_member(&member_pid).await,
        "an invited device is an authorized member"
    );
    assert!(
        !host.is_member(&outsider_pid).await,
        "an uninvited device is rejected"
    );

    let mut changes = host.subscribe().await;
    owner.revoke(member_pid).await.expect("write");
    changes
        .changed()
        .await
        .expect("membership change signalled");
    assert!(
        !host.is_member(&member_pid).await,
        "a revoked device is reported as no longer a member"
    );

    assert!(
        wait_until(Duration::from_secs(5), || async {
            !member
                .membership()
                .expect("member view")
                .is_member(&member_pid)
                .await
        })
        .await,
        "revocation converges to the device's own view"
    );

    for h in &handles {
        h.abort();
    }
}
