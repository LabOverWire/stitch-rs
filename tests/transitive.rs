use stitch_p2p::{Op, PEER_ID_LEN, PeerId, SyncState};

fn peer(n: u8) -> PeerId {
    let mut id = [0u8; PEER_ID_LEN];
    id[0] = n;
    id
}

/// `into` pulls everything `from` has that `into` hasn't seen, applying in order.
fn pull(into: &mut SyncState, from: &SyncState, wall: u64) {
    let cursors = into.cursors();
    for frame in from.delta_since(&cursors) {
        into.receive(frame, wall);
    }
}

/// Line topology 1—2—3: peers 1 and 3 never exchange directly. Mirrors
/// `spec/StitchP2PTransitive.tla`. Confirms peer 1's write reaches peer 3 (and
/// vice versa) only through peer 2, and all three converge.
#[test]
fn line_topology_converges_transitively() {
    let mut p1 = SyncState::new(peer(1));
    let mut p2 = SyncState::new(peer(2));
    let mut p3 = SyncState::new(peer(3));

    p1.local_write(10, Op::Insert, "task", "t1", b"from1".to_vec());
    p2.local_write(11, Op::Insert, "task", "t2", b"from2".to_vec());
    p3.local_write(12, Op::Insert, "task", "t3", b"from3".to_vec());

    // Gossip only along the links 1—2 and 2—3. Two rounds is enough for a
    // 3-node line: round 1 fills peer 2, round 2 spreads to the ends.
    for round in 0..2 {
        let wall = 100 + round;
        pull(&mut p2, &p1, wall);
        pull(&mut p2, &p3, wall);
        pull(&mut p1, &p2, wall);
        pull(&mut p3, &p2, wall);
    }

    for (label, s) in [("p1", &p1), ("p2", &p2), ("p3", &p3)] {
        assert_eq!(s.visible("task", "t1"), Some(&b"from1"[..]), "{label} t1");
        assert_eq!(s.visible("task", "t2"), Some(&b"from2"[..]), "{label} t2");
        assert_eq!(s.visible("task", "t3"), Some(&b"from3"[..]), "{label} t3");
    }
}

/// Concurrent edits to the same record across the line resolve identically on
/// every peer (LWW with peer-id tiebreak), regardless of propagation path.
#[test]
fn concurrent_same_record_edits_converge_across_line() {
    let mut p1 = SyncState::new(peer(1));
    let mut p2 = SyncState::new(peer(2));
    let mut p3 = SyncState::new(peer(3));

    // p1 and p3 both write "shared" at the same wall time, never having seen
    // each other. Peer-id tiebreak must pick the same winner everywhere.
    p1.local_write(50, Op::Insert, "task", "shared", b"by1".to_vec());
    p3.local_write(50, Op::Insert, "task", "shared", b"by3".to_vec());

    for round in 0..3 {
        let wall = 200 + round;
        pull(&mut p2, &p1, wall);
        pull(&mut p2, &p3, wall);
        pull(&mut p1, &p2, wall);
        pull(&mut p3, &p2, wall);
    }

    let w1 = p1.visible("task", "shared").map(<[u8]>::to_vec);
    let w2 = p2.visible("task", "shared").map(<[u8]>::to_vec);
    let w3 = p3.visible("task", "shared").map(<[u8]>::to_vec);
    assert_eq!(w1, w2);
    assert_eq!(w2, w3);
    assert!(w1.is_some());
}

/// A delete originated at one end propagates across the line and wins over the
/// earlier insert everywhere.
#[test]
fn delete_propagates_across_line() {
    let mut p1 = SyncState::new(peer(1));
    let mut p2 = SyncState::new(peer(2));
    let mut p3 = SyncState::new(peer(3));

    p1.local_write(10, Op::Insert, "task", "t1", b"alive".to_vec());
    for round in 0..2 {
        pull(&mut p2, &p1, 100 + round);
        pull(&mut p3, &p2, 100 + round);
    }
    assert_eq!(p3.visible("task", "t1"), Some(&b"alive"[..]));

    // p3 deletes; the tombstone travels back up the line to p1.
    p3.local_write(200, Op::Delete, "task", "t1", Vec::new());
    for round in 0..2 {
        pull(&mut p2, &p3, 300 + round);
        pull(&mut p1, &p2, 300 + round);
    }

    assert_eq!(p1.visible("task", "t1"), None);
    assert_eq!(p2.visible("task", "t1"), None);
    assert_eq!(p3.visible("task", "t1"), None);
}
