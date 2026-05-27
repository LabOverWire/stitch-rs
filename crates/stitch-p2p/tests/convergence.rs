use stitch_p2p::{Applier, Hlc, Op, PEER_ID_LEN, PeerId, Stamp, StampedWrite};

fn peer(n: u8) -> PeerId {
    let mut id = [0u8; PEER_ID_LEN];
    id[0] = n;
    id
}

fn w(seq: u64, p: u8, op: Op, id: &str, data: &[u8]) -> StampedWrite {
    StampedWrite {
        stamp: Stamp::new(Hlc::new(seq, 0), peer(p)),
        op,
        entity: "task".into(),
        id: id.into(),
        data: data.to_vec(),
    }
}

fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut out = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(i);
        for mut tail in permutations(&rest) {
            tail.insert(0, head.clone());
            out.push(tail);
        }
    }
    out
}

fn visible_snapshot(applier: &Applier, ids: &[&str]) -> Vec<Option<Vec<u8>>> {
    ids.iter()
        .map(|id| applier.visible("task", id).map(<[u8]>::to_vec))
        .collect()
}

#[test]
fn all_orderings_of_a_write_set_converge() {
    let writes = vec![
        w(1, 1, Op::Insert, "a", b"a1"),
        w(2, 2, Op::Update, "a", b"a2"),
        w(1, 2, Op::Insert, "b", b"b-from2"),
        w(1, 1, Op::Insert, "b", b"b-from1"),
        w(3, 1, Op::Delete, "a", b""),
    ];
    let ids = ["a", "b"];

    let mut reference: Option<Vec<Option<Vec<u8>>>> = None;
    for order in permutations(&writes) {
        let mut applier = Applier::new();
        for write in order {
            applier.merge(write);
        }
        let snap = visible_snapshot(&applier, &ids);
        match &reference {
            None => reference = Some(snap),
            Some(r) => assert_eq!(&snap, r, "ordering diverged"),
        }
    }

    let r = reference.unwrap();
    assert_eq!(r[0], None, "record a: delete at seq 3 is the LWW winner");
    assert_eq!(
        r[1],
        Some(b"b-from2".to_vec()),
        "record b: peer 2 wins the seq-1 tie"
    );
}

#[test]
fn gc_interleaved_with_stale_write_converges_to_absent() {
    let del = w(2, 2, Op::Delete, "x", b"");
    let stale = w(1, 1, Op::Insert, "x", b"resurrect");

    let mut order_a = Applier::new();
    order_a.merge(del.clone());
    order_a.collect_tombstone("task", "x");
    order_a.merge(stale.clone());

    let mut order_b = Applier::new();
    order_b.merge(stale);
    order_b.merge(del);
    order_b.collect_tombstone("task", "x");

    assert_eq!(order_a.visible("task", "x"), None);
    assert_eq!(order_b.visible("task", "x"), None);
}
