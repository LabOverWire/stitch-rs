# stitch-tasks

A collaborative task board on [stitch-p2p](../stitch-p2p), plus a chaos/soak
harness that exercises the whole sync stack end to end.

## The app

`TaskBoard` is a thin typed layer over `stitch_p2p::Store`: tasks are JSON
documents (`{title, done}`) in the `task` entity. `add` / `rename` / `set_done` /
`remove` map onto the Store's CRUD, so the board inherits the engine's
properties — concurrent edits resolve last-writer-wins, deletes are tombstones,
and owner-controlled membership gates which peers' tasks are visible.

## The exerciser

`harness::Cluster` runs N in-process peers over the **real session protocol**
(duplex pipes, not QUIC — chosen for deterministic workloads, speed, and precise
partition injection; the QUIC/discovery path is covered by
`stitch-p2p/tests/discovery_broker.rs`). Peer 0 owns the project; the rest join
and are invited.

`harness::run_chaos` drives a seeded random workload — concurrent, collision-
prone ops on a shared id pool — while partitioning and healing links and
churning membership (revokes), then heals everything and **asserts every peer
converges to the identical board**. This is the empirical complement to the
TLA+ models in `stitch-p2p/spec/`: the models verify small state spaces
exhaustively; this stresses the real engine under randomized timing and churn.

### Run it

```
# regression soak (cargo test)
cargo test -p stitch-tasks

# ad-hoc, configurable: peers rounds seed
cargo run --release --bin soak -- 6 3000 7
```

Example output:

```
soak: 6 peers, 3000 rounds, seed 7 ...
  ops:        add 791 / rename 768 / toggle 700 / remove 741
  chaos:      235 partitions, 199 heals, 77 revokes
  final board: 4 tasks
  elapsed:    10.01s
  CONVERGED ✓
```

What this exercises in one run: signed writes, membership authorization
(invited members' tasks visible, revoked peers' hidden), HLC last-writer-wins on
colliding edits, tombstones, anti-entropy catch-up after partitions, and
transitive propagation through the mesh.
