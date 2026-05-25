# TLA+ specs for stitch-p2p

Formal models of the multi-leader peer-to-peer sync core, checked with TLC.
These specs are the *reason the implementation is shaped the way it is* —
particularly the tombstone GC design.

## Models

| File | Models | Key invariant |
|---|---|---|
| `StitchP2P.tla` | LWW with Lamport counter + peer tiebreak, writes + deletes | `InvConvergence`, `InvStateIsApplied`, `InvLogMonotonic` |
| `StitchP2PGc.tla` | Multi-record + tombstone GC, `Protected` flag toggles the gc_floor guard | `InvVisibleConvergence` |
| `StitchP2PSkew.tla` | Adversarial clock — every write picks an arbitrary seq | `InvConvergence`, `InvStateIsApplied` |

## Results (all full state-space exhaustion, not `limit_reached`)

| Run | Config | States | Outcome |
|---|---|---|---|
| Base LWW | `StitchP2P.cfg` (2 peers) | 1,121 | converges |
| LWW + tombstones | 2 peers | 5,026 | converges |
| LWW + tombstones | 3 peers, MaxLogLen=1 | 10,405 | converges |
| Unsafe GC | `StitchP2PGc_unsafe_small.cfg` | 281 | **convergence violated (resurrection) at depth 6** |
| Safe GC (per-record floor) | `StitchP2PGc_protected_small.cfg` | 533 | converges |
| Adversarial clock skew | `StitchP2PSkew.cfg` | 14,641 | converges |

## What they establish

1. **Core LWW converges.** Multi-leader writes resolved by `(seq, peer)` total
   order — any two peers that exchange all writes agree, in any interleaving.

2. **Naive tombstone GC breaks convergence.** A peer that deletes a record, GCs
   the tombstone, then receives an older concurrent write *resurrects* the row.
   The unsafe model produces this exact counterexample.

3. **The fix is a per-(peer, record) delete high-water mark.** Keep
   `gc_floor[record] = (seq, peer)` of the GC'd tombstone and reject any incoming
   write that loses to it under LWW. A *global* per-peer floor is wrong — it
   over-rejects unrelated records (the protected model with a scalar floor still
   violated convergence; the per-record floor does not).

4. **Clock skew cannot fork state.** Even when every write carries an arbitrary
   seq, convergence holds. HLC future-bounds are a griefing mitigation, not a
   correctness requirement.

## Reproducing

Use the `tla` MCP tools or TLC directly. Reduced constants (single value,
`MaxLogLen=1`, symmetry on Records/Values) keep the safe-GC and skew models
exhaustively checkable; the unsafe model's counterexample appears at depth 6 so
the small config still catches it. Peers are **not** a symmetry set — `LT`
orders them by id.
