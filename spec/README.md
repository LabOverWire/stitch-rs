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
| `StitchP2PTransitive.tla` | Fixed sync topology; a peer learns an origin's writes only through an intermediary | `InvConvergence`, `InvPrefixBounded` |
| `StitchP2PAuth.tla` | Membership authorization over eventually-consistent membership; `Causal` toggles reject-at-receipt vs read-time filter | `InvConvergence` |

## Results (all full state-space exhaustion, not `limit_reached`)

| Run | Config | States | Outcome |
|---|---|---|---|
| Base LWW | `StitchP2P.cfg` (2 peers) | 1,121 | converges |
| LWW + tombstones | 2 peers | 5,026 | converges |
| LWW + tombstones | 3 peers, MaxLogLen=1 | 10,405 | converges |
| Unsafe GC | `StitchP2PGc_unsafe_small.cfg` | 281 | **convergence violated (resurrection) at depth 6** |
| Safe GC (per-record floor) | `StitchP2PGc_protected_small.cfg` | 533 | converges |
| Adversarial clock skew | `StitchP2PSkew.cfg` | 14,641 | converges |
| Transitive forwarding (line 1—2—3) | `StitchP2PTransitive.cfg` | 1,300 | converges |
| Transitive, single writer × 2 ops | `StitchP2PTransitive_deep.cfg` | 64 | converges |
| Transitive delivery reachable (probe) | `StitchP2PTransitive_probe.cfg` | — | refuted (proves non-vacuity) |
| Membership auth — reject-at-receipt | `StitchP2PAuth_naive.cfg` | 23 | **convergence violated (receive-order dependent)** |
| Membership auth — read-time filter | `StitchP2PAuth_causal.cfg` | 64 | converges |

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

5. **Transitive forwarding converges.** In a line topology where peers 1 and 3
   never connect directly, peer 3 still acquires peer 1's writes through peer 2
   and converges. The enabling rule: a peer applies an origin's writes strictly
   in order (single integer cursor) and serves an origin's write only after
   applying it — so every peer's view of an origin is a contiguous *prefix*,
   never a gapped set. `InvPrefixBounded` + the in-order `Sync` action make gaps
   unconstructible. The probe config refutes "peer 3 never gets peer 1's write,"
   confirming the convergence check is not vacuous.

6. **Membership authorization must be a read-time filter, not a reject.** If a
   peer permanently drops a write because the author isn't *yet* a known member,
   two peers diverge based on whether they received the write or the
   membership grant first (the naive model produces this trace at depth 5). The
   fix: always store validly-signed writes; apply membership as a filter over
   the converged state at read time. Then visibility is a deterministic function
   of (converged data, converged membership) and all peers agree. Signature
   validity *is* checked at receipt — it never changes, so rejecting forgeries
   can't diverge.

## Reproducing

Use the `tla` MCP tools or TLC directly. Reduced constants (single value,
`MaxLogLen=1`, symmetry on Records/Values) keep the safe-GC and skew models
exhaustively checkable; the unsafe model's counterexample appears at depth 6 so
the small config still catches it. Peers are **not** a symmetry set — `LT`
orders them by id.
