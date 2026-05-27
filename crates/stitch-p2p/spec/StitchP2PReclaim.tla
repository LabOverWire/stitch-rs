---- MODULE StitchP2PReclaim ----
\* When is it safe to RECLAIM a tombstone — drop the per-record gc_floor that
\* keeps a deleted record from being resurrected by an older, still-in-flight
\* write?
\*
\* Setup: record r has an older write S (logically before the delete) and a
\* tombstone T (the delete). The gc_floor, once T is applied, blocks S from
\* resurrecting r. Reclaiming drops that floor. If a peer reclaims while a copy
\* of S is still undelivered to it, a late S then resurrects r — divergence.
\*
\* RequireLwm = FALSE: reclaim as soon as the peer has T (naive).  -> expect bug.
\* RequireLwm = TRUE : reclaim only once EVERY peer has S (low-water-mark, i.e.
\*                     everyone has delivered everything below T's HLC). -> safe.

EXTENDS Naturals

CONSTANTS
    Peers,
    RequireLwm   \* gate reclaim on the cursor low-water-mark

ASSUME RequireLwm \in BOOLEAN

VARIABLES
    gotS,        \* gotS[p] : p has delivered the older write S
    gotT,        \* gotT[p] : p has delivered the tombstone T
    floor,       \* floor[p]: the gc_floor is active (blocks S)
    present      \* present[p]: r is currently visible (resurrected by S)

vars == <<gotS, gotT, floor, present>>

Init ==
    /\ gotS = [p \in Peers |-> FALSE]
    /\ gotT = [p \in Peers |-> FALSE]
    /\ floor = [p \in Peers |-> FALSE]
    /\ present = [p \in Peers |-> FALSE]

\* Deliver the older write S. Blocked iff the gc_floor is active; otherwise it
\* becomes the current value (present), which is the resurrection if the floor
\* was already reclaimed.
DeliverS(p) ==
    /\ ~gotS[p]
    /\ gotS' = [gotS EXCEPT ![p] = TRUE]
    /\ present' = [present EXCEPT ![p] = IF floor[p] THEN present[p] ELSE TRUE]
    /\ UNCHANGED <<gotT, floor>>

\* Deliver the tombstone T: r is deleted and the tombstone is collected into the
\* gc_floor.
DeliverT(p) ==
    /\ ~gotT[p]
    /\ gotT' = [gotT EXCEPT ![p] = TRUE]
    /\ floor' = [floor EXCEPT ![p] = TRUE]
    /\ present' = [present EXCEPT ![p] = FALSE]
    /\ UNCHANGED gotS

\* Reclaim: forget the tombstone floor. Safe only once the low-water-mark
\* guarantees no older write is still in flight.
Reclaimable(p) ==
    /\ gotT[p]
    /\ floor[p]
    /\ (RequireLwm => (\A q \in Peers: gotS[q]))

Reclaim(p) ==
    /\ Reclaimable(p)
    /\ floor' = [floor EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<gotS, gotT, present>>

Next ==
    \/ \E p \in Peers: DeliverS(p)
    \/ \E p \in Peers: DeliverT(p)
    \/ \E p \in Peers: Reclaim(p)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ gotS \in [Peers -> BOOLEAN]
    /\ gotT \in [Peers -> BOOLEAN]
    /\ floor \in [Peers -> BOOLEAN]
    /\ present \in [Peers -> BOOLEAN]

FullyDelivered(p) == gotS[p] /\ gotT[p]

\* Once every relevant write has reached two peers, they must agree on whether r
\* is present. Correct converged answer is absent (the delete wins).
InvConvergence ==
    \A p, q \in Peers:
        (FullyDelivered(p) /\ FullyDelivered(q)) => present[p] = present[q]
====
