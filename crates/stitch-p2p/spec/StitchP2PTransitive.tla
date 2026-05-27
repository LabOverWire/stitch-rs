---- MODULE StitchP2PTransitive ----
\* Transitive forwarding: a peer learns an origin's writes through an
\* intermediary, never connecting to the origin directly. Models a fixed sync
\* topology (Links) so we can force C to depend on B for A's writes.
\*
\* Key design rule under test: a peer applies a given origin's writes strictly
\* in order (single integer cursor per origin). A peer can only serve origin's
\* write #k to a neighbor once it has applied #k itself. So every peer's view of
\* an origin is a contiguous PREFIX of that origin's true log — no gaps, no
\* reordering. We verify this prefix property AND end-state convergence.

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Peers,
    Values,
    MaxLogLen,
    Links,           \* set of unordered {p, q} pairs allowed to sync directly
    Writers,         \* subset of Peers permitted to originate writes
    NoPeer,
    NoValue

ASSUME MaxLogLen \in Nat
ASSUME NoPeer \notin Peers

VARIABLES
    truelog,         \* truelog[o] = origin o's own write log (only o appends)
    state,           \* state[p] = current value of the single record
    seen             \* seen[p][o] = count of o's writes p has applied (a prefix length)

vars == <<truelog, state, seen>>

OpWrite == "write"
OpDelete == "delete"

InitSentinel == [seq |-> 0, peer |-> NoPeer, value |-> NoValue, op |-> OpWrite]

LT(a, b) ==
    \/ a.seq < b.seq
    \/ /\ a.seq = b.seq
       /\ a.peer < b.peer

Linked(p, q) == {p, q} \in Links

Symmetry == Permutations(Values)

Init ==
    /\ truelog = [o \in Peers |-> <<>>]
    /\ state = [p \in Peers |-> InitSentinel]
    /\ seen = [p \in Peers |-> [o \in Peers |-> 0]]

\* Origin p appends a write to its own log and applies it locally.
Write(p, v) ==
    /\ p \in Writers
    /\ Len(truelog[p]) < MaxLogLen
    /\ LET new_seq == Len(truelog[p]) + 1
           entry == [seq |-> new_seq, peer |-> p, value |-> v, op |-> OpWrite]
       IN /\ truelog' = [truelog EXCEPT ![p] = Append(truelog[p], entry)]
          /\ state' = [state EXCEPT ![p] = IF LT(state[p], entry) THEN entry ELSE state[p]]
          /\ seen' = [seen EXCEPT ![p][p] = Len(truelog[p]) + 1]

Delete(p) ==
    /\ p \in Writers
    /\ Len(truelog[p]) < MaxLogLen
    /\ LET new_seq == Len(truelog[p]) + 1
           entry == [seq |-> new_seq, peer |-> p, value |-> NoValue, op |-> OpDelete]
       IN /\ truelog' = [truelog EXCEPT ![p] = Append(truelog[p], entry)]
          /\ state' = [state EXCEPT ![p] = IF LT(state[p], entry) THEN entry ELSE state[p]]
          /\ seen' = [seen EXCEPT ![p][p] = Len(truelog[p]) + 1]

\* p learns origin o's next write through neighbor q. q must be linked to p,
\* must itself be ahead of p on o, and p applies o's writes in order.
\* The forwarded bytes are o's genuine write (faithful QUIC forwarding), modeled
\* by reading truelog[o] at the next index.
Sync(p, q, o) ==
    /\ p # q
    /\ Linked(p, q)
    /\ seen[q][o] > seen[p][o]
    /\ LET entry == truelog[o][seen[p][o] + 1]
       IN /\ state' = [state EXCEPT ![p] = IF LT(state[p], entry) THEN entry ELSE state[p]]
          /\ seen' = [seen EXCEPT ![p][o] = seen[p][o] + 1]
          /\ UNCHANGED truelog

Next ==
    \/ \E p \in Peers, v \in Values: Write(p, v)
    \/ \E p \in Peers: Delete(p)
    \/ \E p, q, o \in Peers: Sync(p, q, o)

Spec == Init /\ [][Next]_vars

\* ---------- Invariants ----------

TypeOK ==
    /\ truelog \in [Peers -> Seq([seq: Nat, peer: Peers \cup {NoPeer},
                                  value: Values \cup {NoValue},
                                  op: {OpWrite, OpDelete}])]
    /\ state \in [Peers -> [seq: Nat, peer: Peers \cup {NoPeer},
                            value: Values \cup {NoValue},
                            op: {OpWrite, OpDelete}]]
    /\ seen \in [Peers -> [Peers -> Nat]]

\* A peer never claims to have seen more of an origin than the origin produced.
InvPrefixBounded ==
    \A p, o \in Peers: seen[p][o] <= Len(truelog[o])

\* Convergence: when every peer has applied every origin's full log, agree.
FullySynced(p) == \A o \in Peers: seen[p][o] = Len(truelog[o])

InvConvergence ==
    \A p, q \in Peers:
        (FullySynced(p) /\ FullySynced(q)) => state[p] = state[q]

\* Reachability probe (negation trick). Expect this to be VIOLATED: a violation
\* proves peer 3 acquired peer 1's write despite 1 and 3 not being linked, i.e.
\* transitive delivery through peer 2 actually happens and convergence is not
\* checked vacuously.
InvProbeNoTransitiveDelivery ==
    ~(seen[3][1] > 0 /\ Len(truelog[1]) > 0)
====
