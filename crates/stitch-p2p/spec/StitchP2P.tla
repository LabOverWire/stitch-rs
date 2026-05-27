---- MODULE StitchP2P ----
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Peers,           \* finite set of peer ids, e.g. {1, 2} — must be Nat for total order
    Values,          \* finite set of values a record may take
    MaxLogLen,       \* per-peer log length bound
    NoPeer,          \* sentinel peer id used by the initial state (not in Peers)
    NoValue          \* sentinel value used by the initial state (not in Values)

ASSUME MaxLogLen \in Nat
ASSUME NoPeer \notin Peers

VARIABLES
    log,             \* log[p] = sequence of {seq, peer, value} records by p
    state,           \* state[p] = current value of THE single record (one row model)
    cursor,          \* cursor[p][q] = number of q's log entries that p has applied
    counter          \* counter[p] = peer p's last-used Lamport-style seq

vars == <<log, state, cursor, counter>>

\* Op kinds: "write" carries a Value, "delete" carries NoValue and marks a tombstone.
OpWrite == "write"
OpDelete == "delete"

InitSentinel == [seq |-> 0, peer |-> NoPeer, value |-> NoValue, op |-> OpWrite]

\* Strict total order on (seq, peer). seq breaks first; peer (Nat) breaks ties.
\* The sentinel peer (NoPeer = 0) is strictly less than any real peer id.
LT(a, b) ==
    \/ a.seq < b.seq
    \/ /\ a.seq = b.seq
       /\ a.peer < b.peer

GE(a, b) == ~LT(a, b)

\* ---------- Init ----------

Init ==
    /\ log = [p \in Peers |-> <<>>]
    /\ state = [p \in Peers |-> InitSentinel]
    /\ cursor = [p \in Peers |-> [q \in Peers |-> 0]]
    /\ counter = [p \in Peers |-> 0]

\* ---------- Actions ----------

\* Peer p writes value v locally. Lamport-style: new seq = counter[p] + 1.
\* Applies to local state and advances its own cursor.
Write(p, v) ==
    /\ Len(log[p]) < MaxLogLen
    /\ LET new_seq == counter[p] + 1
           entry == [seq |-> new_seq, peer |-> p, value |-> v, op |-> OpWrite]
       IN /\ log' = [log EXCEPT ![p] = Append(log[p], entry)]
          /\ counter' = [counter EXCEPT ![p] = new_seq]
          /\ state' = [state EXCEPT ![p] = IF LT(state[p], entry) THEN entry ELSE state[p]]
          /\ cursor' = [cursor EXCEPT ![p][p] = Len(log[p]) + 1]

\* Peer p deletes the record. Tombstone is just an entry with op=OpDelete.
\* Like a write, it advances HLC and is applied with the same LWW rule.
Delete(p) ==
    /\ Len(log[p]) < MaxLogLen
    /\ LET new_seq == counter[p] + 1
           entry == [seq |-> new_seq, peer |-> p, value |-> NoValue, op |-> OpDelete]
       IN /\ log' = [log EXCEPT ![p] = Append(log[p], entry)]
          /\ counter' = [counter EXCEPT ![p] = new_seq]
          /\ state' = [state EXCEPT ![p] = IF LT(state[p], entry) THEN entry ELSE state[p]]
          /\ cursor' = [cursor EXCEPT ![p][p] = Len(log[p]) + 1]

\* Peer p applies the next unseen write from peer q.
\* Standard Lamport receive: counter[p] = max(counter[p], entry.seq).
\* State updates only if the incoming entry is greater under LT.
Receive(p, q) ==
    /\ p # q
    /\ cursor[p][q] < Len(log[q])
    /\ LET entry == log[q][cursor[p][q] + 1]
       IN /\ cursor' = [cursor EXCEPT ![p][q] = cursor[p][q] + 1]
          /\ counter' = [counter EXCEPT ![p] =
                            IF entry.seq > counter[p] THEN entry.seq ELSE counter[p]]
          /\ state' = [state EXCEPT ![p] = IF LT(state[p], entry) THEN entry ELSE state[p]]
          /\ UNCHANGED log

Next ==
    \/ \E p \in Peers, v \in Values: Write(p, v)
    \/ \E p \in Peers: Delete(p)
    \/ \E p, q \in Peers: Receive(p, q)

Spec == Init /\ [][Next]_vars

\* ---------- Invariants ----------

TypeOK ==
    /\ log \in [Peers -> Seq([seq: Nat, peer: Peers \cup {NoPeer},
                              value: Values \cup {NoValue},
                              op: {OpWrite, OpDelete}])]
    /\ state \in [Peers -> [seq: Nat, peer: Peers \cup {NoPeer},
                            value: Values \cup {NoValue},
                            op: {OpWrite, OpDelete}]]
    /\ cursor \in [Peers -> [Peers -> Nat]]
    /\ counter \in [Peers -> Nat]

\* Each peer's own log is strictly increasing in seq.
InvLogMonotonic ==
    \A p \in Peers:
        \A i, j \in 1..Len(log[p]):
            i < j => log[p][i].seq < log[p][j].seq

\* When peer p has applied every write from every peer q (cursor[p][q] = Len(log[q])),
\* p's state equals the LWW max of all writes. Two such peers must agree.
FullySynced(p) == \A q \in Peers: cursor[p][q] = Len(log[q])

InvConvergence ==
    \A p, q \in Peers:
        (FullySynced(p) /\ FullySynced(q)) => state[p] = state[q]

\* p's state is >= every write p has applied (under LT). Catch state-going-backwards bugs.
InvStateIsApplied ==
    \A p \in Peers:
        \A q \in Peers:
            \A i \in 1..cursor[p][q]:
                GE(state[p], log[q][i])

====
