---- MODULE StitchP2PSkew ----
\* Adversarial-clock model: every write picks an ARBITRARY seq (not counter+1).
\* This is the worst case for HLC skew / a malicious peer inflating its clock.
\* If convergence holds here, it holds when some peers are honest.

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Peers,
    Values,
    MaxLogLen,
    MaxSeq,          \* largest seq any write may carry
    NoPeer,
    NoValue

ASSUME MaxLogLen \in Nat
ASSUME MaxSeq \in Nat
ASSUME NoPeer \notin Peers

VARIABLES
    log,             \* log[p] = sequence of {seq, peer, value, op}
    state,           \* state[p] = current value of the single record
    cursor           \* cursor[p][q] = number of q's entries p has applied

vars == <<log, state, cursor>>

OpWrite == "write"
OpDelete == "delete"

InitSentinel == [seq |-> 0, peer |-> NoPeer, value |-> NoValue, op |-> OpWrite]

LT(a, b) ==
    \/ a.seq < b.seq
    \/ /\ a.seq = b.seq
       /\ a.peer < b.peer

GE(a, b) == ~LT(a, b)

Symmetry == Permutations(Values)

Init ==
    /\ log = [p \in Peers |-> <<>>]
    /\ state = [p \in Peers |-> InitSentinel]
    /\ cursor = [p \in Peers |-> [q \in Peers |-> 0]]

\* Adversarial write: seq chosen arbitrarily in 1..MaxSeq.
Write(p, v, s) ==
    /\ Len(log[p]) < MaxLogLen
    /\ LET entry == [seq |-> s, peer |-> p, value |-> v, op |-> OpWrite]
       IN /\ log' = [log EXCEPT ![p] = Append(log[p], entry)]
          /\ state' = [state EXCEPT ![p] = IF LT(state[p], entry) THEN entry ELSE state[p]]
          /\ cursor' = [cursor EXCEPT ![p][p] = Len(log[p]) + 1]

\* Adversarial delete: seq chosen arbitrarily in 1..MaxSeq.
Delete(p, s) ==
    /\ Len(log[p]) < MaxLogLen
    /\ LET entry == [seq |-> s, peer |-> p, value |-> NoValue, op |-> OpDelete]
       IN /\ log' = [log EXCEPT ![p] = Append(log[p], entry)]
          /\ state' = [state EXCEPT ![p] = IF LT(state[p], entry) THEN entry ELSE state[p]]
          /\ cursor' = [cursor EXCEPT ![p][p] = Len(log[p]) + 1]

\* Apply q's next unseen entry, in log order. No counter to maintain.
Receive(p, q) ==
    /\ p # q
    /\ cursor[p][q] < Len(log[q])
    /\ LET entry == log[q][cursor[p][q] + 1]
       IN /\ cursor' = [cursor EXCEPT ![p][q] = cursor[p][q] + 1]
          /\ state' = [state EXCEPT ![p] = IF LT(state[p], entry) THEN entry ELSE state[p]]
          /\ UNCHANGED log

Next ==
    \/ \E p \in Peers, v \in Values, s \in 1..MaxSeq: Write(p, v, s)
    \/ \E p \in Peers, s \in 1..MaxSeq: Delete(p, s)
    \/ \E p, q \in Peers: Receive(p, q)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ log \in [Peers -> Seq([seq: Nat, peer: Peers \cup {NoPeer},
                              value: Values \cup {NoValue},
                              op: {OpWrite, OpDelete}])]
    /\ state \in [Peers -> [seq: Nat, peer: Peers \cup {NoPeer},
                            value: Values \cup {NoValue},
                            op: {OpWrite, OpDelete}]]
    /\ cursor \in [Peers -> [Peers -> Nat]]

FullySynced(p) == \A q \in Peers: cursor[p][q] = Len(log[q])

\* Even with arbitrary seqs, two fully-synced peers must agree. This is the
\* claim that LWW convergence is robust to clock skew (worst case: griefing).
InvConvergence ==
    \A p, q \in Peers:
        (FullySynced(p) /\ FullySynced(q)) => state[p] = state[q]

\* State is >= every entry applied (under LT). Holds even with duplicate keys.
InvStateIsApplied ==
    \A p \in Peers:
        \A q \in Peers:
            \A i \in 1..cursor[p][q]:
                GE(state[p], log[q][i])
====
