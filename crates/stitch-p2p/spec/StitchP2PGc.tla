---- MODULE StitchP2PGc ----
\* Multi-record extension of StitchP2P modeling tombstone GC.
\* The question: can a peer that GC'd a tombstone be "resurrected" by a stale
\* write from a peer that was offline during the delete?

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Peers,
    Records,         \* finite set of record ids, e.g. {r1, r2}
    Values,
    MaxLogLen,
    NoPeer,
    NoValue,
    NoRecord,
    Protected        \* TRUE = enforce gc_floor; FALSE = unsafe GC

ASSUME MaxLogLen \in Nat
ASSUME NoPeer \notin Peers
ASSUME NoRecord \notin Records
ASSUME Protected \in BOOLEAN

VARIABLES
    log,             \* log[p] = Seq of {seq, peer, rid, value, op}
    state,           \* state[p][rid] = entry (or InitSentinel for "absent")
    cursor,
    counter,
    gc_floor         \* gc_floor[p][r] = LT-max tombstone p has GC'd for record r (or sentinel)

vars == <<log, state, cursor, counter, gc_floor>>

OpWrite == "write"
OpDelete == "delete"

InitSentinel == [seq |-> 0, peer |-> NoPeer, rid |-> NoRecord,
                 value |-> NoValue, op |-> OpWrite]

LT(a, b) ==
    \/ a.seq < b.seq
    \/ /\ a.seq = b.seq
       /\ a.peer < b.peer

GE(a, b) == ~LT(a, b)

\* Records and Values are compared only by equality, never ordered, so they are
\* interchangeable model values. Peers are NOT (LT orders them by id).
Symmetry == Permutations(Records) \cup Permutations(Values)

Init ==
    /\ log = [p \in Peers |-> <<>>]
    /\ state = [p \in Peers |-> [r \in Records |-> InitSentinel]]
    /\ cursor = [p \in Peers |-> [q \in Peers |-> 0]]
    /\ counter = [p \in Peers |-> 0]
    /\ gc_floor = [p \in Peers |-> [r \in Records |-> InitSentinel]]

\* A peer writes a value to a specific record.
Write(p, r, v) ==
    /\ Len(log[p]) < MaxLogLen
    /\ LET new_seq == counter[p] + 1
           entry == [seq |-> new_seq, peer |-> p, rid |-> r,
                     value |-> v, op |-> OpWrite]
       IN /\ log' = [log EXCEPT ![p] = Append(log[p], entry)]
          /\ counter' = [counter EXCEPT ![p] = new_seq]
          /\ state' = [state EXCEPT ![p][r] = IF LT(state[p][r], entry)
                                              THEN entry ELSE state[p][r]]
          /\ cursor' = [cursor EXCEPT ![p][p] = Len(log[p]) + 1]
          /\ UNCHANGED gc_floor

\* A peer deletes a record. Tombstone is just an entry with op=OpDelete.
Delete(p, r) ==
    /\ Len(log[p]) < MaxLogLen
    /\ LET new_seq == counter[p] + 1
           entry == [seq |-> new_seq, peer |-> p, rid |-> r,
                     value |-> NoValue, op |-> OpDelete]
       IN /\ log' = [log EXCEPT ![p] = Append(log[p], entry)]
          /\ counter' = [counter EXCEPT ![p] = new_seq]
          /\ state' = [state EXCEPT ![p][r] = IF LT(state[p][r], entry)
                                              THEN entry ELSE state[p][r]]
          /\ cursor' = [cursor EXCEPT ![p][p] = Len(log[p]) + 1]
          /\ UNCHANGED gc_floor

\* Receive q's next unseen entry.
\* Protected mode: reject when entry would be losing to a GC'd tombstone for
\* the same record (full LT comparison, per-record floor).
Receive(p, q) ==
    /\ p # q
    /\ cursor[p][q] < Len(log[q])
    /\ LET entry == log[q][cursor[p][q] + 1]
           floor == gc_floor[p][entry.rid]
           reject == Protected /\ ~LT(floor, entry)
       IN /\ cursor' = [cursor EXCEPT ![p][q] = cursor[p][q] + 1]
          /\ counter' = [counter EXCEPT ![p] =
                            IF entry.seq > counter[p] THEN entry.seq
                            ELSE counter[p]]
          /\ IF reject
             THEN /\ UNCHANGED state
                  /\ UNCHANGED gc_floor
             ELSE /\ state' = [state EXCEPT ![p][entry.rid] =
                                  IF LT(state[p][entry.rid], entry)
                                  THEN entry ELSE state[p][entry.rid]]
                  /\ UNCHANGED gc_floor
          /\ UNCHANGED log

\* GC a tombstone: forget it from state. Bumps gc_floor (only enforced
\* in Protected mode).
Gc(p, r) ==
    /\ state[p][r].op = OpDelete
    /\ gc_floor' = [gc_floor EXCEPT ![p][r] =
                       IF LT(gc_floor[p][r], state[p][r])
                       THEN state[p][r] ELSE gc_floor[p][r]]
    /\ state' = [state EXCEPT ![p][r] = InitSentinel]
    /\ UNCHANGED <<log, cursor, counter>>

Next ==
    \/ \E p \in Peers, r \in Records, v \in Values: Write(p, r, v)
    \/ \E p \in Peers, r \in Records: Delete(p, r)
    \/ \E p, q \in Peers: Receive(p, q)
    \/ \E p \in Peers, r \in Records: Gc(p, r)

Spec == Init /\ [][Next]_vars

\* ---------- Invariants ----------

TypeOK ==
    /\ log \in [Peers -> Seq([seq: Nat, peer: Peers \cup {NoPeer},
                              rid: Records \cup {NoRecord},
                              value: Values \cup {NoValue},
                              op: {OpWrite, OpDelete}])]
    /\ state \in [Peers -> [Records -> [seq: Nat,
                                        peer: Peers \cup {NoPeer},
                                        rid: Records \cup {NoRecord},
                                        value: Values \cup {NoValue},
                                        op: {OpWrite, OpDelete}]]]
    /\ cursor \in [Peers -> [Peers -> Nat]]
    /\ counter \in [Peers -> Nat]
    /\ gc_floor \in [Peers -> [Records -> [seq: Nat,
                                           peer: Peers \cup {NoPeer},
                                           rid: Records \cup {NoRecord},
                                           value: Values \cup {NoValue},
                                           op: {OpWrite, OpDelete}]]]

FullySynced(p) == \A q \in Peers: cursor[p][q] = Len(log[q])

\* Observable state: "absent" if sentinel (never seen, or GC'd) OR if current is
\* a tombstone. Otherwise the value the user wrote.
\* This is what the application sees through Store::read.
Absent(s) == s.op = OpDelete \/ s.seq = 0
Visible(s) == IF Absent(s) THEN "absent" ELSE s.value

\* Convergence on observable state. Two fully-synced peers must agree on what
\* their applications would see for each record.
InvVisibleConvergence ==
    \A p, q \in Peers:
        (FullySynced(p) /\ FullySynced(q)) =>
            \A r \in Records: Visible(state[p][r]) = Visible(state[q][r])

\* Strong convergence (raw state matches). Holds without GC, fails with
\* uncoordinated GC. Kept for comparison.
InvConvergence ==
    \A p, q \in Peers:
        (FullySynced(p) /\ FullySynced(q)) =>
            \A r \in Records: state[p][r] = state[q][r]
====
