---- MODULE SessionPull ----
EXTENDS Naturals, FiniteSets

CONSTANTS Peers, Writers, MaxSeq, Mode

ASSUME Mode \in {"baseline", "initiator", "naive"}
ASSUME MaxSeq \in Nat
ASSUME Peers \subseteq Nat
ASSUME Writers \subseteq Peers

None == "none"
Vec == [Peers -> 0..MaxSeq]
Pairs == {pq \in Peers \X Peers : pq[1] # pq[2]}

VARIABLES len, cur, hello, delta, push, lastSent

vars == <<len, cur, hello, delta, push, lastSent>>

Zero == [o \in Peers |-> 0]

Max(a, b) == IF a > b THEN a ELSE b

Ahead(a, b) == \E o \in Peers : a[o] > b[o]

Init ==
    /\ len = Zero
    /\ cur = [p \in Peers |-> Zero]
    /\ hello = [p \in Peers |-> [q \in Peers |-> None]]
    /\ delta = [p \in Peers |-> [q \in Peers |-> None]]
    /\ push = [p \in Peers |-> [q \in Peers |-> 0]]
    /\ lastSent = [p \in Peers |-> [q \in Peers |-> Zero]]

Write(o) ==
    /\ len[o] < MaxSeq
    /\ len' = [len EXCEPT ![o] = len[o] + 1]
    /\ cur' = [cur EXCEPT ![o][o] = len[o] + 1]
    /\ push' = [push EXCEPT ![o] = [q \in Peers |-> IF q = o THEN 0 ELSE len[o] + 1]]
    /\ UNCHANGED <<hello, delta, lastSent>>

CanTick(p, q) ==
    CASE Mode = "baseline" -> TRUE
      [] Mode = "initiator" -> p < q
      [] Mode = "naive" -> lastSent[p][q] # cur[p]

Tick(p, q) ==
    /\ CanTick(p, q)
    /\ hello[p][q] # cur[p]
    /\ hello' = [hello EXCEPT ![p][q] = cur[p]]
    /\ lastSent' = [lastSent EXCEPT ![p][q] = cur[p]]
    /\ UNCHANGED <<len, cur, delta, push>>

Reply(r, c) == Mode = "baseline" \/ Ahead(cur[r], c)

PullBack(r, c) == Mode = "initiator" /\ Ahead(c, cur[r])

DeliverHello(s, r) ==
    /\ hello[s][r] # None
    /\ LET c == hello[s][r]
       IN /\ delta' = IF Reply(r, c) THEN [delta EXCEPT ![r][s] = cur[r]] ELSE delta
          /\ hello' = [hello EXCEPT ![s][r] = None,
                                    ![r][s] = IF PullBack(r, c) THEN cur[r] ELSE @]
          /\ lastSent' = IF PullBack(r, c) THEN [lastSent EXCEPT ![r][s] = cur[r]] ELSE lastSent
    /\ UNCHANGED <<len, cur, push>>

DeliverDelta(s, r) ==
    /\ delta[s][r] # None
    /\ LET d == delta[s][r]
       IN cur' = [cur EXCEPT ![r] = [o \in Peers |-> Max(cur[r][o], d[o])]]
    /\ delta' = [delta EXCEPT ![s][r] = None]
    /\ UNCHANGED <<len, hello, push, lastSent>>

DeliverPush(s, r) ==
    /\ push[s][r] > 0
    /\ cur' = IF cur[r][s] = push[s][r] - 1
                 THEN [cur EXCEPT ![r][s] = push[s][r]]
                 ELSE cur
    /\ push' = [push EXCEPT ![s][r] = 0]
    /\ UNCHANGED <<len, hello, delta, lastSent>>

Drop(s, r) ==
    \/ /\ hello[s][r] # None
       /\ hello' = [hello EXCEPT ![s][r] = None]
       /\ UNCHANGED <<len, cur, delta, push, lastSent>>
    \/ /\ delta[s][r] # None
       /\ delta' = [delta EXCEPT ![s][r] = None]
       /\ UNCHANGED <<len, cur, hello, push, lastSent>>
    \/ /\ push[s][r] > 0
       /\ push' = [push EXCEPT ![s][r] = 0]
       /\ UNCHANGED <<len, cur, hello, delta, lastSent>>

Next ==
    \/ \E o \in Writers : Write(o)
    \/ \E pq \in Pairs :
        \/ Tick(pq[1], pq[2])
        \/ DeliverHello(pq[1], pq[2])
        \/ DeliverDelta(pq[1], pq[2])
        \/ DeliverPush(pq[1], pq[2])
        \/ Drop(pq[1], pq[2])

Fairness ==
    /\ \A pq \in Pairs :
        /\ WF_vars(Tick(pq[1], pq[2]))
        /\ SF_vars(DeliverHello(pq[1], pq[2]))
        /\ SF_vars(DeliverDelta(pq[1], pq[2]))
        /\ SF_vars(DeliverPush(pq[1], pq[2]))

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK ==
    /\ len \in Vec
    /\ cur \in [Peers -> Vec]
    /\ hello \in [Peers -> [Peers -> Vec \cup {None}]]
    /\ delta \in [Peers -> [Peers -> Vec \cup {None}]]
    /\ push \in [Peers -> [Peers -> 0..MaxSeq]]
    /\ lastSent \in [Peers -> [Peers -> Vec]]

InvCursorBound == \A p, o \in Peers : cur[p][o] <= len[o]

InvOwnLog == \A o \in Peers : cur[o][o] = len[o]

AllSynced == \A p, o \in Peers : cur[p][o] = len[o]

Converges == <>[]AllSynced

====
