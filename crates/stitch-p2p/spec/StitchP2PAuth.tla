---- MODULE StitchP2PAuth ----
\* Membership authorization over eventually-consistent membership.
\*
\* A data write W is authored by X; a membership event M makes X a member.
\* Both replicate independently, so peers receive W and M in either order.
\*
\* NAIVE rule (Causal = FALSE): when a peer receives W, it stores W only if it
\* already knows X is a member; otherwise it DROPS W permanently and never
\* reconsiders. Receiving M later does not bring W back.
\*
\* CAUSAL rule (Causal = TRUE): a peer always stores W; visibility is a pure
\* function of the converged state — X was a member at W's logical time. With M
\* causally before W, that's simply "the peer has both W and M."
\*
\* We expect NAIVE to violate convergence (two fully-synced peers disagree on
\* whether W is visible, by receive order) and CAUSAL to hold.

EXTENDS Naturals, FiniteSets

CONSTANTS
    Peers,
    Causal   \* TRUE = causal read-time filter; FALSE = reject-at-receipt

ASSUME Causal \in BOOLEAN

VARIABLES
    gotW,    \* gotW[p]  : p has received the data write W
    gotM,    \* gotM[p]  : p has received the membership event M
    stored   \* stored[p]: p decided to keep W (the naive rule gates this)

vars == <<gotW, gotM, stored>>

Init ==
    /\ gotW = [p \in Peers |-> FALSE]
    /\ gotM = [p \in Peers |-> FALSE]
    /\ stored = [p \in Peers |-> FALSE]

\* Receive the data write. Naive: keep it only if X is a known member now.
\* Causal: always keep it.
RecvW(p) ==
    /\ ~gotW[p]
    /\ gotW' = [gotW EXCEPT ![p] = TRUE]
    /\ stored' = [stored EXCEPT ![p] = IF Causal THEN TRUE ELSE gotM[p]]
    /\ UNCHANGED gotM

\* Receive the membership event. Naive does NOT re-evaluate previously dropped
\* writes (that is the bug); causal doesn't need to.
RecvM(p) ==
    /\ ~gotM[p]
    /\ gotM' = [gotM EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<gotW, stored>>

Next ==
    \/ \E p \in Peers: RecvW(p)
    \/ \E p \in Peers: RecvM(p)

Spec == Init /\ [][Next]_vars

\* What the application sees. With M causally before W, the correct answer is
\* "visible once you have both."
Visible(p) == IF Causal THEN (gotW[p] /\ gotM[p]) ELSE stored[p]

FullySynced(p) == gotW[p] /\ gotM[p]

TypeOK ==
    /\ gotW \in [Peers -> BOOLEAN]
    /\ gotM \in [Peers -> BOOLEAN]
    /\ stored \in [Peers -> BOOLEAN]

\* Two peers that have received both W and M must agree on W's visibility.
InvConvergence ==
    \A p, q \in Peers:
        (FullySynced(p) /\ FullySynced(q)) => Visible(p) = Visible(q)
====
