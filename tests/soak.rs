//! Chaos/soak: many peers hammer a shared task board with concurrent, often
//! colliding operations while links partition and heal and membership churns,
//! then — once healed and quiesced — every peer must converge to the identical
//! board. This is the empirical complement to the TLA+ models: they verify
//! small state spaces exhaustively; this stresses the real engine under
//! randomized timing and churn.

use stitch_tasks::harness::{Chaos, run_chaos};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak_converges_across_seeds() {
    for seed in 0..3u64 {
        let (_, report) = run_chaos(Chaos {
            seed,
            ..Chaos::default()
        })
        .await;
        assert!(report.converged, "peers failed to converge (seed {seed})");
        assert!(
            report.final_tasks > 0,
            "convergence should not be vacuous (seed {seed}): {report:?}"
        );
    }
}
