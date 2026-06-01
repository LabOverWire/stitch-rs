//! Ad-hoc soak runner. Usage:
//!   cargo run --release --bin soak -- [peers] [rounds] [seed]
//!
//! Drives a randomized, partition-and-membership-churning task-board workload
//! across N in-process peers and reports whether they converge.

use std::time::{Duration, Instant};
use stitch_tasks::harness::{Chaos, run_chaos};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let peers = arg(&args, 1, 4);
    let rounds = arg(&args, 2, 1000);
    let seed = arg(&args, 3, 0) as u64;

    let cfg = Chaos {
        peers,
        rounds,
        seed,
        pull: Duration::from_millis(15),
        ..Chaos::default()
    };

    println!(
        "soak: {} peers, {} rounds, seed {} ...",
        cfg.peers, cfg.rounds, cfg.seed
    );
    let start = Instant::now();
    let (_, report) = run_chaos(cfg).await;
    let elapsed = start.elapsed();

    println!(
        "  ops:        add {} / rename {} / toggle {} / remove {}",
        report.adds, report.renames, report.toggles, report.removes
    );
    println!(
        "  chaos:      {} partitions, {} heals, {} revokes",
        report.partitions, report.heals, report.revokes
    );
    println!("  final board: {} tasks", report.final_tasks);
    println!("  elapsed:    {:.2?}", elapsed);
    if report.converged {
        println!("  CONVERGED ✓");
    } else {
        eprintln!("  DID NOT CONVERGE ✗");
        std::process::exit(1);
    }
}

fn arg(args: &[String], i: usize, default: usize) -> usize {
    args.get(i).and_then(|s| s.parse().ok()).unwrap_or(default)
}
