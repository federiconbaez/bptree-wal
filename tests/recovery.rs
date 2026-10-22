//! Crash recovery: WAL truncated or damaged at arbitrary offsets, and a real
//! writer process killed with SIGKILL while it commits.

mod common;

use bptree_wal::{workload, Db, Options};
use common::{copy_dir, TempDir};
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

const SEED: u64 = 0xC0FFEE;

/// Builds a tree with a checkpointed base and `n` more transactions left only
/// in the WAL; returns the WAL length after each of those commits.
fn wal_only_history(dir: &std::path::Path, base: u64, n: u64) -> Vec<u64> {
    let opts = Options {
        checkpoint_bytes: u64::MAX,
        ..Options::default()
    };
    let mut db = Db::open_with(dir, opts).unwrap();
    for t in 1..=base {
        workload::apply(&mut db, SEED, t).unwrap();
    }
    db.checkpoint().unwrap();
    let mut ends = vec![0];
    for t in base + 1..=base + n {
        workload::apply(&mut db, SEED, t).unwrap();
        ends.push(db.wal_len());
    }
    drop(db); // crash: no checkpoint
    ends
}

#[test]
fn truncated_wal_recovers_exactly_the_complete_prefix() {
    let src = TempDir::new("trunc-src");
    let (base, n) = (300, 40);
    let ends = wal_only_history(&src.0, base, n);
    let total = *ends.last().unwrap();
    // Every commit boundary, one byte either side, and a spread of interior cuts.
    let mut cuts: Vec<u64> = ends
        .iter()
        .flat_map(|&e| [e.saturating_sub(1), e, (e + 1).min(total)])
        .collect();
    cuts.extend((0..200).map(|i| total * i / 200));
    cuts.sort_unstable();
    cuts.dedup();
    for cut in cuts {
        let d = TempDir::new("trunc");
        copy_dir(&src.0, &d.0);
        std::fs::OpenOptions::new()
            .write(true)
            .open(d.0.join("wal.log"))
            .unwrap()
            .set_len(cut)
            .unwrap();
        let mut db = Db::open(&d.0).unwrap();
        let complete = ends.iter().filter(|&&e| e > 0 && e <= cut).count() as u64;
        assert_eq!(db.last_txn(), base + complete, "cut at {cut}");
        assert_eq!(db.recovery().replayed_txns as u64, complete);
        workload::check(&mut db, SEED).unwrap_or_else(|e| panic!("cut at {cut}: {e}"));
    }
}

#[test]
fn damaged_record_stops_replay_before_that_transaction() {
    let src = TempDir::new("flip-src");
    let (base, n) = (120, 25);
    let ends = wal_only_history(&src.0, base, n);
    for (i, w) in ends.windows(2).enumerate() {
        for off in [w[0], (w[0] + w[1]) / 2, w[1] - 1] {
            let d = TempDir::new("flip");
            copy_dir(&src.0, &d.0);
            let p = d.0.join("wal.log");
            let mut bytes = std::fs::read(&p).unwrap();
            bytes[off as usize] ^= 0x10;
            std::fs::write(&p, bytes).unwrap();
            let mut db = Db::open(&d.0).unwrap();
            assert_eq!(
                db.last_txn(),
                base + i as u64,
                "flip at {off} in txn {}",
                i + 1
            );
            workload::check(&mut db, SEED).unwrap();
            assert!(db.recovery().discarded_bytes > 0);
        }
    }
}

#[test]
fn recovery_is_idempotent_if_interrupted_before_wal_truncate() {
    // Simulates a crash after replay wrote the data file but before the WAL was
    // emptied: replaying the same WAL again must give the same tree.
    let src = TempDir::new("idem");
    wal_only_history(&src.0, 50, 30);
    let wal = std::fs::read(src.0.join("wal.log")).unwrap();
    let db = Db::open(&src.0).unwrap();
    let txn = db.last_txn();
    drop(db);
    std::fs::write(src.0.join("wal.log"), wal).unwrap();
    let mut db = Db::open(&src.0).unwrap();
    assert_eq!(db.last_txn(), txn);
    assert_eq!(db.recovery().replayed_txns, 30);
    workload::check(&mut db, SEED).unwrap();
}

/// Spawns the writer, waits for `acks` durable commits, then SIGKILLs it.
fn run_and_kill(dir: &std::path::Path, acks: u64, ckpt: u64) -> u64 {
    let mut child = Command::new(env!("CARGO_BIN_EXE_bptree-wal"))
        .args([
            "writer",
            dir.to_str().unwrap(),
            &SEED.to_string(),
            &ckpt.to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let mut last = None;
    let mut seen = 0;
    for line in lines.by_ref() {
        let line = line.unwrap();
        if let Some(n) = line.strip_prefix("ack ") {
            last = Some(n.parse::<u64>().unwrap());
            seen += 1;
            if seen == acks {
                break;
            }
        }
    }
    child.kill().unwrap(); // SIGKILL
    child.wait().unwrap();
    last.expect("writer acknowledged nothing")
}

#[test]
fn sigkill_during_commits_loses_no_acknowledged_transaction() {
    let dir = TempDir::new("kill");
    let mut prev = 0;
    // Same directory across rounds: each writer resumes from the recovered state.
    for round in 0..12u64 {
        let acks = 20 + (round * 37) % 150;
        let ckpt = [16 << 10, 64 << 10, 1 << 20][round as usize % 3];
        let acked = run_and_kill(&dir.0, acks, ckpt);
        let mut db = Db::open(&dir.0).unwrap();
        let t = db.last_txn();
        assert!(
            t == acked || t == acked + 1,
            "round {round}: acked {acked}, recovered {t}"
        );
        assert!(t > prev);
        workload::check(&mut db, SEED).unwrap_or_else(|e| panic!("round {round}: {e}"));
        prev = t;
    }
}
