//! CLI: crash-test writer, checker, small key/value commands and a benchmark.

use bptree_wal::{workload, Db, Options};
use std::io::Write;
use std::process::ExitCode;
use std::time::Instant;

const USAGE: &str = "\
usage:
  bptree-wal put DIR KEY VALUE | get DIR KEY | del DIR KEY | scan DIR [START [END]]
  bptree-wal writer DIR SEED [CHECKPOINT_BYTES]   run the deterministic workload forever,
                                                  printing `ack N` after each durable commit
  bptree-wal check DIR SEED                       verify the tree against the workload model
  bptree-wal bench DIR [N]                        insert/get throughput";

fn main() -> ExitCode {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let s = |i: usize| a.get(i).map(String::as_str);
    let r = match (s(0), s(1)) {
        (Some("put"), Some(d)) if a.len() == 4 => Db::open(d).and_then(|mut db| {
            db.put(a[2].as_bytes(), a[3].as_bytes())?;
            db.close()
        }),
        (Some("get"), Some(d)) if a.len() == 3 => Db::open(d)
            .and_then(|mut db| {
                match db.get(a[2].as_bytes())? {
                    Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                    None => return Ok(Some(1)),
                }
                Ok(None)
            })
            .map(|c| {
                if let Some(c) = c {
                    std::process::exit(c)
                }
            }),
        (Some("del"), Some(d)) if a.len() == 3 => Db::open(d).and_then(|mut db| {
            let found = db.delete(a[2].as_bytes())?;
            db.close()?;
            if !found {
                std::process::exit(1);
            }
            Ok(())
        }),
        (Some("scan"), Some(d)) => Db::open(d).and_then(|mut db| {
            for (k, v) in db.scan(s(2).map(str::as_bytes), s(3).map(str::as_bytes))? {
                println!(
                    "{}\t{}",
                    String::from_utf8_lossy(&k),
                    String::from_utf8_lossy(&v)
                );
            }
            Ok(())
        }),
        (Some("writer"), Some(d)) if a.len() >= 3 => writer(d, &a[2], s(3)),
        (Some("check"), Some(d)) if a.len() == 3 => check(d, &a[2]),
        (Some("bench"), Some(d)) => bench(d, s(2).and_then(|n| n.parse().ok()).unwrap_or(100_000)),
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bptree-wal: {e}");
            ExitCode::FAILURE
        }
    }
}

fn seed(s: &str) -> u64 {
    s.parse().unwrap_or_else(|_| {
        eprintln!("bad seed {s}");
        std::process::exit(2)
    })
}

fn writer(dir: &str, seed_s: &str, ckpt: Option<&str>) -> bptree_wal::Result<()> {
    let seed = seed(seed_s);
    let opts = Options {
        checkpoint_bytes: ckpt.and_then(|c| c.parse().ok()).unwrap_or(256 << 10),
        ..Options::default()
    };
    let mut db = Db::open_with(dir, opts)?;
    let rec = db.recovery().clone();
    let mut out = std::io::stdout().lock();
    let _ = writeln!(
        out,
        "start txn={} replayed={} discarded={}",
        db.last_txn(),
        rec.replayed_txns,
        rec.discarded_bytes
    );
    loop {
        let t = db.last_txn() + 1;
        workload::apply(&mut db, seed, t)?;
        // Printed only after write() returned, i.e. after the WAL fsync.
        if writeln!(out, "ack {t}").and_then(|_| out.flush()).is_err() {
            return Ok(());
        }
    }
}

fn check(dir: &str, seed_s: &str) -> bptree_wal::Result<()> {
    let seed = seed(seed_s);
    let mut db = Db::open(dir)?;
    let rec = db.recovery().clone();
    match workload::check(&mut db, seed) {
        Ok(n) => {
            println!(
                "ok txn={} keys={n} replayed_txns={} replayed_pages={} discarded_bytes={}",
                db.last_txn(),
                rec.replayed_txns,
                rec.replayed_pages,
                rec.discarded_bytes
            );
            Ok(())
        }
        Err(e) => {
            eprintln!("bptree-wal: check failed: {e}");
            std::process::exit(1)
        }
    }
}

fn key(i: u64) -> [u8; 16] {
    // Scrambled so inserts land all over the tree, not just the rightmost leaf.
    let x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(17);
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&x.to_be_bytes());
    k[8..].copy_from_slice(&i.to_be_bytes());
    k
}

fn bench(dir: &str, n: u64) -> bptree_wal::Result<()> {
    let _ = std::fs::remove_dir_all(dir);
    let val = [7u8; 100];
    let mut db = Db::open(dir)?;
    let k = 2_000.min(n);
    let t = Instant::now();
    for i in 0..k {
        db.put(&key(i), &val)?;
    }
    let single = k as f64 / t.elapsed().as_secs_f64();
    println!("put, 1 key/txn, fsync each : {single:>10.0} txn/s ({k} txns)");

    let mut db2 = Db::open_with(format!("{dir}/batched"), Options::default())?;
    let t = Instant::now();
    let mut i = 0;
    while i < n {
        let end = (i + 1000).min(n);
        db2.write(|tx| {
            for j in i..end {
                tx.put(&key(j), &val)?;
            }
            Ok(())
        })?;
        i = end;
    }
    db2.checkpoint()?;
    let batched = n as f64 / t.elapsed().as_secs_f64();
    println!(
        "put, 1000 keys/txn          : {batched:>10.0} keys/s ({n} keys, incl. final checkpoint)"
    );

    let t = Instant::now();
    for i in 0..n {
        let j = i.wrapping_mul(7919) % n;
        if db2.get(&key(j))?.is_none() {
            panic!("missing key {j}");
        }
    }
    let gets = n as f64 / t.elapsed().as_secs_f64();
    println!("get, random, warm cache     : {gets:>10.0} keys/s");
    let t = Instant::now();
    let all = db2.scan(None, None)?;
    println!(
        "full scan                   : {:>10.0} keys/s ({} keys)",
        all.len() as f64 / t.elapsed().as_secs_f64(),
        all.len()
    );
    println!("pages={} stats={:?}", db2.page_count(), db2.stats());
    db2.close()?;
    db.close()?;
    Ok(())
}
