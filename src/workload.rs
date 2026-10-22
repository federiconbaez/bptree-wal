//! Deterministic transaction stream shared by the crash tests and the CLI.
//! Transaction `t` of seed `s` is always the same, so a writer can resume after
//! a crash at `last_txn() + 1` and a checker can rebuild the expected state.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

struct SplitMix(u64);
impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

pub const KEY_SPACE: u64 = 20_000;

pub fn txn_ops(seed: u64, txn: u64) -> Vec<Op> {
    let mut r = SplitMix(seed ^ txn.wrapping_mul(0xD6E8_FEB8_6659_FD93));
    let n = 1 + r.next() % 16;
    (0..n)
        .map(|i| {
            let key = format!("k{:08}", r.next() % KEY_SPACE).into_bytes();
            if r.next() % 10 == 0 {
                Op::Delete(key)
            } else {
                let len = (r.next() % 300) as usize;
                let tag = format!("t{txn}.{i}:");
                let mut v = tag.into_bytes();
                v.resize(len.max(v.len()), b'a' + (txn % 26) as u8);
                Op::Put(key, v)
            }
        })
        .collect()
}

/// Expected contents after transactions `1..=upto`.
pub fn model(seed: u64, upto: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut m = BTreeMap::new();
    for t in 1..=upto {
        for op in txn_ops(seed, t) {
            match op {
                Op::Put(k, v) => {
                    m.insert(k, v);
                }
                Op::Delete(k) => {
                    m.remove(&k);
                }
            }
        }
    }
    m
}

pub fn apply(db: &mut crate::Db, seed: u64, txn: u64) -> crate::Result<()> {
    db.write(|tx| {
        for op in txn_ops(seed, txn) {
            match op {
                Op::Put(k, v) => tx.put(&k, &v)?,
                Op::Delete(k) => {
                    tx.delete(&k)?;
                }
            }
        }
        Ok(())
    })
}

/// Compares the whole tree with the model at `db.last_txn()`.
pub fn check(db: &mut crate::Db, seed: u64) -> Result<usize, String> {
    let n = db.verify().map_err(|e| format!("verify: {e}"))?;
    let want = model(seed, db.last_txn());
    let got = db.scan(None, None).map_err(|e| format!("scan: {e}"))?;
    if got.len() != n || got.len() != want.len() {
        return Err(format!(
            "txn {}: {} keys in tree, {} scanned, {} expected",
            db.last_txn(),
            n,
            got.len(),
            want.len()
        ));
    }
    for ((gk, gv), (wk, wv)) in got.iter().zip(&want) {
        if gk != wk || gv != wv {
            return Err(format!(
                "txn {}: mismatch at key {}",
                db.last_txn(),
                String::from_utf8_lossy(gk)
            ));
        }
    }
    Ok(n)
}
