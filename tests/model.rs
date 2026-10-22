//! Model-based tests: random operation sequences against `BTreeMap`, with
//! simulated crashes (drop without close) and reopen in the middle.

mod common;

use bptree_wal::{Db, Error, Options};
use common::TempDir;
use proptest::prelude::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Del(Vec<u8>),
    Batch(Vec<(Vec<u8>, Vec<u8>)>),
    Crash,
    Checkpoint,
}

fn key() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        (0u16..400).prop_map(|i| format!("{i:05}").into_bytes()),
        proptest::collection::vec(any::<u8>(), 0..=128),
    ]
}

fn val() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..64),
        proptest::collection::vec(any::<u8>(), 900..=1024),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        10 => (key(), val()).prop_map(|(k, v)| Op::Put(k, v)),
        3 => key().prop_map(Op::Del),
        2 => proptest::collection::vec((key(), val()), 1..40).prop_map(Op::Batch),
        1 => Just(Op::Crash),
        1 => Just(Op::Checkpoint),
    ]
}

fn small_ckpt() -> Options {
    Options {
        checkpoint_bytes: 64 << 10,
        ..Options::default()
    }
}

fn assert_same(db: &mut Db, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    assert_eq!(db.verify().unwrap(), model.len());
    let all = db.scan(None, None).unwrap();
    assert!(all.iter().map(|(k, v)| (k, v)).eq(model.iter()));
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    #[test]
    fn matches_btreemap_across_crashes(ops in proptest::collection::vec(op(), 1..250)) {
        let dir = TempDir::new("model");
        let mut db = Db::open_with(&dir.0, small_ckpt()).unwrap();
        let mut model = BTreeMap::new();
        for op in ops {
            match op {
                Op::Put(k, v) => {
                    db.put(&k, &v).unwrap();
                    model.insert(k, v);
                }
                Op::Del(k) => {
                    prop_assert_eq!(db.delete(&k).unwrap(), model.remove(&k).is_some());
                }
                Op::Batch(kvs) => {
                    db.write(|tx| {
                        for (k, v) in &kvs {
                            tx.put(k, v)?;
                        }
                        Ok(())
                    }).unwrap();
                    model.extend(kvs);
                }
                Op::Crash => {
                    let txn = db.last_txn();
                    drop(db);
                    db = Db::open_with(&dir.0, small_ckpt()).unwrap();
                    prop_assert_eq!(db.last_txn(), txn);
                }
                Op::Checkpoint => db.checkpoint().unwrap(),
            }
        }
        assert_same(&mut db, &model);
        for k in model.keys().take(50) {
            let got = db.get(k).unwrap();
            prop_assert_eq!(got.as_ref(), model.get(k));
        }
        db.close().unwrap();
        let mut db = Db::open(&dir.0).unwrap();
        prop_assert_eq!(db.recovery().replayed_txns, 0);
        assert_same(&mut db, &model);
    }

    #[test]
    fn range_scans_match(keys in proptest::collection::btree_set(key(), 0..300), lo in key(), hi in key()) {
        let dir = TempDir::new("range");
        let mut db = Db::open(&dir.0).unwrap();
        db.write(|tx| {
            for k in &keys {
                tx.put(k, k)?;
            }
            Ok(())
        }).unwrap();
        let got: Vec<Vec<u8>> = db.scan(Some(&lo), Some(&hi)).unwrap().into_iter().map(|(k, _)| k).collect();
        let want: Vec<Vec<u8>> = keys.iter().filter(|k| **k >= lo && **k < hi).cloned().collect();
        prop_assert_eq!(got, want);
    }
}

#[test]
fn many_sequential_and_reverse_inserts_build_a_deep_valid_tree() {
    let dir = TempDir::new("deep");
    let mut db = Db::open(&dir.0).unwrap();
    db.write(|tx| {
        for i in 0..20_000u32 {
            tx.put(&i.to_be_bytes(), &[1; 40])?;
            tx.put(&(u32::MAX - i).to_be_bytes(), &[2; 40])?;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(db.verify().unwrap(), 40_000);
    assert!(db.page_count() > 400);
    db.close().unwrap();
    let mut db = Db::open(&dir.0).unwrap();
    assert_eq!(db.get(&7u32.to_be_bytes()).unwrap(), Some(vec![1; 40]));
    assert_eq!(
        db.get(&(u32::MAX - 7).to_be_bytes()).unwrap(),
        Some(vec![2; 40])
    );
    assert_eq!(db.verify().unwrap(), 40_000);
}

#[test]
fn limits_and_failed_transactions() {
    let dir = TempDir::new("limits");
    let mut db = Db::open(&dir.0).unwrap();
    assert!(matches!(
        db.put(&[0; 129], b"v"),
        Err(Error::KeyTooLarge(129))
    ));
    assert!(matches!(
        db.put(b"k", &[0; 1025]),
        Err(Error::ValueTooLarge(1025))
    ));
    db.put(&[9; 128], &[9; 1024]).unwrap();
    let before = db.last_txn();
    let r = db.write(|tx| {
        tx.put(b"a", b"1")?;
        assert_eq!(tx.get(b"a")?, Some(b"1".to_vec()));
        tx.put(b"b", &[0; 2000])
    });
    assert!(r.is_err());
    assert_eq!(db.last_txn(), before, "a failed closure consumes no id");
    assert_eq!(
        db.get(b"a").unwrap(),
        None,
        "nothing from the failed closure is visible"
    );
    assert!(!db.delete(b"missing").unwrap());
    assert_eq!(
        db.last_txn(),
        before + 1,
        "an empty write still commits one id"
    );
}

#[test]
fn rejects_a_foreign_data_file() {
    let dir = TempDir::new("foreign");
    std::fs::create_dir_all(&dir.0).unwrap();
    std::fs::write(dir.0.join("data.db"), vec![0xAB; 8192]).unwrap();
    let err = Db::open(&dir.0).err().expect("must fail");
    assert!(err.to_string().contains("bad magic"), "{err}");
}
