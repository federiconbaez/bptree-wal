//! Redo write-ahead log of full page images.
//!
//! Record: `magic u32 | kind u8 | txn u64 | page u32 | len u32 | payload | crc32`
//! where the CRC covers everything before it. A transaction is a run of
//! `PAGE` records followed by one `COMMIT`; replay applies a transaction only
//! when its commit record is intact, and stops at the first damaged record.

use crate::page::{corrupt, PAGE_SIZE};
use std::fs::File;
use std::io::{self, Read, Write};

const MAGIC: u32 = 0x5741_4C31; // "WAL1"
const KIND_PAGE: u8 = 1;
const KIND_COMMIT: u8 = 2;
const HEADER: usize = 4 + 1 + 8 + 4 + 4;

pub fn encode_txn(txn: u64, pages: &[(u32, Box<[u8; PAGE_SIZE]>)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(pages.len() * (HEADER + PAGE_SIZE + 4) + HEADER + 4);
    for (id, img) in pages {
        record(&mut out, KIND_PAGE, txn, *id, &img[..]);
    }
    record(&mut out, KIND_COMMIT, txn, 0, &[]);
    out
}

fn record(out: &mut Vec<u8>, kind: u8, txn: u64, page: u32, payload: &[u8]) {
    let start = out.len();
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(&txn.to_le_bytes());
    out.extend_from_slice(&page.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    let crc = crc32fast::hash(&out[start..]);
    out.extend_from_slice(&crc.to_le_bytes());
}

/// A committed transaction recovered from the log.
pub struct Committed {
    pub txn: u64,
    pub pages: Vec<(u32, Box<[u8; PAGE_SIZE]>)>,
}

/// Result of scanning a log: complete transactions plus how the scan ended.
pub struct Scan {
    pub committed: Vec<Committed>,
    /// Bytes covered by complete transactions.
    pub valid_len: u64,
    /// Bytes after `valid_len` that were ignored (torn tail or damage).
    pub discarded: u64,
}

pub fn scan(file: &mut File) -> io::Result<Scan> {
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    let mut at = 0usize;
    let mut valid_len = 0usize;
    let mut committed = Vec::new();
    let mut pending: Vec<(u32, Box<[u8; PAGE_SIZE]>)> = Vec::new();
    let mut pending_txn: Option<u64> = None;
    while let Some((kind, txn, page, payload, next)) = parse(&buf, at) {
        if pending_txn.is_some_and(|t| t != txn) {
            break; // interleaved transaction ids: treat as damage
        }
        match kind {
            KIND_PAGE if payload.len() == PAGE_SIZE => {
                pending_txn = Some(txn);
                pending.push((page, Box::new(payload.try_into().unwrap())));
            }
            KIND_COMMIT if payload.is_empty() => {
                committed.push(Committed {
                    txn,
                    pages: std::mem::take(&mut pending),
                });
                pending_txn = None;
                valid_len = next;
            }
            _ => break,
        }
        at = next;
    }
    Ok(Scan {
        committed,
        valid_len: valid_len as u64,
        discarded: (buf.len() - valid_len) as u64,
    })
}

fn parse(buf: &[u8], at: usize) -> Option<(u8, u64, u32, &[u8], usize)> {
    let h = buf.get(at..at + HEADER)?;
    if u32::from_le_bytes(h[0..4].try_into().unwrap()) != MAGIC {
        return None;
    }
    let kind = h[4];
    let txn = u64::from_le_bytes(h[5..13].try_into().unwrap());
    let page = u32::from_le_bytes(h[13..17].try_into().unwrap());
    let len = u32::from_le_bytes(h[17..21].try_into().unwrap()) as usize;
    if len > PAGE_SIZE {
        return None;
    }
    let end = at + HEADER + len;
    let crc = u32::from_le_bytes(buf.get(end..end + 4)?.try_into().unwrap());
    if crc != crc32fast::hash(&buf[at..end]) {
        return None;
    }
    Some((kind, txn, page, &buf[at + HEADER..end], end + 4))
}

pub fn append(file: &mut File, bytes: &[u8], sync: bool) -> io::Result<()> {
    file.write_all(bytes)?;
    if sync {
        file.sync_data()?;
    }
    Ok(())
}

pub fn ensure_page_ids(c: &Committed, page_limit: u32) -> io::Result<()> {
    match c.pages.iter().find(|(id, _)| *id > page_limit) {
        Some((id, _)) => Err(corrupt(&format!(
            "WAL page id {id} beyond limit {page_limit}"
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};

    fn tmpfile(tag: &str) -> (std::path::PathBuf, File) {
        let p = std::env::temp_dir().join(format!("bptree-wal-unit-{tag}-{}", std::process::id()));
        let f = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&p)
            .unwrap();
        (p, f)
    }

    fn img(b: u8) -> Box<[u8; PAGE_SIZE]> {
        Box::new([b; PAGE_SIZE])
    }

    #[test]
    fn torn_tail_drops_only_the_incomplete_txn() {
        let (path, mut f) = tmpfile("torn");
        let t1 = encode_txn(1, &[(1, img(1)), (2, img(2))]);
        let t2 = encode_txn(2, &[(3, img(3))]);
        append(&mut f, &t1, false).unwrap();
        append(&mut f, &t2[..t2.len() - 1], false).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        let s = scan(&mut f).unwrap();
        assert_eq!(s.committed.len(), 1);
        assert_eq!(s.committed[0].txn, 1);
        assert_eq!(s.committed[0].pages.len(), 2);
        assert_eq!(s.valid_len, t1.len() as u64);
        assert_eq!(s.discarded, (t2.len() - 1) as u64);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn flipped_byte_stops_replay_at_that_txn() {
        let (path, mut f) = tmpfile("flip");
        let t1 = encode_txn(1, &[(1, img(1))]);
        let mut t2 = encode_txn(2, &[(2, img(2))]);
        let t3 = encode_txn(3, &[(3, img(3))]);
        t2[100] ^= 0x40;
        for t in [&t1, &t2, &t3] {
            append(&mut f, t, false).unwrap();
        }
        f.seek(SeekFrom::Start(0)).unwrap();
        let s = scan(&mut f).unwrap();
        assert_eq!(
            s.committed.iter().map(|c| c.txn).collect::<Vec<_>>(),
            vec![1]
        );
        std::fs::remove_file(path).unwrap();
    }
}
