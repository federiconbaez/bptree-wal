//! A page-based on-disk B+tree with a redo write-ahead log.
//!
//! Durability model (no-steal / redo-only):
//! - a transaction's dirty pages are appended to the WAL as full page images
//!   followed by a commit record, and the WAL is fsynced before `write` returns;
//! - the data file is only touched by a checkpoint, which writes every page
//!   changed since the last one, fsyncs the data file, then truncates the WAL;
//! - on open, committed transactions found in the WAL are replayed into the
//!   data file and a checkpoint follows. A torn or damaged tail is discarded.
//!
//! So after a crash at any instant the tree reopens as of the last transaction
//! whose commit record reached the disk intact.

pub mod page;
pub mod wal;
pub mod workload;

use page::{Meta, Node, MAX_KEY, MAX_VALUE, PAGE_SIZE};
use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{self, Seek, SeekFrom};
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Options {
    /// fsync the WAL on every commit (turning this off trades durability of the
    /// last transactions for throughput; recovery stays consistent).
    pub sync_commits: bool,
    /// Checkpoint automatically once the WAL grows past this many bytes.
    pub checkpoint_bytes: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            sync_commits: true,
            checkpoint_bytes: 8 << 20,
        }
    }
}

/// What `open` found in the WAL.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Recovery {
    pub replayed_txns: usize,
    pub replayed_pages: usize,
    pub discarded_bytes: u64,
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub commits: u64,
    pub wal_bytes_written: u64,
    pub checkpoints: u64,
    pub pages_written: u64,
}

pub struct Db {
    dir: PathBuf,
    data: File,
    wal: File,
    meta: Meta,
    /// Every node loaded so far plus every node changed since the last checkpoint.
    cache: HashMap<u32, Node>,
    /// Pages whose newest image lives only in the WAL.
    unflushed: BTreeSet<u32>,
    wal_len: u64,
    opts: Options,
    recovery: Recovery,
    stats: Stats,
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    KeyTooLarge(usize),
    ValueTooLarge(usize),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{e}"),
            Error::KeyTooLarge(n) => write!(f, "key of {n} bytes exceeds {MAX_KEY}"),
            Error::ValueTooLarge(n) => write!(f, "value of {n} bytes exceeds {MAX_VALUE}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

const DATA: &str = "data.db";
const WAL: &str = "wal.log";

impl Db {
    pub fn open(dir: impl AsRef<Path>) -> Result<Db> {
        Self::open_with(dir, Options::default())
    }

    pub fn open_with(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let data = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(DATA))?;
        let mut wal = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(WAL))?;
        let fresh = data.metadata()?.len() == 0;
        if fresh {
            let meta = Meta {
                root: 1,
                page_count: 2,
                txn: 0,
            };
            data.write_all_at(&Node::empty_leaf().encode()[..], PAGE_SIZE as u64)?;
            data.write_all_at(&meta.encode()[..], 0)?;
            data.sync_all()?;
            File::open(&dir)?.sync_all()?; // make the new directory entries durable
        }

        // Redo: apply every committed transaction, then make it durable and empty the log.
        wal.seek(SeekFrom::Start(0))?;
        let scan = wal::scan(&mut wal)?;
        let mut recovery = Recovery {
            discarded_bytes: scan.discarded,
            ..Recovery::default()
        };
        for c in &scan.committed {
            wal::ensure_page_ids(c, u32::MAX - 1)?;
            for (id, img) in &c.pages {
                data.write_all_at(&img[..], u64::from(*id) * PAGE_SIZE as u64)?;
            }
            recovery.replayed_txns += 1;
            recovery.replayed_pages += c.pages.len();
        }
        if !scan.committed.is_empty() {
            data.sync_all()?;
        }
        wal.set_len(0)?;
        wal.seek(SeekFrom::Start(0))?;
        wal.sync_all()?;

        let mut mp = [0u8; PAGE_SIZE];
        data.read_exact_at(&mut mp, 0)?;
        let meta = Meta::decode(&mp)?;
        Ok(Db {
            dir,
            data,
            wal,
            meta,
            cache: HashMap::new(),
            unflushed: BTreeSet::new(),
            wal_len: 0,
            opts,
            recovery,
            stats: Stats::default(),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
    pub fn recovery(&self) -> &Recovery {
        &self.recovery
    }
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
    /// Id of the last committed transaction (0 for a fresh tree).
    pub fn last_txn(&self) -> u64 {
        self.meta.txn
    }
    pub fn wal_len(&self) -> u64 {
        self.wal_len
    }
    pub fn page_count(&self) -> u32 {
        self.meta.page_count
    }

    fn load(&mut self, id: u32) -> io::Result<&Node> {
        if !self.cache.contains_key(&id) {
            if id == 0 || id >= self.meta.page_count {
                return Err(page::corrupt(&format!("page id {id} out of range")));
            }
            let mut buf = [0u8; PAGE_SIZE];
            self.data
                .read_exact_at(&mut buf, u64::from(id) * PAGE_SIZE as u64)?;
            self.cache.insert(id, Node::decode(&buf)?);
        }
        Ok(&self.cache[&id])
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut id = self.meta.root;
        loop {
            match self.load(id)? {
                Node::Internal { keys, children } => id = children[child_index(keys, key)],
                Node::Leaf { keys, vals, .. } => {
                    return Ok(keys
                        .binary_search_by(|k| k.as_slice().cmp(key))
                        .ok()
                        .map(|i| vals[i].clone()))
                }
            }
        }
    }

    /// Key/value pairs with `start <= key < end` (unbounded when `None`), in order.
    pub fn scan(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let lo = start.unwrap_or(&[]);
        let mut id = self.meta.root;
        while let Node::Internal { keys, children } = self.load(id)? {
            id = children[child_index(keys, lo)];
        }
        let mut out = Vec::new();
        loop {
            let Node::Leaf { keys, vals, next } = self.load(id)? else {
                return Err(page::corrupt("leaf chain reached an internal node").into());
            };
            for (k, v) in keys.iter().zip(vals) {
                if k.as_slice() < lo {
                    continue;
                }
                if end.is_some_and(|e| k.as_slice() >= e) {
                    return Ok(out);
                }
                out.push((k.clone(), v.clone()));
            }
            if *next == 0 {
                return Ok(out);
            }
            id = *next;
        }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.write(|tx| tx.put(key, val))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let mut found = false;
        self.write(|tx| {
            found = tx.delete(key)?;
            Ok(())
        })?;
        Ok(found)
    }

    /// Runs `f` as one atomic, durable transaction and returns once it is in the
    /// fsynced WAL. If `f` fails nothing is applied and no id is consumed.
    pub fn write<F>(&mut self, f: F) -> Result<()>
    where
        F: FnOnce(&mut Tx<'_>) -> Result<()>,
    {
        let meta = self.meta;
        let mut tx = Tx {
            db: self,
            dirty: HashMap::new(),
            meta,
        };
        f(&mut tx)?;
        let Tx { dirty, meta, .. } = tx;
        // Every successful write() is one transaction id, even if it changed no
        // node (e.g. deleting a missing key): ids then match calls one-to-one.
        let meta = Meta {
            txn: meta.txn + 1,
            ..meta
        };
        let mut pages: Vec<(u32, Box<[u8; PAGE_SIZE]>)> =
            dirty.iter().map(|(id, n)| (*id, n.encode())).collect();
        pages.sort_by_key(|(id, _)| *id);
        pages.push((0, meta.encode()));
        let bytes = wal::encode_txn(meta.txn, &pages);
        wal::append(&mut self.wal, &bytes, self.opts.sync_commits)?;
        self.wal_len += bytes.len() as u64;
        self.stats.commits += 1;
        self.stats.wal_bytes_written += bytes.len() as u64;
        for (id, n) in dirty {
            self.unflushed.insert(id);
            self.cache.insert(id, n);
        }
        self.meta = meta;
        if self.wal_len >= self.opts.checkpoint_bytes {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Writes pages changed since the last checkpoint to the data file, fsyncs it
    /// and truncates the WAL.
    pub fn checkpoint(&mut self) -> Result<()> {
        if self.unflushed.is_empty() && self.wal_len == 0 {
            return Ok(());
        }
        for id in std::mem::take(&mut self.unflushed) {
            let img = self.cache[&id].encode();
            self.data
                .write_all_at(&img[..], u64::from(id) * PAGE_SIZE as u64)?;
            self.stats.pages_written += 1;
        }
        self.data.write_all_at(&self.meta.encode()[..], 0)?;
        self.data.sync_all()?;
        self.wal.set_len(0)?;
        self.wal.seek(SeekFrom::Start(0))?;
        self.wal.sync_all()?;
        self.wal_len = 0;
        self.stats.checkpoints += 1;
        Ok(())
    }

    /// Checkpoints and closes. Dropping a `Db` without `close` is a valid
    /// "crash": the next `open` recovers from the WAL.
    pub fn close(mut self) -> Result<()> {
        self.checkpoint()
    }

    /// Walks the whole tree and checks structural invariants: keys sorted and
    /// within their parent's bounds, uniform leaf depth, a leaf chain that visits
    /// every leaf in order, no page reachable twice. Returns the key count.
    pub fn verify(&mut self) -> Result<usize> {
        let mut leaves = Vec::new();
        let mut seen = BTreeSet::new();
        let mut leaf_depth = None;
        let root = self.meta.root;
        self.verify_node(
            root,
            Bound::Unbounded,
            Bound::Unbounded,
            0,
            &mut leaf_depth,
            &mut leaves,
            &mut seen,
        )?;
        let mut count = 0;
        for (i, &id) in leaves.iter().enumerate() {
            let Node::Leaf { keys, next, .. } = self.load(id)? else {
                unreachable!()
            };
            count += keys.len();
            let expect = leaves.get(i + 1).copied().unwrap_or(0);
            if *next != expect {
                return Err(page::corrupt(&format!(
                    "leaf {id} links to {next}, expected {expect}"
                ))
                .into());
            }
        }
        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    fn verify_node(
        &mut self,
        id: u32,
        lo: Bound<Vec<u8>>,
        hi: Bound<Vec<u8>>,
        depth: usize,
        leaf_depth: &mut Option<usize>,
        leaves: &mut Vec<u32>,
        seen: &mut BTreeSet<u32>,
    ) -> Result<()> {
        if !seen.insert(id) {
            return Err(page::corrupt(&format!("page {id} reachable twice")).into());
        }
        let node = self.load(id)?.clone();
        let keys = match &node {
            Node::Leaf { keys, .. } | Node::Internal { keys, .. } => keys,
        };
        if keys.windows(2).any(|w| w[0] >= w[1]) {
            return Err(page::corrupt(&format!("page {id}: keys not strictly increasing")).into());
        }
        let in_bounds = |k: &Vec<u8>| {
            (match &lo {
                Bound::Included(l) => k >= l,
                _ => true,
            }) && (match &hi {
                Bound::Excluded(h) => k < h,
                _ => true,
            })
        };
        if !keys.iter().all(in_bounds) {
            return Err(page::corrupt(&format!("page {id}: key outside parent bounds")).into());
        }
        if node.overflows() {
            return Err(page::corrupt(&format!("page {id}: overflows")).into());
        }
        match node {
            Node::Leaf { .. } => {
                if *leaf_depth.get_or_insert(depth) != depth {
                    return Err(page::corrupt("leaves at different depths").into());
                }
                leaves.push(id);
            }
            Node::Internal { keys, children } => {
                if children.len() != keys.len() + 1 || keys.is_empty() {
                    return Err(page::corrupt(&format!("page {id}: bad fan-out")).into());
                }
                for (i, &c) in children.iter().enumerate() {
                    let clo = if i == 0 {
                        lo.clone()
                    } else {
                        Bound::Included(keys[i - 1].clone())
                    };
                    let chi = if i == keys.len() {
                        hi.clone()
                    } else {
                        Bound::Excluded(keys[i].clone())
                    };
                    self.verify_node(c, clo, chi, depth + 1, leaf_depth, leaves, seen)?;
                }
            }
        }
        Ok(())
    }
}

/// Index of the child that may contain `key`: number of separators `<= key`.
fn child_index(keys: &[Vec<u8>], key: &[u8]) -> usize {
    keys.partition_point(|k| k.as_slice() <= key)
}

/// A write transaction. Changes are buffered until the closure returns `Ok`.
pub struct Tx<'a> {
    db: &'a mut Db,
    dirty: HashMap<u32, Node>,
    meta: Meta,
}

impl Tx<'_> {
    fn node(&mut self, id: u32) -> Result<Node> {
        if let Some(n) = self.dirty.get(&id) {
            return Ok(n.clone());
        }
        Ok(self.db.load(id)?.clone())
    }

    fn alloc(&mut self) -> u32 {
        let id = self.meta.page_count;
        self.meta.page_count += 1;
        id
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut id = self.meta.root;
        loop {
            match self.node(id)? {
                Node::Internal { keys, children } => id = children[child_index(&keys, key)],
                Node::Leaf { keys, vals, .. } => {
                    return Ok(keys
                        .binary_search_by(|k| k.as_slice().cmp(key))
                        .ok()
                        .map(|i| vals[i].clone()))
                }
            }
        }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        if key.len() > MAX_KEY {
            return Err(Error::KeyTooLarge(key.len()));
        }
        if val.len() > MAX_VALUE {
            return Err(Error::ValueTooLarge(val.len()));
        }
        let root = self.meta.root;
        if let Some((sep, right)) = self.insert(root, key, val)? {
            let new_root = self.alloc();
            self.dirty.insert(
                new_root,
                Node::Internal {
                    keys: vec![sep],
                    children: vec![root, right],
                },
            );
            self.meta.root = new_root;
        }
        Ok(())
    }

    fn insert(&mut self, id: u32, key: &[u8], val: &[u8]) -> Result<Option<(Vec<u8>, u32)>> {
        match self.node(id)? {
            Node::Leaf {
                mut keys,
                mut vals,
                next,
            } => {
                match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                    Ok(i) => vals[i] = val.to_vec(),
                    Err(i) => {
                        keys.insert(i, key.to_vec());
                        vals.insert(i, val.to_vec());
                    }
                }
                let node = Node::Leaf { keys, vals, next };
                if !node.overflows() {
                    self.dirty.insert(id, node);
                    return Ok(None);
                }
                let Node::Leaf {
                    mut keys,
                    mut vals,
                    next,
                } = node
                else {
                    unreachable!()
                };
                let sizes: Vec<usize> = keys
                    .iter()
                    .zip(&vals)
                    .map(|(k, v)| page::leaf_entry_len(k, v))
                    .collect();
                let m = split_point(&sizes);
                let rk = keys.split_off(m);
                let rv = vals.split_off(m);
                let right = self.alloc();
                let sep = rk[0].clone();
                self.dirty.insert(
                    right,
                    Node::Leaf {
                        keys: rk,
                        vals: rv,
                        next,
                    },
                );
                self.dirty.insert(
                    id,
                    Node::Leaf {
                        keys,
                        vals,
                        next: right,
                    },
                );
                Ok(Some((sep, right)))
            }
            Node::Internal {
                mut keys,
                mut children,
            } => {
                let i = child_index(&keys, key);
                let Some((sep, new_child)) = self.insert(children[i], key, val)? else {
                    return Ok(None);
                };
                keys.insert(i, sep);
                children.insert(i + 1, new_child);
                let node = Node::Internal { keys, children };
                if !node.overflows() {
                    self.dirty.insert(id, node);
                    return Ok(None);
                }
                let Node::Internal {
                    mut keys,
                    mut children,
                } = node
                else {
                    unreachable!()
                };
                let sizes: Vec<usize> = keys.iter().map(|k| page::internal_entry_len(k)).collect();
                let m = split_point(&sizes).min(keys.len() - 2);
                // keys[m] moves up; left keeps keys[..m] and children[..=m].
                let mut rk = keys.split_off(m);
                let promoted = rk.remove(0);
                let rc = children.split_off(m + 1);
                let right = self.alloc();
                self.dirty.insert(
                    right,
                    Node::Internal {
                        keys: rk,
                        children: rc,
                    },
                );
                self.dirty.insert(id, Node::Internal { keys, children });
                Ok(Some((promoted, right)))
            }
        }
    }

    /// Removes `key`. Leaves are not merged; empty leaves stay in the chain.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let mut id = self.meta.root;
        loop {
            match self.node(id)? {
                Node::Internal { keys, children } => id = children[child_index(&keys, key)],
                Node::Leaf {
                    mut keys,
                    mut vals,
                    next,
                } => {
                    let Ok(i) = keys.binary_search_by(|k| k.as_slice().cmp(key)) else {
                        return Ok(false);
                    };
                    keys.remove(i);
                    vals.remove(i);
                    self.dirty.insert(id, Node::Leaf { keys, vals, next });
                    return Ok(true);
                }
            }
        }
    }
}

/// First index of the right half when splitting entries of the given sizes
/// roughly in half by bytes; both halves are non-empty.
fn split_point(sizes: &[usize]) -> usize {
    let total: usize = sizes.iter().sum();
    let mut acc = 0;
    for (i, s) in sizes.iter().enumerate() {
        acc += s;
        if acc * 2 >= total {
            return (i + 1).clamp(1, sizes.len() - 1);
        }
    }
    sizes.len() - 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_point_is_balanced_and_nonempty() {
        assert_eq!(split_point(&[10, 10, 10, 10]), 2);
        assert_eq!(split_point(&[1000, 1, 1]), 1);
        assert_eq!(split_point(&[1, 1, 1000]), 2);
        assert_eq!(split_point(&[5, 5]), 1);
    }

    #[test]
    fn child_index_routes_equal_keys_right() {
        let keys = vec![b"g".to_vec(), b"p".to_vec()];
        assert_eq!(child_index(&keys, b"a"), 0);
        assert_eq!(child_index(&keys, b"g"), 1);
        assert_eq!(child_index(&keys, b"h"), 1);
        assert_eq!(child_index(&keys, b"p"), 2);
        assert_eq!(child_index(&keys, b"z"), 2);
    }
}
