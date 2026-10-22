//! Fixed-size page encoding: B+tree nodes and the meta page, each with a CRC.

use std::io;

pub const PAGE_SIZE: usize = 4096;
pub const MAX_KEY: usize = 128;
pub const MAX_VALUE: usize = 1024;

const LEAF: u8 = 1;
const INTERNAL: u8 = 2;
const NODE_HEADER: usize = 1 + 2 + 4;
const CRC_LEN: usize = 4;
/// Bytes available for entries in one node page.
pub const NODE_CAPACITY: usize = PAGE_SIZE - NODE_HEADER - CRC_LEN;

const META_MAGIC: &[u8; 8] = b"BPTWAL01";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    Leaf {
        keys: Vec<Vec<u8>>,
        vals: Vec<Vec<u8>>,
        next: u32,
    },
    /// `children.len() == keys.len() + 1`; child `i` holds keys in `[keys[i-1], keys[i])`.
    Internal {
        keys: Vec<Vec<u8>>,
        children: Vec<u32>,
    },
}

impl Node {
    pub fn empty_leaf() -> Node {
        Node::Leaf {
            keys: Vec::new(),
            vals: Vec::new(),
            next: 0,
        }
    }

    /// Encoded size of the entries (excluding header and CRC).
    pub fn payload_len(&self) -> usize {
        match self {
            Node::Leaf { keys, vals, .. } => keys
                .iter()
                .zip(vals)
                .map(|(k, v)| leaf_entry_len(k, v))
                .sum(),
            Node::Internal { keys, .. } => keys.iter().map(|k| internal_entry_len(k)).sum(),
        }
    }

    pub fn overflows(&self) -> bool {
        self.payload_len() > NODE_CAPACITY
    }

    pub fn encode(&self) -> Box<[u8; PAGE_SIZE]> {
        let mut p = Box::new([0u8; PAGE_SIZE]);
        let mut w = 0usize;
        let mut put = |bytes: &[u8]| {
            p[w..w + bytes.len()].copy_from_slice(bytes);
            w += bytes.len();
        };
        match self {
            Node::Leaf { keys, vals, next } => {
                put(&[LEAF]);
                put(&(keys.len() as u16).to_le_bytes());
                put(&next.to_le_bytes());
                for (k, v) in keys.iter().zip(vals) {
                    put(&(k.len() as u16).to_le_bytes());
                    put(&(v.len() as u16).to_le_bytes());
                    put(k);
                    put(v);
                }
            }
            Node::Internal { keys, children } => {
                put(&[INTERNAL]);
                put(&(keys.len() as u16).to_le_bytes());
                put(&children[0].to_le_bytes());
                for (k, c) in keys.iter().zip(&children[1..]) {
                    put(&(k.len() as u16).to_le_bytes());
                    put(k);
                    put(&c.to_le_bytes());
                }
            }
        }
        assert!(w <= PAGE_SIZE - CRC_LEN, "node overflows its page");
        let crc = crc32fast::hash(&p[..PAGE_SIZE - CRC_LEN]);
        p[PAGE_SIZE - CRC_LEN..].copy_from_slice(&crc.to_le_bytes());
        p
    }

    pub fn decode(p: &[u8; PAGE_SIZE]) -> io::Result<Node> {
        let crc = u32::from_le_bytes(p[PAGE_SIZE - CRC_LEN..].try_into().unwrap());
        if crc != crc32fast::hash(&p[..PAGE_SIZE - CRC_LEN]) {
            return Err(corrupt("node page checksum mismatch"));
        }
        let mut r = Reader {
            p: &p[..PAGE_SIZE - CRC_LEN],
            at: 0,
        };
        let kind = r.u8()?;
        let n = r.u16()? as usize;
        let first = r.u32()?;
        match kind {
            LEAF => {
                let (mut keys, mut vals) = (Vec::with_capacity(n), Vec::with_capacity(n));
                for _ in 0..n {
                    let kl = r.u16()? as usize;
                    let vl = r.u16()? as usize;
                    keys.push(r.bytes(kl)?.to_vec());
                    vals.push(r.bytes(vl)?.to_vec());
                }
                Ok(Node::Leaf {
                    keys,
                    vals,
                    next: first,
                })
            }
            INTERNAL => {
                let mut keys = Vec::with_capacity(n);
                let mut children = Vec::with_capacity(n + 1);
                children.push(first);
                for _ in 0..n {
                    let kl = r.u16()? as usize;
                    keys.push(r.bytes(kl)?.to_vec());
                    children.push(r.u32()?);
                }
                Ok(Node::Internal { keys, children })
            }
            k => Err(corrupt(&format!("unknown node kind {k}"))),
        }
    }
}

pub fn leaf_entry_len(k: &[u8], v: &[u8]) -> usize {
    4 + k.len() + v.len()
}

pub fn internal_entry_len(k: &[u8]) -> usize {
    2 + k.len() + 4
}

/// Page 0: tree root, allocated page count and the id of the last committed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Meta {
    pub root: u32,
    pub page_count: u32,
    pub txn: u64,
}

impl Meta {
    pub fn encode(&self) -> Box<[u8; PAGE_SIZE]> {
        let mut p = Box::new([0u8; PAGE_SIZE]);
        p[..8].copy_from_slice(META_MAGIC);
        p[8..12].copy_from_slice(&self.root.to_le_bytes());
        p[12..16].copy_from_slice(&self.page_count.to_le_bytes());
        p[16..24].copy_from_slice(&self.txn.to_le_bytes());
        let crc = crc32fast::hash(&p[..24]);
        p[24..28].copy_from_slice(&crc.to_le_bytes());
        p
    }

    pub fn decode(p: &[u8; PAGE_SIZE]) -> io::Result<Meta> {
        if &p[..8] != META_MAGIC {
            return Err(corrupt("bad magic: not a bptree-wal data file"));
        }
        let crc = u32::from_le_bytes(p[24..28].try_into().unwrap());
        if crc != crc32fast::hash(&p[..24]) {
            return Err(corrupt("meta page checksum mismatch"));
        }
        Ok(Meta {
            root: u32::from_le_bytes(p[8..12].try_into().unwrap()),
            page_count: u32::from_le_bytes(p[12..16].try_into().unwrap()),
            txn: u64::from_le_bytes(p[16..24].try_into().unwrap()),
        })
    }
}

pub fn corrupt(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

struct Reader<'a> {
    p: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .filter(|&e| e <= self.p.len())
            .ok_or_else(|| corrupt("truncated node"))?;
        let s = &self.p[self.at..end];
        self.at = end;
        Ok(s)
    }
    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.bytes(1)?[0])
    }
    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_roundtrip_and_checksum() {
        let n = Node::Leaf {
            keys: vec![b"a".to_vec(), b"bb".to_vec()],
            vals: vec![b"1".to_vec(), vec![]],
            next: 9,
        };
        let mut p = n.encode();
        assert_eq!(Node::decode(&p).unwrap(), n);
        p[20] ^= 1;
        assert!(Node::decode(&p).is_err());
    }

    #[test]
    fn internal_roundtrip() {
        let n = Node::Internal {
            keys: vec![b"m".to_vec(), b"t".to_vec()],
            children: vec![3, 4, 5],
        };
        assert_eq!(Node::decode(&n.encode()).unwrap(), n);
    }

    #[test]
    fn meta_roundtrip_and_magic() {
        let m = Meta {
            root: 7,
            page_count: 12,
            txn: 99,
        };
        assert_eq!(Meta::decode(&m.encode()).unwrap(), m);
        let mut p = m.encode();
        p[0] = b'X';
        assert!(Meta::decode(&p).is_err());
    }

    #[test]
    fn largest_entries_fit_twice() {
        // A split must always leave room for at least two maximal entries per node.
        assert!(2 * leaf_entry_len(&[0; MAX_KEY], &[0; MAX_VALUE]) <= NODE_CAPACITY);
        assert!(2 * internal_entry_len(&[0; MAX_KEY]) <= NODE_CAPACITY);
    }
}
