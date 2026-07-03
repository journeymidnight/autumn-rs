//! Generic node/edge graph stored as adjacency lists on the KV store.
//!
//! Pure codec only — the async graph methods live on `MemoryStore` (lib.rs) and
//! the key layout in `keys.rs`. A node record carries a `kind` (also indexed
//! under `nidx/` for by-kind listing) plus opaque `attrs` bytes; edges carry
//! opaque attr bytes on the authoritative forward key. Nothing here knows about
//! code / "Function" / "CALLS" — that schema lives in the consumer.

/// Authoritative node record, encoded as `[u32 kind_len LE][kind][attrs]`.
pub(crate) struct NodeRecord {
    pub kind: String,
    pub attrs: Vec<u8>,
}

impl NodeRecord {
    pub fn encode(&self) -> Vec<u8> {
        let kb = self.kind.as_bytes();
        let mut out = Vec::with_capacity(4 + kb.len() + self.attrs.len());
        out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        out.extend_from_slice(kb);
        out.extend_from_slice(&self.attrs);
        out
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < 4 {
            return None;
        }
        let kl = u32::from_le_bytes(b[..4].try_into().ok()?) as usize;
        let end = 4usize.checked_add(kl)?;
        if b.len() < end {
            return None;
        }
        Some(Self {
            kind: String::from_utf8_lossy(&b[4..end]).into_owned(),
            attrs: b[end..].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_record_roundtrip() {
        let n = NodeRecord {
            kind: "Function".into(),
            attrs: b"file=foo.rs;line=12".to_vec(),
        };
        let d = NodeRecord::decode(&n.encode()).unwrap();
        assert_eq!(d.kind, "Function");
        assert_eq!(d.attrs, b"file=foo.rs;line=12");

        // empty kind + empty attrs round-trip.
        let e = NodeRecord {
            kind: String::new(),
            attrs: Vec::new(),
        };
        let d2 = NodeRecord::decode(&e.encode()).unwrap();
        assert!(d2.kind.is_empty() && d2.attrs.is_empty());

        // malformed: too short, or a length that overruns the buffer.
        assert!(NodeRecord::decode(&[0, 0]).is_none());
        assert!(NodeRecord::decode(&[10, 0, 0, 0, b'x']).is_none());
    }
}
