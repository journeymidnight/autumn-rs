//! KV key encoding/decoding for the FUSE filesystem.
//!
//! Key layout uses a single-byte type prefix + big-endian u64 fields for natural sort order.
//!
//! | Prefix | Purpose       | Key format                                |
//! |--------|---------------|-------------------------------------------|
//! | 0x01   | Inode meta    | [0x01][ino: u64 BE]                       |
//! | 0x02   | Dir entry     | [0x02][parent_ino: u64 BE][name bytes]    |
//! | 0x03   | File extent   | [0x03][ino: u64 BE][logical_off: u64 BE]  |
//! | 0x04   | FS superblock | [0x04][field bytes]                       |
//!
//! F247: data is stored as **variable-length extents** keyed by their logical
//! byte offset (was fixed 256 KiB chunks keyed by chunk index). BigEndian
//! `logical_off` keeps extents naturally sorted, so a range-scan of the
//! `[0x03][ino]` prefix returns a file's extents in offset order.

const PREFIX_INODE: u8 = 0x01;
const PREFIX_DIRENT: u8 = 0x02;
const PREFIX_EXTENT: u8 = 0x03;
const PREFIX_SUPER: u8 = 0x04;

/// Encode inode metadata key: `[0x01][ino BE]`
pub fn inode_key(ino: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(PREFIX_INODE);
    key.extend_from_slice(&ino.to_be_bytes());
    key
}

/// Decode inode number from an inode key.
#[cfg(test)]
pub fn parse_inode_key(key: &[u8]) -> Option<u64> {
    if key.len() == 9 && key[0] == PREFIX_INODE {
        Some(u64::from_be_bytes(key[1..9].try_into().unwrap()))
    } else {
        None
    }
}

/// Encode directory entry key: `[0x02][parent_ino BE][name]`
pub fn dirent_key(parent_ino: u64, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(9 + name.len());
    key.push(PREFIX_DIRENT);
    key.extend_from_slice(&parent_ino.to_be_bytes());
    key.extend_from_slice(name);
    key
}

/// Encode directory entry prefix for Range scan: `[0x02][parent_ino BE]`
pub fn dirent_prefix(parent_ino: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(PREFIX_DIRENT);
    key.extend_from_slice(&parent_ino.to_be_bytes());
    key
}

/// Parse directory entry key → (parent_ino, name bytes).
pub fn parse_dirent_key(key: &[u8]) -> Option<(u64, &[u8])> {
    if key.len() >= 9 && key[0] == PREFIX_DIRENT {
        let parent = u64::from_be_bytes(key[1..9].try_into().unwrap());
        let name = &key[9..];
        Some((parent, name))
    } else {
        None
    }
}

/// Encode file data extent key: `[0x03][ino BE][logical_off BE]` (F247).
///
/// `logical_off` is the byte offset in the file where this extent's data
/// begins (NOT a chunk index). The extent's value length is variable (≤
/// `MAX_EXTENT`).
pub fn extent_key(ino: u64, logical_off: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(PREFIX_EXTENT);
    key.extend_from_slice(&ino.to_be_bytes());
    key.extend_from_slice(&logical_off.to_be_bytes());
    key
}

/// Encode the extent prefix for a Range scan over a file's extents:
/// `[0x03][ino BE]`.
pub fn extent_prefix(ino: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(PREFIX_EXTENT);
    key.extend_from_slice(&ino.to_be_bytes());
    key
}

/// Parse an extent key → (ino, logical_off).
pub fn parse_extent_key(key: &[u8]) -> Option<(u64, u64)> {
    if key.len() == 17 && key[0] == PREFIX_EXTENT {
        let ino = u64::from_be_bytes(key[1..9].try_into().unwrap());
        let logical_off = u64::from_be_bytes(key[9..17].try_into().unwrap());
        Some((ino, logical_off))
    } else {
        None
    }
}

// ── F-FS-STRIPE: lane-striped extent keys ────────────────────────────────────
// A large file's extents are spread across `lanes` partitions so a single file's
// writes/reads parallelise beyond one partition/log_stream. The lane byte sits
// HIGH (right after 0x03) so it dominates partition routing; lane boundaries
// `[0x03][lane]` are STATIC (ino-independent), so fs can be pre-split into lane
// partitions at bootstrap without knowing any ino.

/// Which lane an extent at `logical_off` lives on: `(off / unit_bytes) % lanes`.
/// `lanes` must be ≥ 1 and `unit_bytes` ≥ 1 (validated by the writer).
pub fn stripe_lane(logical_off: u64, lanes: u8, unit_bytes: u32) -> u8 {
    ((logical_off / unit_bytes as u64) % lanes as u64) as u8
}

/// Encode a STRIPED file data extent key: `[0x03][lane][ino BE][logical_off BE]`
/// (18 B). `lane = stripe_lane(logical_off, lanes, unit_bytes)`.
pub fn extent_key_striped(ino: u64, logical_off: u64, lanes: u8, unit_bytes: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(18);
    key.push(PREFIX_EXTENT);
    key.push(stripe_lane(logical_off, lanes, unit_bytes));
    key.extend_from_slice(&ino.to_be_bytes());
    key.extend_from_slice(&logical_off.to_be_bytes());
    key
}

/// Parse a STRIPED extent key → (lane, ino, logical_off).
pub fn parse_striped_extent_key(key: &[u8]) -> Option<(u8, u64, u64)> {
    if key.len() == 18 && key[0] == PREFIX_EXTENT {
        let lane = key[1];
        let ino = u64::from_be_bytes(key[2..10].try_into().unwrap());
        let logical_off = u64::from_be_bytes(key[10..18].try_into().unwrap());
        Some((lane, ino, logical_off))
    } else {
        None
    }
}

/// The lane-partition split boundary (RELATIVE key `[0x03][lane]`). Splitting the
/// fs partition at each of `[0x03][1]..[0x03][N]` yields N lane partitions:
/// lane 0 = `[start, [0x03][1])` (also holds 0x01/0x02 metadata + legacy
/// non-striped `[0x03][ino…]` data, which all sort here), lane L = the L-th slice.
pub fn stripe_lane_boundary(lane: u8) -> Vec<u8> {
    vec![PREFIX_EXTENT, lane]
}

/// Encode superblock field key: `[0x04][field]`
pub fn super_key(field: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + field.len());
    key.push(PREFIX_SUPER);
    key.extend_from_slice(field);
    key
}

/// Well-known superblock key for the next inode counter.
pub fn next_inode_key() -> Vec<u8> {
    super_key(b"next_inode")
}

/// Well-known superblock key holding this tenant's on-disk layout version
/// (`schema::SCHEMA_VERSION`, BE u64). Relative like every other key, so it lives
/// at `fs/{tenant}/[0x04]schema_version` — each tenant stamps + verifies its own
/// layout independently.
pub fn schema_version_key() -> Vec<u8> {
    super_key(b"schema_version")
}

/// F-FS-GEOM-DECLARED: the fs-wide DECLARED stripe geometry
/// (`[0x04]stripe_geom` → rkyv `StripeLayout`).
///
/// Geometry is a policy value and this is its authoritative home. It used to
/// have none: `autumnfs` reverse-engineered the lane count from the CURRENT
/// partition split points on every large-file create. Partitions are the
/// manager's elastic placement unit — it splits, merges and rebalances them
/// autonomously — so deriving a durable per-file property from a mutable
/// placement artifact drifted silently (a merged lane boundary narrowed every
/// subsequent file with no error; a transient lookup failure degraded to
/// `lanes=1` permanently for that file).
///
/// Absent key = this fs is not striped (new files stay single-partition). It
/// lives beside `[0x04]schema_version` so it is read by the same fail-loud
/// mount-time sequence, and it is a NEW key — no existing value layout changes,
/// so adopting it needs no schema bump and no reset.
pub fn stripe_geom_key() -> Vec<u8> {
    super_key(b"stripe_geom")
}

/// UNLINK-1: unlink-intent tombstone `[0x04]rmtomb/[ino BE]`. Written the
/// moment an inode becomes UNREACHABLE (its last dirent gone) and removed
/// after its extents + inode key are deleted; the mount-time sweep replays
/// any survivor so a crash mid-unlink can no longer leak the file's data
/// KVs forever. INVARIANT: a tombstone is only ever written for an
/// unreachable inode — the sweep deletes data unconditionally.
pub fn unlink_tombstone_key(ino: u64) -> Vec<u8> {
    let mut k = super_key(b"rmtomb/");
    k.extend_from_slice(&ino.to_be_bytes());
    k
}

pub fn unlink_tombstone_prefix() -> Vec<u8> {
    super_key(b"rmtomb/")
}

pub fn parse_unlink_tombstone(key: &[u8]) -> Option<u64> {
    let p = unlink_tombstone_prefix();
    if key.len() == p.len() + 8 && key.starts_with(&p) {
        Some(u64::from_be_bytes(key[p.len()..].try_into().unwrap()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlink_tombstone_roundtrip() {
        let k = unlink_tombstone_key(0xDEAD_BEEF);
        assert!(k.starts_with(&unlink_tombstone_prefix()));
        assert_eq!(parse_unlink_tombstone(&k), Some(0xDEAD_BEEF));
        // wrong length / prefix
        assert_eq!(parse_unlink_tombstone(&unlink_tombstone_prefix()), None);
        assert_eq!(parse_unlink_tombstone(&next_inode_key()), None);
        // must not collide with other super keys under prefix scan
        assert!(!next_inode_key().starts_with(&unlink_tombstone_prefix()));
    }

    #[test]
    fn inode_key_roundtrip() {
        let key = inode_key(42);
        assert_eq!(parse_inode_key(&key), Some(42));
    }

    #[test]
    fn super_keys_are_distinct() {
        // Both are superblock keys but must not collide with each other or the
        // rmtomb prefix (all live under 0x04).
        let ni = next_inode_key();
        let sv = schema_version_key();
        assert_ne!(ni, sv);
        assert!(sv.starts_with(&[PREFIX_SUPER]));
        assert!(!sv.starts_with(&unlink_tombstone_prefix()));
        assert_eq!(parse_unlink_tombstone(&sv), None);
    }

    #[test]
    fn dirent_key_roundtrip() {
        let key = dirent_key(1, b"hello.txt");
        let (parent, name) = parse_dirent_key(&key).unwrap();
        assert_eq!(parent, 1);
        assert_eq!(name, b"hello.txt");
    }

    #[test]
    fn extent_key_roundtrip() {
        // logical_off, not a chunk index
        let key = extent_key(100, 8 * 1024 * 1024);
        let (ino, off) = parse_extent_key(&key).unwrap();
        assert_eq!(ino, 100);
        assert_eq!(off, 8 * 1024 * 1024);
    }

    #[test]
    fn striped_extent_offsets_bounded_and_correct() {
        use crate::schema::{striped_extent_offsets, MAX_EXTENT};
        let u = MAX_EXTENT as u64;
        let uu = MAX_EXTENT as u32;
        assert_eq!(striped_extent_offsets(0, uu).unwrap(), Vec::<u64>::new());
        assert_eq!(striped_extent_offsets(1, uu).unwrap(), vec![0]); // 1 byte → 1 extent
        assert_eq!(striped_extent_offsets(u, uu).unwrap(), vec![0]); // exactly one full extent
        assert_eq!(striped_extent_offsets(u + 1, uu).unwrap(), vec![0, u]); // spills to 2nd
        assert_eq!(striped_extent_offsets(3 * u, uu).unwrap(), vec![0, u, 2 * u]);
        // coco P3: a corrupt near-u64::MAX size errors instead of wrapping / OOMing.
        assert!(striped_extent_offsets(u64::MAX, uu).is_err());
        // A corrupt stamp must error, not divide by zero.
        assert!(striped_extent_offsets(1024, 0).is_err());
    }

    #[test]
    fn stripe_geom_key_is_a_superblock_key_below_every_lane() {
        use crate::schema::MAX_EXTENT;
        let k = stripe_geom_key();
        assert_eq!(k[0], PREFIX_SUPER);
        // It must NOT collide with the other superblock fields.
        assert_ne!(k, schema_version_key());
        assert_ne!(k, next_inode_key());
        assert!(!k.starts_with(&unlink_tombstone_prefix()));
        // Superblock (0x04) sorts ABOVE every extent key (0x03), so declaring
        // geometry never lands inside a lane partition — it stays with the rest
        // of the metadata and is reachable no matter how fs is presplit.
        assert!(k > extent_key_striped(u64::MAX, 0, 255, MAX_EXTENT as u32));
    }

    #[test]
    fn stripe_geom_codec_round_trips_and_rejects_corruption() {
        use crate::schema::{decode_stripe_geom, encode_stripe_geom, StripeLayout};
        let g = StripeLayout { lanes: 6, unit_bytes: 8 << 20 };
        assert_eq!(decode_stripe_geom(&encode_stripe_geom(&g)).unwrap(), g);
        // An absent key is the caller's `Ok(None)`; an EMPTY value is corruption.
        assert!(decode_stripe_geom(&[]).is_err());
        assert!(decode_stripe_geom(b"not rkyv at all").is_err());
        // A zero in either field would divide by zero in `stripe_lane` /
        // `striped_extent_offsets`, so the decoder rejects it up front rather
        // than letting it reach the key builder.
        for bad in [
            StripeLayout { lanes: 0, unit_bytes: 8 << 20 },
            StripeLayout { lanes: 6, unit_bytes: 0 },
        ] {
            assert!(
                decode_stripe_geom(&encode_stripe_geom(&bad)).is_err(),
                "decoder accepted {bad:?}"
            );
        }
    }

    #[test]
    fn striped_offsets_follow_the_stamp_not_the_constant() {
        // THE regression this parameterisation exists for: a file written when
        // the extent cap was 8 MiB must keep enumerating on 8 MiB even after
        // MAX_EXTENT is retuned. Before the fix the stride came from the
        // constant, so a shrink made every striped file enumerate offsets that
        // do not match its written keys — and a missing extent reads as ZEROS
        // (sparse-file semantics), so the file silently came back half zero-filled.
        use crate::schema::striped_extent_offsets;
        const EIGHT_M: u32 = 8 * 1024 * 1024;
        const FOUR_M: u32 = 4 * 1024 * 1024;
        let size = 24 * 1024 * 1024u64; // 3 extents at 8 MiB, 6 at 4 MiB
        assert_eq!(
            striped_extent_offsets(size, EIGHT_M).unwrap(),
            vec![0, 8 << 20, 16 << 20],
            "an 8 MiB-stamped file must enumerate on 8 MiB whatever MAX_EXTENT is today"
        );
        assert_eq!(
            striped_extent_offsets(size, FOUR_M).unwrap().len(),
            6,
            "a 4 MiB-stamped file enumerates on 4 MiB"
        );
        // The two stampings genuinely disagree — which is exactly why reading the
        // stride from a global constant could not have been correct for both.
        assert_ne!(
            striped_extent_offsets(size, EIGHT_M).unwrap(),
            striped_extent_offsets(size, FOUR_M).unwrap()
        );
    }

    #[test]
    fn striped_extent_keys_round_trip_and_route_by_lane() {
        // F-FS-STRIPE: 4 lanes, unit = 8 MiB → extent e lands on lane e%4.
        let (lanes, unit) = (4u8, 8 * 1024 * 1024u32);
        for e in 0..12u64 {
            let off = e * unit as u64;
            let k = extent_key_striped(100, off, lanes, unit);
            assert_eq!(k.len(), 18);
            let (lane, ino, o) = parse_striped_extent_key(&k).unwrap();
            assert_eq!((lane, ino, o), ((e % 4) as u8, 100, off));
            // The lane byte sits at position 1 (right after 0x03) so it dominates
            // routing: the key falls in `[[0x03][lane], [0x03][lane+1])`.
            assert!(k.as_slice() >= stripe_lane_boundary(lane).as_slice());
            assert!(k.as_slice() < stripe_lane_boundary(lane + 1).as_slice());
        }
        // A non-striped (17 B) key never parses as striped and vice-versa.
        assert!(parse_striped_extent_key(&extent_key(100, 0)).is_none());
        assert!(parse_extent_key(&extent_key_striped(100, 0, 4, 4096)).is_none());
    }

    #[test]
    fn extent_prefix_is_key_floor() {
        let pre = extent_prefix(100);
        let k0 = extent_key(100, 0);
        let k1 = extent_key(100, 1);
        assert!(pre.as_slice() <= k0.as_slice());
        assert!(k0 < k1, "extents sort by logical offset");
        // prefix bytes match the key's first 9 bytes
        assert_eq!(&k0[..9], pre.as_slice());
    }

    #[test]
    fn dirent_keys_sort_by_parent_then_name() {
        let k1 = dirent_key(1, b"aaa");
        let k2 = dirent_key(1, b"bbb");
        let k3 = dirent_key(2, b"aaa");
        assert!(k1 < k2, "same parent, name a < b");
        assert!(k2 < k3, "parent 1 < parent 2");
    }

    #[test]
    fn extent_keys_sort_by_ino_then_offset() {
        let k1 = extent_key(10, 0);
        let k2 = extent_key(10, 8 * 1024 * 1024);
        let k3 = extent_key(11, 0);
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn type_prefixes_isolate_namespaces() {
        let ik = inode_key(1);
        let dk = dirent_key(1, b"x");
        let ck = extent_key(1, 0);
        let sk = next_inode_key();
        assert!(ik < dk, "inode prefix 0x01 < dirent prefix 0x02");
        assert!(dk < ck, "dirent prefix 0x02 < chunk prefix 0x03");
        assert!(ck < sk, "chunk prefix 0x03 < super prefix 0x04");
    }
}
