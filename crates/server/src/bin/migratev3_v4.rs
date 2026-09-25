//! `migratev3_v4` — convert an `fs/` tree from schema v3 to v4, once, by hand,
//! with every filesystem client stopped; then delete this tool.
//!
//! v4 adds two fields to `InodeMeta` (`generation`, `segments`), which moves
//! its rkyv layout, so every `[0x01][ino]` value is rewritten: decoded with
//! the vendored v3 definition below, re-encoded as v4 with `generation = 1`
//! and `segments = None`. Nothing else changes — dirents, extents, the stripe
//! declaration, inode numbers and link counts are untouched — and the tree's
//! `[0x04]schema_version` stamp moves from 3 to 4 last, so no v4 binary mounts
//! a half-converted tree and no v3 binary mounts a converted one.
//!
//! Interrupted runs resume: after each page the last converted key is written
//! to `[0x04]migrate_v4_cursor`, and a rerun continues after it rather than
//! decoding already-converted values as v3. There is no guessing from the
//! bytes which layout a value has — the stamp and the cursor say.
//!
//! ```text
//! migratev3_v4 --manager HOST:PORT [--credential-file F] [--dry-run] [--unstamped-is-v3]
//! ```

use anyhow::{anyhow, bail, Context, Result};
use rkyv::{Archive, Deserialize, Serialize};

use autumn_client::ClusterClient;
use autumn_fuse::key;
use autumn_fuse::schema::{self, InodeMeta, StripeLayout};

/// `InodeMeta` as schema v3 stored it. rkyv's layout depends on the field
/// list, not the type name, so this decodes v3 bytes exactly.
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct InodeMetaV3 {
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    nlink: u32,
    atime_secs: i64,
    atime_nsecs: u32,
    mtime_secs: i64,
    mtime_nsecs: u32,
    ctime_secs: i64,
    ctime_nsecs: u32,
    inline_data: Option<Vec<u8>>,
    symlink_target: Option<Vec<u8>>,
    stripe: Option<StripeLayout>,
}

fn convert(v3: InodeMetaV3) -> InodeMeta {
    InodeMeta {
        mode: v3.mode,
        uid: v3.uid,
        gid: v3.gid,
        size: v3.size,
        nlink: v3.nlink,
        atime_secs: v3.atime_secs,
        atime_nsecs: v3.atime_nsecs,
        mtime_secs: v3.mtime_secs,
        mtime_nsecs: v3.mtime_nsecs,
        ctime_secs: v3.ctime_secs,
        ctime_nsecs: v3.ctime_nsecs,
        inline_data: v3.inline_data,
        symlink_target: v3.symlink_target,
        stripe: v3.stripe,
        generation: 1,
        segments: None,
    }
}

/// Whether a stored value is already a v4 inode: it validates as v4 AND
/// re-encodes to exactly these bytes. Needed for one window only — a page
/// written but its cursor not yet — where a rerun meets converted values after
/// the cursor. Decoding those as v3 either errors or yields garbage, so each
/// value is checked rather than assumed; v3 bytes never re-encode identically
/// as v4 because the v4 layout is longer.
fn is_v4(bytes: &[u8]) -> bool {
    schema::decode_inode_meta(bytes).is_ok_and(|m| schema::encode_inode_meta(&m) == bytes)
}

/// The value to write for a stored inode value, or `None` if it is already v4.
fn convert_value(bytes: &[u8]) -> Result<Option<Vec<u8>>> {
    if is_v4(bytes) {
        return Ok(None);
    }
    convert_bytes(bytes).map(Some)
}

fn convert_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let v3: InodeMetaV3 = autumn_rpc::partition_rpc::rkyv_decode(bytes).map_err(|e| anyhow!("{e:?}"))?;
    Ok(schema::encode_inode_meta(&convert(v3)))
}

fn cursor_key() -> Vec<u8> {
    key::super_key(b"migrate_v4_cursor")
}

async fn stamp(c: &ClusterClient) -> Result<Option<u64>> {
    match c.get(&key::schema_version_key()).await.map_err(|e| anyhow!("{e}"))? {
        None => Ok(None),
        Some(v) => Ok(Some(u64::from_be_bytes(
            v.as_slice().try_into().map_err(|_| anyhow!("schema_version is {} bytes", v.len()))?,
        ))),
    }
}

async fn run(c: &ClusterClient, dry_run: bool, unstamped_is_v3: bool) -> Result<()> {
    match stamp(c).await? {
        Some(4) => {
            println!("already v4; nothing to do");
            return Ok(());
        }
        Some(3) => {}
        Some(v) => bail!("fs schema is v{v}, this tool converts v3 only"),
        // Before v4 only the fuse mount and the Python binding stamped; a
        // tree built by `autumnfs` or the S3 gateway alone has v3 inodes and
        // no stamp. Converting it is right only if that is what it is, so the
        // operator says so.
        None if unstamped_is_v3 => println!("no schema stamp; treating the tree as v3 as instructed"),
        None => bail!(
            "fs has no schema stamp. If it holds inodes, they were written by autumnfs or the \
             S3 gateway before v4 and are v3: rerun with --unstamped-is-v3"
        ),
    }
    let prefix = vec![0x01u8];
    let mut start = match c.get(&cursor_key()).await.map_err(|e| anyhow!("{e}"))? {
        Some(last) => {
            println!("resuming after {}", hex(&last));
            autumn_fuse::dir::name_successor(&last)
        }
        _ => prefix.clone(),
    };
    let (mut converted, mut pages) = (0u64, 0u64);
    loop {
        let r = c.range(&prefix, &start, 512).await.map_err(|e| anyhow!("range: {e}"))?;
        let Some(last) = r.entries.last().map(|e| e.key.clone()) else { break };
        let keys: Vec<&[u8]> = r.entries.iter().map(|e| e.key.as_slice()).collect();
        let values = c.get_many(&keys).await;
        let mut items: Vec<(&[u8], bytes::Bytes, u64)> = Vec::with_capacity(keys.len());
        for (k, v) in keys.iter().zip(values) {
            let Some(v) = v.map_err(|e| anyhow!("get {}: {e}", hex(k)))? else { continue };
            let Some(new) = convert_value(&v).with_context(|| format!("inode key {} is not a v3 InodeMeta", hex(k)))? else {
                continue;
            };
            items.push((k, bytes::Bytes::from(new), 0));
        }
        converted += items.len() as u64;
        if !dry_run {
            for res in c.put_many(&items).await {
                res.map_err(|e| anyhow!("put: {e}"))?;
            }
            c.put(&cursor_key(), &last).await.map_err(|e| anyhow!("cursor: {e}"))?;
        }
        pages += 1;
        if pages % 20 == 0 {
            println!("  {converted} inodes");
        }
        if !r.has_more {
            break;
        }
        start = autumn_fuse::dir::name_successor(&last);
    }
    if dry_run {
        println!("dry run: {converted} inodes decode as v3 and would convert; nothing written");
        return Ok(());
    }
    c.put(&key::schema_version_key(), &4u64.to_be_bytes()).await.map_err(|e| anyhow!("stamp: {e}"))?;
    c.delete(&cursor_key()).await.map_err(|e| anyhow!("cursor delete: {e}"))?;
    println!("converted {converted} inodes; fs schema is now v4");
    Ok(())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn main() -> Result<()> {
    let mut manager = String::new();
    let mut cred: Option<String> = None;
    let mut dry_run = false;
    let mut unstamped_is_v3 = false;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--manager" => manager = it.next().context("--manager needs a value")?,
            "--credential-file" => cred = Some(it.next().context("--credential-file needs a value")?),
            "--dry-run" => dry_run = true,
            "--unstamped-is-v3" => unstamped_is_v3 = true,
            other => bail!("unknown argument {other}"),
        }
    }
    if manager.is_empty() {
        bail!("--manager is required");
    }
    compio::runtime::Runtime::new()?.block_on(async move {
        let c = match cred {
            Some(p) => {
                let (principal, secret) = autumn_client::read_credential_file(&p)?;
                ClusterClient::connect_with_credential(&manager, "fs", principal, secret).await?
            }
            None => ClusterClient::connect(&manager, "fs").await?,
        };
        run(&c, dry_run, unstamped_is_v3).await
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v3() -> InodeMetaV3 {
        InodeMetaV3 {
            mode: 0o100644,
            uid: 1,
            gid: 2,
            size: 12345,
            nlink: 2,
            atime_secs: 10,
            atime_nsecs: 11,
            mtime_secs: 12,
            mtime_nsecs: 13,
            ctime_secs: 14,
            ctime_nsecs: 15,
            inline_data: Some(b"inline".to_vec()),
            symlink_target: None,
            stripe: Some(StripeLayout { lanes: 24, unit_bytes: 8 << 20 }),
        }
    }

    #[test]
    fn every_v3_field_survives_and_v4_fields_start_fresh() {
        let old = v3();
        let bytes = autumn_rpc::partition_rpc::rkyv_encode(&old).to_vec();
        let new = schema::decode_inode_meta(&convert_bytes(&bytes).unwrap()).unwrap();
        assert_eq!(
            (new.mode, new.uid, new.gid, new.size, new.nlink),
            (old.mode, old.uid, old.gid, old.size, old.nlink)
        );
        assert_eq!(
            (new.atime_secs, new.atime_nsecs, new.mtime_secs, new.mtime_nsecs, new.ctime_secs, new.ctime_nsecs),
            (10, 11, 12, 13, 14, 15)
        );
        assert_eq!(new.inline_data, old.inline_data);
        assert_eq!(new.stripe, old.stripe);
        assert_eq!((new.generation, new.segments), (1, None));
    }

    /// The resume window: a page converted but its cursor not yet written.
    /// A rerun must leave converted values alone and convert only v3 ones.
    #[test]
    fn a_rerun_skips_converted_values_and_converts_the_rest() {
        let v3_bytes = autumn_rpc::partition_rpc::rkyv_encode(&v3()).to_vec();
        let v4_bytes = convert_bytes(&v3_bytes).unwrap();
        assert!(!is_v4(&v3_bytes), "v3 bytes are not taken for v4");
        assert!(is_v4(&v4_bytes));
        assert_eq!(convert_value(&v4_bytes).unwrap(), None, "converted once, never twice");
        assert_eq!(convert_value(&v3_bytes).unwrap(), Some(v4_bytes));
        // Other shapes of v3 inode too: a directory, a symlink, no stripe.
        for m in [
            InodeMetaV3 { inline_data: None, stripe: None, mode: 0o040755, ..v3() },
            InodeMetaV3 { symlink_target: Some(b"/x".to_vec()), inline_data: None, ..v3() },
        ] {
            let b = autumn_rpc::partition_rpc::rkyv_encode(&m).to_vec();
            assert!(!is_v4(&b));
            assert!(is_v4(&convert_value(&b).unwrap().unwrap()));
        }
    }
}
