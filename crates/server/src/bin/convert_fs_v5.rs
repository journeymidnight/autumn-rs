//! One-shot converter of the `fs/` tree from schema v4 to v5. **Run once per
//! cluster against a stopped filesystem, then delete this file** (and its
//! `[[bin]]` and `autumn_params::convert`).
//!
//! v5 moves the metadata behind a hash byte so it spreads over partitions:
//! an inode from `[0x01][ino]` to `[0x06][meta_hash(ino)][0x01][ino]`, a
//! directory entry from `[0x02][parent][name]` to
//! `[0x06][meta_hash(parent)][0x02][parent][name]`. Values do not change.
//!
//! The stamp goes to `SCHEMA_CONVERTING_TO_V5` first, so neither a v4 nor a
//! v5 build mounts the tree while keys move, and to 5 last. Each page of old
//! keys is put under its new key and then deleted; the regions never
//! overlap, so a run that stops anywhere is run again and picks up the rest.
//! Every mount, S3 gateway and `autumn.Fs` must be stopped first: one already
//! running checked the stamp when it started and would write v4 keys.
//!
//! ```bash
//! convert_fs_v5 --manager 127.0.0.1:9001 --dry-run
//! convert_fs_v5 --manager 127.0.0.1:9001
//! autumn-op presplit --namespace fs --parts N   # cut the metadata region
//! ```

use std::process::ExitCode;

use anyhow::{anyhow, bail, Context, Result};
use autumn_client::{ClusterClient, WriteOptions};
use autumn_fs::{key, schema};
use autumn_params::convert::ConvertFsParams;
use autumn_params::Positional;

const USAGE: &str = "usage: convert_fs_v5 [--manager <host:port>] [--credential-file <path>] [--dry-run]";
const V4_INODE: u8 = 0x01;
const V4_DIRENT: u8 = 0x02;
const PAGE: u32 = 512;

fn main() -> ExitCode {
    let p = ConvertFsParams::default();
    autumn_params::parse_or_exit(USAGE, &[&p], Positional::Refuse);
    let _ = autumn_transport::init_with(p.transport.get().into());
    let rt = match compio::runtime::RuntimeBuilder::new().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("convert_fs_v5: create runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(&p)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("convert_fs_v5: {e:#}");
            ExitCode::FAILURE
        }
    }
}

/// The v5 key of a v4 metadata key; `None` for a key of any other shape.
fn v5_key(old: &[u8]) -> Option<Vec<u8>> {
    let (&kind, rest) = old.split_first()?;
    if rest.len() < 8 {
        return None;
    }
    let (owner, name) = rest.split_at(8);
    let owner = u64::from_be_bytes(owner.try_into().ok()?);
    match kind {
        V4_INODE if name.is_empty() => Some(key::inode_key(owner)),
        V4_DIRENT => Some(key::dirent_key(owner, name)),
        _ => None,
    }
}

async fn connect(p: &ConvertFsParams) -> Result<ClusterClient> {
    let client = match p.credential_file.get() {
        Some(path) => {
            let (principal, secret) = autumn_client::read_credential_file(&path).context("--credential-file")?;
            ClusterClient::connect_with_credential(&p.manager.get(), "fs", principal, secret)
                .await
                .context("connect to manager (with credential)")?
        }
        None => ClusterClient::connect(&p.manager.get(), "fs").await.context("connect to manager")?,
    };
    client
        .wait_for_cluster_ready(std::time::Duration::from_secs(20), std::time::Duration::from_millis(200))
        .await
        .context("wait for cluster ready")?;
    Ok(client)
}

async fn put_stamp(client: &ClusterClient, version: u64) -> Result<()> {
    client
        .put(&key::schema_version_key(), version.to_be_bytes().to_vec(), WriteOptions::default())
        .await
        .map_err(|e| anyhow!("write schema stamp {version:#x}: {e}"))
}

/// Keys under the one-byte `prefix`, counted without changing anything.
async fn count(client: &ClusterClient, prefix: u8) -> Result<u64> {
    let (mut n, mut from) = (0u64, vec![prefix]);
    loop {
        let page = client.range(&[prefix], &from, PAGE).await.map_err(|e| anyhow!("KV range: {e}"))?;
        n += page.entries.len() as u64;
        match page.entries.last() {
            Some(last) if page.has_more => from = [last.key.as_slice(), &[0]].concat(),
            _ => return Ok(n),
        }
    }
}

/// Move every key under the one-byte v4 `prefix` to its v5 key.
async fn convert(client: &ClusterClient, prefix: u8) -> Result<u64> {
    let (mut moved, mut from) = (0u64, vec![prefix]);
    loop {
        // Resume after the last page rather than at the prefix: the moved
        // keys are deleted, and a scan from the prefix would walk all their
        // tombstones again on every page.
        let page = client.range(&[prefix], &from, PAGE).await.map_err(|e| anyhow!("KV range: {e}"))?;
        let has_more = page.has_more;
        let old: Vec<Vec<u8>> = page.entries.into_iter().map(|e| e.key).collect();
        let Some(last) = old.last() else { return Ok(moved) };
        from = [last.as_slice(), &[0]].concat();
        let refs: Vec<&[u8]> = old.iter().map(Vec::as_slice).collect();
        let values = client.get_many(&refs).await;
        let mut new = Vec::with_capacity(old.len());
        for (k, v) in old.iter().zip(values) {
            let v = v.map_err(|e| anyhow!("KV get {}: {e}", hex(k)))?;
            // Listed but gone: nothing to move; the delete below is a no-op.
            let Some(v) = v else { continue };
            let nk = v5_key(k).ok_or_else(|| anyhow!("key {} is not a v4 inode or directory entry", hex(k)))?;
            new.push((nk, compio::bytes::Bytes::from(v)));
        }
        let items: Vec<(&[u8], compio::bytes::Bytes)> = new.iter().map(|(k, v)| (k.as_slice(), v.clone())).collect();
        for (r, (k, _)) in client.put_many(&items, WriteOptions::default()).await.into_iter().zip(&new) {
            r.map_err(|e| anyhow!("KV put {}: {e}", hex(k)))?;
        }
        for (r, k) in client.delete_many(&refs, WriteOptions::default()).await.into_iter().zip(&old) {
            r.map_err(|e| anyhow!("KV delete {}: {e}", hex(k)))?;
        }
        moved += new.len() as u64;
        if !has_more {
            return Ok(moved);
        }
    }
}

fn hex(k: &[u8]) -> String {
    k.iter().map(|b| format!("{b:02x}")).collect()
}

async fn run(p: &ConvertFsParams) -> Result<()> {
    let client = connect(p).await?;
    let stamp = match client.get(&key::schema_version_key()).await.map_err(|e| anyhow!("read schema stamp: {e}"))? {
        None => None,
        Some(v) => Some(u64::from_be_bytes(
            v.as_slice().try_into().map_err(|_| anyhow!("schema stamp holds {} bytes, want 8", v.len()))?,
        )),
    };
    match stamp {
        Some(schema::SCHEMA_VERSION) => {
            println!("fs/ is already schema v{}", schema::SCHEMA_VERSION);
            return Ok(());
        }
        Some(4) | Some(schema::SCHEMA_CONVERTING_TO_V5) => {}
        None if count(&client, V4_INODE).await? == 0 => {
            println!("fs/ holds no tree: nothing to convert");
            return Ok(());
        }
        None => bail!("fs/ holds inodes but no schema stamp (written before v4): not convertible by this tool"),
        Some(v) => bail!("fs/ is schema stamp {v:#x}; this tool converts v4"),
    }
    if p.dry_run.get() {
        let (i, d) = (count(&client, V4_INODE).await?, count(&client, V4_DIRENT).await?);
        println!("would move {i} inodes and {d} directory entries to schema v{}", schema::SCHEMA_VERSION);
        return Ok(());
    }
    put_stamp(&client, schema::SCHEMA_CONVERTING_TO_V5).await?;
    let inodes = convert(&client, V4_INODE).await?;
    let dirents = convert(&client, V4_DIRENT).await?;
    put_stamp(&client, schema::SCHEMA_VERSION).await?;
    println!(
        "moved {inodes} inodes and {dirents} directory entries; fs/ is schema v{}",
        schema::SCHEMA_VERSION
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v4_keys_map_to_their_v5_keys() {
        let ino = [[V4_INODE].as_slice(), &42u64.to_be_bytes()].concat();
        assert_eq!(v5_key(&ino), Some(key::inode_key(42)));
        let dirent = [[V4_DIRENT].as_slice(), &7u64.to_be_bytes(), b"name"].concat();
        assert_eq!(v5_key(&dirent), Some(key::dirent_key(7, b"name")));
        let root_entry = [[V4_DIRENT].as_slice(), &1u64.to_be_bytes()].concat();
        assert_eq!(v5_key(&root_entry), Some(key::dirent_key(1, b"")), "an empty name is still an entry key");
        assert_eq!(v5_key(&[[V4_INODE].as_slice(), &42u64.to_be_bytes(), b"x"].concat()), None);
        assert_eq!(v5_key(&[V4_INODE, 1, 2]), None);
        assert_eq!(v5_key(&[0x03, 0, 0, 0, 0, 0, 0, 0, 1]), None);
    }
}
