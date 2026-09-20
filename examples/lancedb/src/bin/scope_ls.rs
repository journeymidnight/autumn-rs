//! Lists what an Autumn scope actually holds, through the Rust client.
//!
//! The point is to check a Python writer from outside its own stack: run it
//! before and after a Python demo and the object count moves, with Lance's
//! real key layout underneath. A provider that quietly wrote nowhere would
//! leave the scope empty while Python still reported rows, because Lance
//! caches what it just wrote.
use anyhow::Result;
use autumn_object_store::AutumnObjectStore;
use futures::TryStreamExt;
use object_store::ObjectStore;

#[tokio::main]
async fn main() -> Result<()> {
    let manager = std::env::var("AUTUMN_MANAGER")?;
    let scope = std::env::var("AUTUMN_OBJECT_SCOPE")?;
    let store = AutumnObjectStore::connect(&manager, &scope).await?;
    let objects: Vec<_> = store.list(None).try_collect().await?;
    let bytes: u64 = objects.iter().map(|m| m.size).sum();
    println!("objects={} bytes={}", objects.len(), bytes);
    for meta in objects.iter().take(10) {
        println!("  {}", meta.location);
    }
    Ok(())
}
