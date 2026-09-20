//! Identical object workloads over native Autumn and an S3-compatible endpoint.
use anyhow::Result;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::{sync::Arc, time::Instant};

fn report(label: &str, phase: &str, bytes: usize, samples: &mut [f64], wall: f64) {
    samples.sort_by(f64::total_cmp);
    println!(
        "{}",
        serde_json::json!({"backend":label,"phase":phase,"operations":samples.len(),
        "p50_ms":samples[samples.len()/2]*1000.0,
        "p99_ms":samples[((samples.len() as f64*0.99).ceil() as usize-1).min(samples.len()-1)]*1000.0,
        "mib_s":bytes as f64/(1024.0*1024.0)/wall})
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let backend = std::env::args().nth(1).expect("object_bench autumn|s3");
    let store: Arc<dyn ObjectStore> = match backend.as_str() {
        "autumn" => Arc::new(
            autumn_object_store::AutumnObjectStore::connect(
                std::env::var("AUTUMN_MANAGER")?,
                std::env::var("AUTUMN_OBJECT_SCOPE")?,
            )
            .await?,
        ),
        "s3" => Arc::new(
            object_store::aws::AmazonS3Builder::from_env()
                .with_allow_http(true)
                .build()?,
        ),
        _ => anyhow::bail!("unknown backend"),
    };
    let prefix = format!(
        "bench-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    for size in [64 * 1024usize, 1024 * 1024, 4 * 1024 * 1024] {
        let data = Bytes::from((0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>());
        let paths: Vec<_> = (0..64)
            .map(|i| Path::from(format!("{prefix}/data/{size}/{i:04}")))
            .collect();
        let begin = Instant::now();
        let mut samples: Vec<f64> = futures::stream::iter(paths.clone())
            .map(|path| {
                let store = store.clone();
                let data = data.clone();
                async move {
                    let start = Instant::now();
                    store.put(&path, data.into()).await?;
                    Ok::<_, object_store::Error>(start.elapsed().as_secs_f64())
                }
            })
            .buffer_unordered(8)
            .try_collect()
            .await?;
        report(
            &backend,
            &format!("put_{size}_c8"),
            size * 64,
            &mut samples,
            begin.elapsed().as_secs_f64(),
        );
        let begin = Instant::now();
        let mut samples: Vec<f64> = futures::stream::iter(paths.clone())
            .map(|path| {
                let store = store.clone();
                let data = data.clone();
                async move {
                    let start = Instant::now();
                    let value = store.get_range(&path, 0..size as u64).await?;
                    let elapsed = start.elapsed().as_secs_f64();
                    assert_eq!(value, data);
                    Ok::<_, object_store::Error>(elapsed)
                }
            })
            .buffer_unordered(8)
            .try_collect()
            .await?;
        report(
            &backend,
            &format!("get_range_{size}_c8"),
            size * 64,
            &mut samples,
            begin.elapsed().as_secs_f64(),
        );
    }
    let fragments = Path::from(format!("{prefix}/fragments"));
    futures::stream::iter(0..1100)
        .map(|i| {
            let store = store.clone();
            let path = Path::from(format!("{fragments}/{i:04}.lance"));
            async move { store.put(&path, "metadata".into()).await }
        })
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await?;
    let begin = Instant::now();
    let mut samples = Vec::new();
    for _ in 0..100 {
        let start = Instant::now();
        let found: Vec<_> = store.list(Some(&fragments)).try_collect().await?;
        assert_eq!(found.len(), 1100);
        samples.push(start.elapsed().as_secs_f64());
    }
    report(
        &backend,
        "list_1100",
        0,
        &mut samples,
        begin.elapsed().as_secs_f64(),
    );
    let paths = store
        .list(Some(&Path::from(prefix)))
        .map_ok(|m| m.location)
        .boxed();
    store.delete_stream(paths).try_collect::<Vec<_>>().await?;
    Ok(())
}
