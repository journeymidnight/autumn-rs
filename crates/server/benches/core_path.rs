//! Fixed-key benchmark: core_path <manager> <tcp|ucx> <size> <seconds> <depth> [load|read|write|direct]
//! Load once, then measure versions against the same keys. Errors fail the run.
use autumn_client::{alloc_value_buf, bulk_worthwhile, fan_out, ClusterClient};
use futures::StreamExt;
use std::cell::Cell;
use std::time::{Duration, Instant};

async fn run(
    client: &ClusterClient,
    keys: &[Vec<u8>],
    value: &bytes::Bytes,
    seconds: u64,
    depth: usize,
    mode: &str,
    sample: bool,
) -> (u64, Vec<u64>) {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let seq = Cell::new(0usize);
    let futs = std::iter::from_fn(|| {
        if Instant::now() >= deadline {
            return None;
        }
        let i = seq.get();
        seq.set(i + 1);
        let key = &keys[i % keys.len()];
        Some(async move {
            let start = Instant::now();
            match mode {
                "write" if bulk_worthwhile(value.len()) => {
                    client.put_bulk(key, value.clone()).await.unwrap()
                }
                "write" => client.put(key, value).await.unwrap(),
                "direct" => {
                    assert_eq!(
                        client.get_direct(key).await.unwrap().unwrap().len(),
                        value.len()
                    );
                }
                _ if bulk_worthwhile(value.len()) => {
                    assert_eq!(
                        client.get_pooled(key).await.unwrap().unwrap().len(),
                        value.len()
                    );
                }
                _ => {
                    assert_eq!(client.get(key).await.unwrap().unwrap().len(), value.len());
                }
            }
            (i, start.elapsed().as_nanos() as u64)
        })
    });
    let results = fan_out(futs, depth);
    futures::pin_mut!(results);
    let mut count = 0;
    let mut latencies = Vec::new();
    while let Some((_, (i, ns))) = results.next().await {
        count += 1;
        if sample && i % 16 == 0 {
            latencies.push(ns);
        }
    }
    (count, latencies)
}

fn main() {
    let args: Vec<_> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    assert!(
        args.len() >= 5,
        "core_path <manager> <tcp|ucx> <size> <seconds> <depth> [load|read|write|direct]"
    );
    let size: usize = args[2].parse().unwrap();
    let seconds: u64 = args[3].parse().unwrap();
    let depth: usize = args[4].parse().unwrap();
    let mode = args.get(5).map(String::as_str).unwrap_or("read");
    assert!(
        size > 0 && seconds > 0 && depth > 0,
        "size, seconds and depth must be positive"
    );
    assert!(
        matches!(mode, "load" | "read" | "write" | "direct"),
        "unknown mode"
    );
    autumn_transport::init_with(match args[1].as_str() {
        "tcp" => autumn_transport::TransportKind::Tcp,
        "ucx" => autumn_transport::TransportKind::Ucx,
        _ => panic!("unknown transport"),
    });
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let client = ClusterClient::connect(&args[0], "bench/core-path")
            .await
            .unwrap();
        let keys: Vec<_> = (0..256)
            .map(|i| format!("{size:09}/{i:04}").into_bytes())
            .collect();
        let mut buf = alloc_value_buf(size);
        for (i, b) in buf.as_mut_slice().iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let value = buf.freeze();
        if mode == "load" {
            let puts = keys.iter().map(|key| client.put_bulk(key, value.clone()));
            let results = fan_out(puts, depth);
            futures::pin_mut!(results);
            while let Some((_, r)) = results.next().await {
                r.unwrap();
            }
            println!("loaded={} size={size}", keys.len());
            return;
        }
        run(&client, &keys, &value, 2, depth, mode, false).await;
        let t = Instant::now();
        let (ops, mut ns) = run(&client, &keys, &value, seconds, depth, mode, true).await;
        let wall = t.elapsed().as_secs_f64();
        ns.sort_unstable();
        let pool = autumn_transport::regpool_snapshot();
        println!(
            "{}",
            serde_json::json!({
                "transport":args[1], "mode":mode, "size":size, "depth":depth, "ops":ops,
                "seconds":wall, "ops_per_sec":ops as f64 / wall,
                "mib_per_sec":ops as f64 * size as f64 / wall / 1048576.0,
                "p50_us":ns[ns.len()/2] as f64 / 1000.0,
                "p99_us":ns[(ns.len()-1)*99/100] as f64 / 1000.0,
                "pool_acquires":pool.acquire_total, "pool_hits":pool.hit_total
            })
        );
    });
}
