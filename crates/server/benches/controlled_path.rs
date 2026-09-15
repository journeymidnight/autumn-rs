//! Fixed-work benchmark with an external measurement barrier.
//! controlled_path MANAGER tcp|ucx SIZE OPS DEPTH PARTITIONS load|read|direct|get|write
//! `read` goes through the partition server; `direct` reads large values from an extent node;
//! `get` uses the plain `ClusterClient::get` at every size.
use autumn_client::{alloc_value_buf, bulk_worthwhile, fan_out, ClusterClient};
use futures::StreamExt;
use std::{
    io::{self, BufRead, Write},
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

const KEYS: usize = 64;

fn keys(part: usize, partitions: usize, size: usize) -> Vec<Vec<u8>> {
    let span = u32::MAX as u64 / partitions as u64;
    let prefix = span * part as u64 + span / 2;
    (0..KEYS)
        .map(|i| format!("{prefix:08x}/{size:09}/{i:04}").into_bytes())
        .collect()
}

async fn batch(
    client: &ClusterClient,
    keys: &[Vec<u8>],
    value: &bytes::Bytes,
    count: usize,
    depth: usize,
    write: bool,
    read_mode: &str,
    verify: bool,
) -> Vec<u64> {
    let requests = (0..count).map(|i| async move {
        let key = &keys[i % keys.len()];
        let start = Instant::now();
        if write {
            if bulk_worthwhile(value.len()) {
                client.put_bulk(key, value.clone()).await.unwrap();
            } else {
                client.put(key, value).await.unwrap();
            }
        } else if read_mode == "get" {
            let got = client.get(key).await.unwrap().expect("missing key");
            assert_eq!(got.len(), value.len());
            if verify {
                assert_eq!(&got[..], value.as_ref());
            }
        } else if read_mode == "direct" && bulk_worthwhile(value.len()) {
            let got = client.get_direct(key).await.unwrap().expect("missing key");
            assert_eq!(got.len(), value.len());
            if verify {
                assert_eq!(&got[..], value.as_ref());
            }
        } else if bulk_worthwhile(value.len()) {
            let got = client.get_pooled(key).await.unwrap().expect("missing key");
            assert_eq!(got.len(), value.len());
            if verify {
                assert_eq!(&got[..], value.as_ref());
            }
        } else {
            let got = client.get(key).await.unwrap().expect("missing key");
            assert_eq!(got.len(), value.len());
            if verify {
                assert_eq!(&got[..], value.as_ref());
            }
        }
        start.elapsed().as_nanos() as u64
    });
    let requests = fan_out(requests, depth);
    futures::pin_mut!(requests);
    let mut latencies = Vec::with_capacity(count);
    while let Some((_, ns)) = requests.next().await {
        latencies.push(ns);
    }
    latencies
}

fn line() {
    let mut text = String::new();
    io::stdin().lock().read_line(&mut text).unwrap();
    assert_eq!(text.trim(), "go", "measurement controller disconnected");
}

fn marker(value: u64) {
    #[cfg(target_os = "linux")]
    unsafe {
        // A no-op prctl query; the tracing collector recognizes its extra
        // arguments. No process flags or global kernel settings are changed.
        let window: u64 = std::env::var("AUTUMN_PERF_WINDOW")
            .unwrap_or_default()
            .parse()
            .unwrap_or(0);
        libc::prctl(libc::PR_GET_DUMPABLE, 0x41555455u64, value, window, 0u64);
    }
}

fn main() {
    marker(2);
    let args: Vec<_> = std::env::args()
        .skip(1)
        .filter(|x| x != "--bench")
        .collect();
    assert_eq!(args.len(), 7);
    let size: usize = args[2].parse().unwrap();
    let count: usize = args[3].parse().unwrap();
    let depth: usize = args[4].parse().unwrap();
    let partitions: usize = args[5].parse().unwrap();
    let mode = args[6].clone();
    assert!(size > 0 && count > 0 && depth > 0 && matches!(partitions, 1 | 4));
    assert!(matches!(
        mode.as_str(),
        "load" | "read" | "direct" | "get" | "write"
    ));
    autumn_transport::init_with(match args[1].as_str() {
        "tcp" => autumn_transport::TransportKind::Tcp,
        "ucx" => autumn_transport::TransportKind::Ucx,
        _ => panic!("unknown transport"),
    });
    let ready = Arc::new(Barrier::new(partitions + 1));
    let start = Arc::new(Barrier::new(partitions + 1));
    let done = Arc::new(Barrier::new(partitions + 1));
    let finish = Arc::new(Barrier::new(partitions + 1));
    let (tx, rx) = std::sync::mpsc::channel();
    let mut threads = Vec::new();
    for part in 0..partitions {
        let (ready, start, done, finish, tx) = (
            ready.clone(),
            start.clone(),
            done.clone(),
            finish.clone(),
            tx.clone(),
        );
        let manager = args[0].clone();
        let mode = mode.clone();
        threads.push(std::thread::spawn(move || {
            autumn_common::cpu_pin::pin_current(Some(40 + part)).unwrap();
            compio::runtime::Runtime::new().unwrap().block_on(async {
                let client = ClusterClient::connect(&manager, "bench/controlled")
                    .await
                    .unwrap();
                client.set_rpc_timeout(Duration::from_secs(30));
                let keys = keys(part, partitions, size);
                let mut buf = alloc_value_buf(size);
                for (i, byte) in buf.as_mut_slice().iter_mut().enumerate() {
                    *byte = (i % 251) as u8;
                }
                let value = buf.freeze();
                if mode == "load" {
                    batch(&client, &keys, &value, KEYS, depth, true, "read", false).await;
                    batch(&client, &keys, &value, KEYS, depth, false, "read", true).await;
                } else {
                    // Fixed warmup work and byte verification outside timing.
                    batch(
                        &client,
                        &keys,
                        &value,
                        KEYS,
                        depth,
                        mode == "write",
                        &mode,
                        true,
                    )
                    .await;
                }
                ready.wait();
                start.wait();
                let latencies = if mode == "load" {
                    vec![]
                } else {
                    batch(
                        &client,
                        &keys,
                        &value,
                        count,
                        depth,
                        mode == "write",
                        &mode,
                        false,
                    )
                    .await
                };
                tx.send((part, latencies)).unwrap();
                done.wait();
                finish.wait();
            });
        }));
    }
    ready.wait();
    println!("READY {}", std::process::id());
    io::stdout().flush().unwrap();
    line();
    marker(1);
    let t = Instant::now();
    start.wait();
    done.wait();
    let seconds = t.elapsed().as_secs_f64();
    marker(0);
    println!("DONE");
    io::stdout().flush().unwrap();
    line();
    finish.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    drop(tx);
    let mut latency = Vec::new();
    let mut per_partition = Vec::new();
    for (part, samples) in rx {
        per_partition.push((part, samples.len()));
        latency.extend(samples);
    }
    latency.sort_unstable();
    let ops = latency.len();
    println!(
        "{}",
        serde_json::json!({
            "mode":mode,"size":size,"operations_per_partition":count,"depth":depth,
            "partitions":partitions,"per_partition_ops":per_partition,"ops":ops,"bytes":ops*size,
            "seconds":seconds,"mib_per_sec":ops as f64*size as f64/seconds/1048576.0,
            "p50_us":latency.get(ops/2).copied().unwrap_or(0) as f64/1000.0,
            "p99_us":latency.get(ops.saturating_sub(1)*99/100).copied().unwrap_or(0) as f64/1000.0
        })
    );
}
