#![allow(
    dead_code,
    unused_imports,
    unused_must_use,
    clippy::while_let_loop,
    clippy::needless_range_loop
)] // bench/throwaway perf code
//! Benchmark: ExtentNode 4KB append via autumn-rpc.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tempfile::TempDir;

fn main() {
    let payload = Bytes::from(vec![0xABu8; 4096]);
    let ops = 50_000u64;

    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(data_dir.join("disk_id"), "1").unwrap();

        let config = autumn_stream::ExtentNodeConfig::new(data_dir, 1);

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = std::net::TcpListener::bind(addr).unwrap();
        let bound = listener.local_addr().unwrap();
        drop(listener);

        // `ExtentNode` is `!Send` (Rc fields), so build it INSIDE the server
        // thread's runtime rather than moving it across the OS-thread boundary;
        // only the (Send) config + addr cross over.
        std::thread::spawn(move || {
            compio::runtime::Runtime::new().unwrap().block_on(async {
                let node = autumn_stream::ExtentNode::new(config).await.unwrap();
                node.serve(bound).await.unwrap();
            });
        });

        for _ in 0..100 {
            if std::net::TcpStream::connect(bound).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let client = autumn_rpc::client::RpcClient::connect(bound).await.unwrap();

        // Alloc extent
        use autumn_stream::extent_rpc::*;
        let extent_id = 1u64;
        {
            let payload = rkyv_encode(&AllocExtentReq { extent_id });
            client.call(MSG_ALLOC_EXTENT, payload).await.unwrap();
        }

        // Warmup
        for i in 0..500u32 {
            let req = AppendReq {
                extent_id,
                eversion: 1,
                commit: i as u64 * payload.len() as u64,
                owner_epoch: 1,
                payload: payload.clone(),
            };
            client.call(MSG_APPEND, req.encode()).await.unwrap();
        }
        let warmup_commit = 500u64 * payload.len() as u64;

        // Sequential
        let start = Instant::now();
        for i in 0..ops {
            let commit = warmup_commit + (i as u64) * payload.len() as u64;
            let req = AppendReq {
                extent_id,
                eversion: 1,
                commit,
                owner_epoch: 1,
                payload: payload.clone(),
            };
            client.call(MSG_APPEND, req.encode()).await.unwrap();
        }
        let elapsed = start.elapsed();
        let ops_sec = ops as f64 / elapsed.as_secs_f64();
        let mb_sec =
            (ops as f64 * payload.len() as f64) / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        let lat_us = elapsed.as_micros() as f64 / ops as f64;
        println!("=== autumn-rpc ExtentNode append (4KB, sequential) ===");
        println!("  ops:     {ops}");
        println!("  time:    {elapsed:.2?}");
        println!("  ops/s:   {ops_sec:.0}");
        println!("  MB/s:    {mb_sec:.1}");
        println!("  lat:     {lat_us:.1} us/op");

        // Pipelined depth sweep — this is the dimension the write-path rewrite
        // touches (coalescer batching + concurrent pwrite). Fresh extent per
        // depth, in-order commits (RpcClient's single writer_task serialises
        // sends, so a sliding-window FuturesUnordered still delivers appends in
        // commit order == extent.len; errors are counted to catch any reorder).
        use futures::stream::{FuturesUnordered, StreamExt};
        let pipe_ops = 50_000u64;
        println!("=== pipelined depth sweep (4KB, fresh extent per depth) ===");
        for depth in [1usize, 4, 8, 16, 32, 64] {
            let eid = 100u64 + depth as u64;
            {
                let payload = rkyv_encode(&AllocExtentReq { extent_id: eid });
                client.call(MSG_ALLOC_EXTENT, payload).await.unwrap();
            }
            let mut errs = 0u64;
            let start = Instant::now();
            let mut inflight = FuturesUnordered::new();
            let mut issued = 0u64;
            let mk = |i: u64| {
                AppendReq {
                    extent_id: eid,
                    eversion: 1,
                    commit: i * payload.len() as u64,
                    owner_epoch: 1,
                    payload: payload.clone(),
                }
                .encode()
            };
            while issued < depth as u64 && issued < pipe_ops {
                inflight.push(client.call(MSG_APPEND, mk(issued)));
                issued += 1;
            }
            while let Some(r) = inflight.next().await {
                if r.is_err() {
                    errs += 1;
                }
                if issued < pipe_ops {
                    inflight.push(client.call(MSG_APPEND, mk(issued)));
                    issued += 1;
                }
            }
            let el = start.elapsed();
            let ops_sec = pipe_ops as f64 / el.as_secs_f64();
            let mb_sec =
                (pipe_ops as f64 * payload.len() as f64) / (1024.0 * 1024.0) / el.as_secs_f64();
            let lat = el.as_micros() as f64 / pipe_ops as f64;
            println!(
                "  depth={depth:<3} {ops_sec:>8.0} ops/s  {mb_sec:>7.1} MB/s  {lat:>7.1} us/op  errs={errs}"
            );
        }
    });
}
