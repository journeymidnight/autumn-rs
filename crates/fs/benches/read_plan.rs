//! Isolate cached filesystem read planning from network and disk throughput.
//! cargo bench -p autumn-fs --bench read_plan -- <manager> [iterations]
use std::collections::VecDeque;
use std::hint::black_box;
use std::time::Instant;

use autumn_fs::{
    meta, read,
    schema::{InodeState, StripeLayout, MAX_EXTENT},
    state::FsState,
};

fn main() {
    let mut args = std::env::args().skip(1).filter(|a| a != "--bench");
    let manager = args
        .next()
        .expect("usage: read_plan <manager> [iterations]");
    let iterations: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(2000);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut state = FsState::new(&manager).await.unwrap();
        for gib in [1u64, 64, 1024] {
            let size = gib << 30;
            for striped in [false, true] {
                let mut inode = meta::new_file_meta(0o644, 0, 0);
                inode.size = size;
                if striped {
                    inode.stripe = Some(StripeLayout {
                        lanes: 24,
                        unit_bytes: MAX_EXTENT as u32,
                    });
                }
                state.inodes.insert(
                    42,
                    InodeState {
                        meta: inode,
                        write_buf: None,
                        pending_flushes: VecDeque::new(),
                        flush_error: None,
                        dirty: false,
                        open_count: 1,
                        cached_version: 0,
                        extents: Some(
                            (0..size / MAX_EXTENT as u64)
                                .map(|i| (i * MAX_EXTENT as u64, MAX_EXTENT as u32))
                                .collect(),
                        ),
                    },
                );
                let t = Instant::now();
                for i in 0..iterations {
                    let offset = (i * 1_000_003) % (size - (1 << 20));
                    let plan = read::prepare(&mut state, 42, offset as i64, 1 << 20)
                        .await
                        .unwrap();
                    assert_eq!(plan.actual_size, 1 << 20);
                    assert!(!plan.chunks.is_empty() && plan.chunks.len() <= 2);
                    black_box(plan);
                }
                println!(
                    "gib={gib} striped={striped} iterations={iterations} ns_per_read={:.1}",
                    t.elapsed().as_nanos() as f64 / iterations as f64
                );
            }
        }
    });
}
