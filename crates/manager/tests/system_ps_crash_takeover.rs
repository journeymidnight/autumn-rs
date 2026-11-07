//! Crash takeover with an UN-FLUSHED active memtable.
//!
//! The capability under test is the harness itself: taking a partition over
//! from a PS that died without draining, so the successor must rebuild the
//! un-flushed tail from the log. Several crash-shaped invariants can only be
//! exercised from that state — anything that lives solely in the active
//! memtable at the moment of death (a just-ACKed write, a GC value-relocation
//! that has been appended but not yet flushed into an SST) is made durable by a
//! graceful drain, so a clean shutdown quietly proves nothing.
//!
//! Neither in-process helper can produce it:
//!   - `start_partition_server` never exits; dropping the client leaves the
//!     server heartbeating, so a takeover PS contests the per-partition
//!     `owner_epoch` and its open/serve wedges (the listener never binds).
//!   - `start_partition_server_stoppable` exits, but `shutdown()` rotates the
//!     active memtable and flushes every imm first — it MAKES DURABLE exactly
//!     the state a crash test needs to lose.
//!
//! So the PS runs as a child process and is SIGKILLed
//! (`start_partition_server_killable`), which leaves the durable state where a
//! power cut would and exercises the real `autumn-ps` startup path.
//!
//! The assertion is the one that makes the state meaningful: a write ACKed but
//! never flushed must come back, because logStream is the WAL and the successor
//! replays it from the last checkpoint's vp_head.

mod support;

use std::time::Duration;

use autumn_rpc::client::RpcClient;

use support::*;

#[test]
fn a_write_acked_but_never_flushed_survives_sigkill_and_takeover() {
    let mgr_addr = pick_addr();
    start_manager(mgr_addr);

    let n1_dir = tempfile::tempdir().expect("n1 tmpdir");
    let n2_dir = tempfile::tempdir().expect("n2 tmpdir");
    let n1_addr = pick_addr();
    let n2_addr = pick_addr();
    start_extent_node(n1_addr, n1_dir.path().to_path_buf(), 1);
    start_extent_node(n2_addr, n2_dir.path().to_path_buf(), 2);

    let ps1_addr = pick_addr();
    let ps2_addr = pick_addr();

    let mut ps1 = compio::runtime::Runtime::new().unwrap().block_on(async {
        let mgr = RpcClient::connect(mgr_addr).await.expect("connect mgr");
        register_two_nodes(&mgr, n1_addr, n2_addr, 88).await;

        let (log, row, meta) = create_three_streams(&mgr).await;
        upsert_partition(&mgr, 901, log, row, meta, b"a", b"z").await;
        drop(mgr);

        let ps1 = start_partition_server_killable(71, mgr_addr, ps1_addr);
        let c1 = RpcClient::connect(ps1_addr).await.expect("connect ps1");

        // FLUSHED: lands in an SST, so it survives on the checkpoint path.
        ps_put(&c1, 901, b"kflushed", b"v-flushed").await;
        ps_flush(&c1, 901).await;

        // UN-FLUSHED: ACKed (so its WAL record is durable on every replica) but
        // deliberately never flushed — it exists only in the active memtable.
        // This is the state the harness exists to create.
        ps_put(&c1, 901, b"kunflushed", b"v-unflushed").await;
        assert_eq!(
            ps_get(&c1, 901, b"kunflushed").await.value,
            b"v-unflushed",
            "pre-crash: the un-flushed write must be readable from the memtable"
        );

        ps1
    });

    // Crash. No drain, no flush — the active memtable is simply gone.
    ps1.kill();

    compio::runtime::Runtime::new().unwrap().block_on(async {
        // The successor rebuilds the un-flushed tail by replaying logStream
        // from the last checkpoint's vp_head.
        let _ps2 = start_partition_server_killable(72, mgr_addr, ps2_addr);
        let c2 = RpcClient::connect(ps2_addr).await.expect("connect ps2");

        // The takeover PS binds only after recovery, but the region may still
        // be settling onto it.
        let mut got = Vec::new();
        for _ in 0..100 {
            let r = ps_get(&c2, 901, b"kflushed").await;
            if r.value == b"v-flushed" {
                got = r.value.clone();
                break;
            }
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_eq!(
            got, b"v-flushed",
            "the flushed write must survive takeover (checkpoint path)"
        );

        assert_eq!(
            ps_get(&c2, 901, b"kunflushed").await.value,
            b"v-unflushed",
            "the ACKed-but-never-flushed write must be replayed from logStream: \
             it was ACKed, so losing it on a crash is acknowledged-write loss"
        );
    });
}
