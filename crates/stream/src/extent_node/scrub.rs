//! Scrub: check sealed payload files against the checksums recorded for them,
//! on the node that holds them, when asked.
//!
//! The scrub shares nothing with the hot path. Appends, reads, seals, EC
//! conversion and repairs never compute, write or consult a checksum; the
//! scrub is the only reader and the only writer of the `.ck` sidecars. What it
//! costs is what it reads, paced on this node (`ScrubPacer`), and nothing but
//! the request and the outcomes crosses the network.
//!
//! The MANAGER asks (`MSG_SCRUB_EXTENTS`, dispatched by `autumn-op scrub` and
//! the weekly policy) and names, per task, the payload file and its length — it
//! is what knows the extent is sealed and which file is its payload here. For
//! each file:
//! - checksums recorded for exactly this length: every block is compared; a
//!   block that differs, or that can no longer be read in full, is ROT, and the
//!   manager isolates the slot on that outcome and rebuilds it;
//! - none: the content is hashed and recorded (trust-on-first-use — bytes
//!   already damaged before their first scrub are recorded as they are).
//!
//! A task is not looked at while a recovery or EC conversion is in flight on
//! the extent, and its result is dropped if the content was replaced while it
//! ran (`ExtentEntry::content_gen`): what it read may be the bytes replaced.

use super::*;
use crate::extent_rpc::{
    ScrubDone, ScrubExtentsReq, ScrubTask, SCRUB_OUTCOME_CLEAN, SCRUB_OUTCOME_DESCRIBED,
    SCRUB_OUTCOME_FAILED, SCRUB_OUTCOME_ROT, SCRUB_OUTCOME_SKIPPED,
};

/// One shard's waiting scrub tasks, in arrival order.
///
/// A file asked for again while it is still waiting is not queued twice: the
/// op asking joins the waiting entry and gets the same outcome — dropping it
/// would leave that op waiting for an outcome nobody sends. (A file asked for
/// while it is already being read is queued again and read again.) Indexed by
/// file, because a whole-cluster scrub queues one task per extent a shard
/// holds and a scan per task would be quadratic on the shard's own thread.
#[derive(Debug, Default)]
pub(super) struct ScrubQueue {
    order: std::collections::VecDeque<ScrubTask>,
    /// file → (the waiting task's own op, the other ops that joined it).
    waiting: HashMap<(u64, u8, u32), (u64, Vec<u64>)>,
}

fn file_key(t: &ScrubTask) -> (u64, u8, u32) {
    (t.extent_id, t.payload_location, t.shard_index)
}

impl ExtentNode {
    /// `MSG_SCRUB_EXTENTS`: queue the tasks this shard owns, hand the rest to
    /// their sibling shards, and answer at once — the scrub runs in the
    /// background and reports on `df`.
    pub(super) async fn handle_scrub_extents(&self, payload: Bytes) -> HandlerResult {
        let req: ScrubExtentsReq =
            rkyv_decode(&payload).map_err(|e| (StatusCode::InvalidArgument, e))?;
        let mut mine = Vec::new();
        let mut elsewhere: std::collections::BTreeMap<String, Vec<ScrubTask>> =
            std::collections::BTreeMap::new();
        for task in req.tasks {
            match (self.owns_extent(task.extent_id), self.sibling_for_extent(task.extent_id)) {
                (false, Some(sibling)) => elsewhere.entry(sibling.to_string()).or_default().push(task),
                _ => mine.push(task),
            }
        }
        for (sibling, tasks) in elsewhere {
            let payload = rkyv_encode(&ScrubExtentsReq { tasks: tasks.clone() });
            if let Err((_, why)) = self
                .forward_rpc_to_sibling(&sibling, MSG_SCRUB_EXTENTS, payload)
                .await
            {
                // Say so per task, or the op waits for outcomes that are
                // never coming.
                for t in tasks {
                    self.done.push_scrub_done(scrub_done(
                        &t,
                        SCRUB_OUTCOME_FAILED,
                        format!("could not reach the shard that owns it: {why}"),
                    ));
                }
            }
        }
        let accepted = mine.len();
        self.enqueue_scrub(mine);
        code_resp(CODE_OK, format!("accepted {accepted} scrub task(s)"))
    }

    /// Add tasks to this shard's queue and make sure a worker is draining it.
    /// A file already waiting is not queued twice; the op asking again joins
    /// the waiting entry and gets the same outcome.
    pub(super) fn enqueue_scrub(&self, tasks: Vec<ScrubTask>) {
        {
            let mut queue = self.scrub_queue.borrow_mut();
            for task in tasks {
                match queue.waiting.get_mut(&file_key(&task)) {
                    Some((own, also_for)) => {
                        if *own != task.op_id && !also_for.contains(&task.op_id) {
                            also_for.push(task.op_id);
                            self.done.note_scrub_queued(task.op_id);
                        }
                    }
                    None => {
                        self.done.note_scrub_queued(task.op_id);
                        queue
                            .waiting
                            .insert(file_key(&task), (task.op_id, Vec::new()));
                        queue.order.push_back(task);
                    }
                }
            }
        }
        if self.scrub_worker_running.get() || self.scrub_queue.borrow().order.is_empty() {
            return;
        }
        self.scrub_worker_running.set(true);
        let node = self.clone();
        compio::runtime::spawn(async move {
            let mut pacer = extent_scrub::ScrubPacer::new(node.scrub_bytes_per_sec);
            loop {
                // Pop under a borrow that ends with this statement: the task
                // below awaits, and new requests push while it does.
                let next = {
                    let mut q = node.scrub_queue.borrow_mut();
                    q.order.pop_front().map(|task| {
                        let also_for = q
                            .waiting
                            .remove(&file_key(&task))
                            .map(|(_, v)| v)
                            .unwrap_or_default();
                        (task, also_for)
                    })
                };
                let Some((task, also_for)) = next else { break };
                let done = match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                    node.scrub_task(&task, &mut pacer),
                ))
                .await
                {
                    Ok(done) => done,
                    Err(_) => scrub_done(&task, SCRUB_OUTCOME_FAILED, "scrub task panicked".into()),
                };
                // Outcome first, count after: `df` samples the counts BEFORE it
                // drains the outcomes, so a task is always seen in one or the
                // other, never in neither, whichever shard thread runs when.
                for op_id in also_for {
                    node.done.push_scrub_done(ScrubDone {
                        op_id,
                        ..done.clone()
                    });
                    node.done.note_scrub_finished(op_id);
                }
                node.done.push_scrub_done(done);
                node.done.note_scrub_finished(task.op_id);
            }
            node.scrub_worker_running.set(false);
        })
        .detach();
    }

    /// Run one task to its outcome.
    pub(super) async fn scrub_task(
        &self,
        task: &ScrubTask,
        pacer: &mut extent_scrub::ScrubPacer,
    ) -> ScrubDone {
        let extent_id = task.extent_id;
        let skipped = |why: &str| scrub_done(task, SCRUB_OUTCOME_SKIPPED, why.to_string());
        if self.op_in_flight(extent_id) {
            return skipped("a recovery or EC conversion is changing it");
        }
        let Some(location) = PayloadLocation::from_wire_byte(task.payload_location) else {
            return scrub_done(
                task,
                SCRUB_OUTCOME_FAILED,
                format!("payload location {} names no file this build knows", task.payload_location),
            );
        };
        let payload = PayloadRef::for_extent(location, task.shard_index);
        let Some(entry) = self.extents.get(&extent_id).map(|e| Rc::clone(e.value())) else {
            return skipped("not held here");
        };
        if entry.corrupt_meta.load(Ordering::SeqCst) {
            return skipped("its .meta is quarantined");
        }
        if !entry.holds_payload(payload) {
            return skipped("this node holds no such file");
        }
        // Only content this node durably holds, at exactly the length the
        // manager names: a short `.dat` is a replica that missed the seal
        // and is being caught up, and checksums of it would describe a file
        // that never existed.
        let held = match location {
            PayloadLocation::InDat => entry
                .len
                .load(Ordering::SeqCst)
                .min(entry.coalescer.last_synced.load(Ordering::SeqCst)),
            PayloadLocation::InShardFile => entry.shard_file_len(task.shard_index).unwrap_or(0),
        };
        let fits = match location {
            PayloadLocation::InDat => held >= task.length,
            PayloadLocation::InShardFile => held == task.length,
        };
        if !fits {
            return skipped(&format!("holds {held} of the {} bytes named", task.length));
        }

        let generation = entry.content_gen.load(Ordering::SeqCst);
        let disk = match self.disk_for(entry.disk_id) {
            Ok(d) => d,
            Err(e) => return scrub_done(task, SCRUB_OUTCOME_FAILED, e),
        };
        let ck_path = match location {
            PayloadLocation::InDat => disk.ck_path(extent_id),
            PayloadLocation::InShardFile => disk.shard_ck_path(extent_id, task.shard_index),
        };
        let recorded = match compio::fs::read(&ck_path).await {
            Ok(raw) => extent_cksum::ExtentChecksums::decode(&raw, extent_id)
                .filter(|ck| ck.sealed_length == task.length),
            Err(_) => None,
        };
        let file = match self.payload_file(&entry, payload).await {
            Ok(f) => f,
            Err(e) => return scrub_done(task, SCRUB_OUTCOME_FAILED, e),
        };
        let block_bytes = recorded
            .as_ref()
            .map_or(extent_cksum::CK_BLOCK_BYTES, |ck| ck.block_bytes);
        let blocks = extent_cksum::block_count_for(task.length, block_bytes);
        let mut hashed = Vec::with_capacity(if recorded.is_some() { 0 } else { blocks });
        for i in 0..blocks {
            let (start, end) = extent_cksum::block_range(i, block_bytes, task.length);
            let wait = pacer.pace(std::time::Instant::now(), end - start);
            if !wait.is_zero() {
                compio::time::sleep(wait).await;
            }
            let read = file_pread(Rc::clone(&file), start, (end - start) as usize).await;
            if self.replaced_since(extent_id, &entry, generation) {
                return skipped("its content was replaced while it was being scrubbed");
            }
            let found = match read {
                Ok(buf) => crc32c::crc32c(&buf),
                // Short against a description is a finding: the file once held
                // this block and has lost it. No checksum mismatches when the
                // bytes are simply gone.
                Err(e) if recorded.is_some() && is_short_read(&e) => {
                    return self
                        .rot(task, describe_file(payload), i, "is no longer readable in full");
                }
                Err(e) => {
                    return scrub_done(
                        task,
                        SCRUB_OUTCOME_FAILED,
                        format!("read block {i}: {e}"),
                    )
                }
            };
            match &recorded {
                Some(ck) if ck.blocks[i] != found => {
                    return self
                        .rot(task, describe_file(payload), i, "differs from its checksum");
                }
                Some(_) => {}
                None => hashed.push(found),
            }
        }
        if recorded.is_some() {
            return scrub_done(task, SCRUB_OUTCOME_CLEAN, format!("{blocks} block(s) match"));
        }

        let ck = extent_cksum::ExtentChecksums {
            sealed_length: task.length,
            block_bytes,
            blocks: hashed,
        };
        if let Err(e) = self.persist_checksums(extent_id, &ck_path, &ck).await {
            return scrub_done(task, SCRUB_OUTCOME_FAILED, format!("write checksums: {e}"));
        }
        // Replaced or deleted while the sidecar was being written: it would
        // describe content nobody holds, and a later scrub would condemn the
        // replacement with it.
        if self.replaced_since(extent_id, &entry, generation) {
            if let Err(e) = compio::fs::remove_file(&ck_path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(extent_id, error = %e, "left checksums of replaced content");
                }
            }
            return skipped("its content was replaced while it was being scrubbed");
        }
        scrub_done(task, SCRUB_OUTCOME_DESCRIBED, format!("recorded {blocks} block(s)"))
    }

    fn op_in_flight(&self, extent_id: u64) -> bool {
        self.recovery_inflight.contains_key(&extent_id)
            || self.ec_convert_inflight.contains_key(&extent_id)
    }

    /// Has anything replaced this extent's content since `generation`?
    fn replaced_since(&self, extent_id: u64, entry: &ExtentEntry, generation: u64) -> bool {
        self.op_in_flight(extent_id)
            || entry.content_gen.load(Ordering::SeqCst) != generation
            || !self.extents.contains_key(&extent_id)
    }

    fn rot(&self, task: &ScrubTask, file: String, block: usize, what: &str) -> ScrubDone {
        tracing::error!(
            extent_id = task.extent_id,
            file = %file,
            block,
            "SCRUB FOUND CONTENT ROT — block {block} of {file} {what}"
        );
        scrub_done(task, SCRUB_OUTCOME_ROT, format!("block {block} of {file} {what}"))
    }

    /// Run tasks to completion now, unpaced, for tests. Their outcomes also
    /// land in the `df` queue, as a worker's would.
    pub async fn test_scrub(&self, tasks: Vec<ScrubTask>) -> Vec<ScrubDone> {
        let mut pacer = extent_scrub::ScrubPacer::new(0);
        let mut out = Vec::new();
        for task in tasks {
            let done = self.scrub_task(&task, &mut pacer).await;
            self.done.push_scrub_done(done.clone());
            out.push(done);
        }
        out
    }
}

fn scrub_done(task: &ScrubTask, outcome: u8, message: String) -> ScrubDone {
    ScrubDone {
        extent_id: task.extent_id,
        payload_location: task.payload_location,
        shard_index: task.shard_index,
        op_id: task.op_id,
        eversion: task.eversion,
        outcome,
        message,
    }
}

/// `.dat` or `.shard{i}`, for logs.
fn describe_file(p: PayloadRef) -> String {
    match p.location {
        PayloadLocation::InDat => ".dat".to_string(),
        PayloadLocation::InShardFile => format!(".shard{}", p.shard_index),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: usize = 1024 * 1024;

    async fn node(dir: &std::path::Path) -> ExtentNode {
        ExtentNode::new(ExtentNodeConfig::new(dir.to_path_buf(), 1))
            .await
            .expect("node")
    }

    /// A sealed `.dat` of `content`, durably held, the way a replica is once
    /// its appends have synced.
    async fn sealed_dat(node: &ExtentNode, eid: u64, content: &[u8]) -> Rc<ExtentEntry> {
        let entry = node.ensure_extent(eid).await.expect("entry");
        let f = node.extent_file(&entry).await.expect("file");
        file_pwrite_chunked(f, 0, Bytes::copy_from_slice(content))
            .await
            .expect("write");
        entry.note_durable_install(content.len() as u64);
        entry
    }

    fn dat_task(eid: u64, len: usize) -> ScrubTask {
        ScrubTask {
            extent_id: eid,
            payload_location: PayloadLocation::InDat.as_byte(),
            shard_index: 0,
            length: len as u64,
            eversion: 7,
            op_id: 42,
        }
    }

    fn flip(path: &std::path::Path, at: usize) {
        let mut b = std::fs::read(path).expect("read");
        b[at] ^= 0x01;
        std::fs::write(path, &b).expect("rot");
    }

    fn outcome(done: &[ScrubDone]) -> u8 {
        assert_eq!(done.len(), 1, "{done:?}");
        done[0].outcome
    }

    /// The first scrub records the content; the next one compares against it
    /// and finds a flipped bit, which is queued for the manager's `df` with the
    /// eversion the request carried.
    #[compio::test]
    async fn a_first_scrub_records_and_a_later_one_catches_rot() {
        let dir = tempfile::tempdir().unwrap();
        let node = node(dir.path()).await;
        let content: Vec<u8> = (0..(2 * MIB + 777)).map(|i| (i % 251) as u8).collect();
        sealed_dat(&node, 11, &content).await;
        let task = dat_task(11, content.len());

        let done = node.test_scrub(vec![task.clone()]).await;
        assert_eq!(outcome(&done), SCRUB_OUTCOME_DESCRIBED, "{done:?}");
        assert_eq!(done[0].op_id, 42, "the op id is echoed");
        let disk = node.disk_for(1).unwrap();
        assert!(disk.ck_path(11).exists());
        assert_eq!(outcome(&node.test_scrub(vec![task.clone()]).await), SCRUB_OUTCOME_CLEAN);

        flip(&disk.extent_path(11), MIB + 5);
        let done = node.test_scrub(vec![task]).await;
        assert_eq!(outcome(&done), SCRUB_OUTCOME_ROT, "{done:?}");
        assert!(done[0].message.contains("block 1"), "{done:?}");
        let queued = node.done.peek_scrub_done();
        let rot = queued.last().expect("queued for df");
        assert_eq!((rot.extent_id, rot.outcome, rot.eversion), (11, SCRUB_OUTCOME_ROT, 7));
    }

    /// Bytes that are simply gone mismatch nothing; a described block that can
    /// no longer be read in full is rot all the same.
    #[compio::test]
    async fn a_described_file_that_lost_its_tail_is_rot() {
        let dir = tempfile::tempdir().unwrap();
        let node = node(dir.path()).await;
        let content = vec![0x5Au8; 2 * MIB];
        let entry = sealed_dat(&node, 12, &content).await;
        let task = dat_task(12, content.len());
        assert_eq!(outcome(&node.test_scrub(vec![task.clone()]).await), SCRUB_OUTCOME_DESCRIBED);

        let f = node.extent_file(&entry).await.unwrap();
        f.set_len(MIB as u64 + 10).await.unwrap();
        let done = node.test_scrub(vec![task]).await;
        assert_eq!(outcome(&done), SCRUB_OUTCOME_ROT, "{done:?}");
    }

    /// A shard file is scrubbed the same way, against its own sidecar.
    #[compio::test]
    async fn a_shard_file_is_recorded_then_checked() {
        let dir = tempfile::tempdir().unwrap();
        let node = node(dir.path()).await;
        let shard: Vec<u8> = (0..(MIB + 300)).map(|i| (i * 7 % 253) as u8).collect();
        node.write_shard_stripe_local(13, 2, 0, 8 * MIB as u64, 2, Bytes::copy_from_slice(&shard))
            .await
            .expect("stage");
        let task = ScrubTask {
            extent_id: 13,
            payload_location: PayloadLocation::InShardFile.as_byte(),
            shard_index: 2,
            length: shard.len() as u64,
            eversion: 3,
            op_id: 0,
        };
        assert_eq!(outcome(&node.test_scrub(vec![task.clone()]).await), SCRUB_OUTCOME_DESCRIBED);
        let disk = node.disk_for(1).unwrap();
        assert!(disk.shard_ck_path(13, 2).exists());
        assert!(!disk.ck_path(13).exists(), "the shard's description is its own file");

        flip(&disk.shard_path(13, 2), MIB + 100);
        assert_eq!(outcome(&node.test_scrub(vec![task]).await), SCRUB_OUTCOME_ROT);
    }

    /// Nothing is recorded for content that is not there to describe: a file
    /// this node does not hold, a shard of another length, a replica still
    /// short of the seal (being caught up), or an extent an op is changing.
    #[compio::test]
    async fn content_that_is_not_settled_here_is_skipped_and_never_described() {
        let dir = tempfile::tempdir().unwrap();
        let node = node(dir.path()).await;
        let disk = node.disk_for(1).unwrap();

        assert_eq!(outcome(&node.test_scrub(vec![dat_task(20, 4096)]).await), SCRUB_OUTCOME_SKIPPED);

        sealed_dat(&node, 21, &[1u8; 4096]).await;
        assert_eq!(
            outcome(&node.test_scrub(vec![dat_task(21, 8192)]).await),
            SCRUB_OUTCOME_SKIPPED,
            "a replica short of the sealed length is behind, not describable"
        );
        assert!(!disk.ck_path(21).exists());

        node.write_shard_stripe_local(22, 0, 0, 1 << 20, 2, Bytes::from(vec![2u8; 4096]))
            .await
            .unwrap();
        let mut shard = dat_task(22, 8192);
        shard.payload_location = PayloadLocation::InShardFile.as_byte();
        assert_eq!(outcome(&node.test_scrub(vec![shard]).await), SCRUB_OUTCOME_SKIPPED);
        assert!(!disk.shard_ck_path(22, 0).exists());

        sealed_dat(&node, 23, &[3u8; 4096]).await;
        node.ec_convert_inflight.insert(23, ());
        assert_eq!(outcome(&node.test_scrub(vec![dat_task(23, 4096)]).await), SCRUB_OUTCOME_SKIPPED);
        assert!(!disk.ck_path(23).exists());
    }

    /// A scrub whose content is replaced under it (a repair installing new
    /// bytes) neither reports nor records what it read.
    #[compio::test]
    async fn content_replaced_mid_scrub_drops_the_result() {
        let dir = tempfile::tempdir().unwrap();
        let node = node(dir.path()).await;
        let content = vec![9u8; 3 * MIB];
        let entry = sealed_dat(&node, 30, &content).await;
        // 1 MiB/s: the first block goes at once, the second waits a second.
        let mut pacer = extent_scrub::ScrubPacer::new(MIB as u64);
        let task = dat_task(30, content.len());
        let (done, ()) = futures::join!(node.scrub_task(&task, &mut pacer), async {
            compio::time::sleep(std::time::Duration::from_millis(200)).await;
            entry.note_durable_install(content.len() as u64);
        });
        assert_eq!(done.outcome, SCRUB_OUTCOME_SKIPPED, "{done:?}");
        assert!(!node.disk_for(1).unwrap().ck_path(30).exists());
    }

    /// A repair that replaces content drops the old description first, so the
    /// next scrub records the new bytes instead of condemning them.
    #[compio::test]
    async fn forgetting_a_description_removes_it() {
        let dir = tempfile::tempdir().unwrap();
        let node = node(dir.path()).await;
        let content = vec![4u8; MIB];
        let entry = sealed_dat(&node, 31, &content).await;
        node.test_scrub(vec![dat_task(31, content.len())]).await;
        let ck = node.disk_for(1).unwrap().ck_path(31);
        assert!(ck.exists());
        let generation = entry.content_gen.load(Ordering::SeqCst);
        node.forget_description(&entry, &ck).await;
        assert!(!ck.exists());
        assert!(entry.content_gen.load(Ordering::SeqCst) > generation);
    }

    /// Deleting an extent takes every sidecar the scrub wrote; a discarded
    /// shard takes its own.
    #[compio::test]
    async fn delete_and_discard_take_the_sidecars_with_them() {
        let dir = tempfile::tempdir().unwrap();
        let node = node(dir.path()).await;
        let disk = node.disk_for(1).unwrap();
        let entry = sealed_dat(&node, 32, &[5u8; 4096]).await;
        node.write_shard_stripe_local(32, 3, 0, 1 << 20, 2, Bytes::from(vec![6u8; 4096]))
            .await
            .unwrap();
        node.write_shard_stripe_local(32, 4, 0, 1 << 20, 2, Bytes::from(vec![7u8; 4096]))
            .await
            .unwrap();
        let mut tasks = vec![dat_task(32, 4096)];
        for i in [3u32, 4] {
            let mut t = dat_task(32, 4096);
            t.payload_location = PayloadLocation::InShardFile.as_byte();
            t.shard_index = i;
            tasks.push(t);
        }
        for d in node.test_scrub(tasks).await {
            assert_eq!(d.outcome, SCRUB_OUTCOME_DESCRIBED, "{d:?}");
        }

        entry
            .discard_shard_file(&disk.shard_path(32, 3), 3)
            .await
            .unwrap();
        assert!(!disk.shard_ck_path(32, 3).exists(), "discard left the sidecar");

        std::fs::remove_file(disk.shard_path(32, 4)).unwrap();
        disk.remove_extent_files(32).await.unwrap();
        assert!(!disk.ck_path(32).exists());
        assert!(
            !disk.shard_ck_path(32, 4).exists(),
            "a sidecar whose shard was already gone survived the delete"
        );
    }

    /// A file two ops ask for is read once and reported to BOTH — the second
    /// op must not wait forever for an outcome nobody sends — and each op is
    /// listed as queued until its outcome is out.
    #[compio::test]
    async fn a_file_asked_for_by_two_ops_is_reported_to_both() {
        let dir = tempfile::tempdir().unwrap();
        let node = node(dir.path()).await;
        sealed_dat(&node, 50, &[1u8; 4096]).await;
        let mut second = dat_task(50, 4096);
        second.op_id = 43;
        // Queued without a worker yet: the worker is started by the call, so
        // both land in the queue before it runs (no await in between).
        node.enqueue_scrub(vec![dat_task(50, 4096), second]);
        assert_eq!(node.done.scrub_queued_ops(), vec![42, 43]);
        let mut done = Vec::new();
        for _ in 0..100 {
            done.extend(node.done.take_scrub_done());
            if done.len() >= 2 {
                break;
            }
            compio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let mut ops: Vec<u64> = done.iter().map(|d| d.op_id).collect();
        ops.sort();
        assert_eq!(ops, vec![42, 43], "{done:?}");
        assert!(node.done.scrub_queued_ops().is_empty());
    }

    /// The request path: tasks are queued, a worker drains them in the
    /// background, and every outcome reaches the `df` queue.
    #[compio::test]
    async fn queued_tasks_are_drained_and_reported() {
        let dir = tempfile::tempdir().unwrap();
        let node = node(dir.path()).await;
        sealed_dat(&node, 40, &[1u8; 4096]).await;
        sealed_dat(&node, 41, &[2u8; 4096]).await;
        let req = ScrubExtentsReq {
            tasks: vec![dat_task(40, 4096), dat_task(41, 4096), dat_task(40, 4096)],
        };
        node.handle_scrub_extents(rkyv_encode(&req)).await.expect("accepted");
        let mut done = Vec::new();
        for _ in 0..100 {
            done.extend(node.done.take_scrub_done());
            if done.len() >= 2 {
                break;
            }
            compio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let mut ids: Vec<u64> = done.iter().map(|d| d.extent_id).collect();
        ids.sort();
        assert_eq!(ids, vec![40, 41], "a file queued twice by one op is scrubbed once: {done:?}");
        assert!(done.iter().all(|d| d.outcome == SCRUB_OUTCOME_DESCRIBED), "{done:?}");
        assert!(!node.scrub_worker_running.get(), "the worker exits when the queue is empty");
    }
}
