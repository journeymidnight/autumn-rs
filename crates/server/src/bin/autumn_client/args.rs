//! `autumn-client` CLI argument parsing: `Command` / `Args` + the hand-rolled
//! `parse_args` and its deprecation-warn helpers. Split out of `main.rs`
//! (2026-06-25, behaviour-preserving). `usage()` / `warn_*_deprecated_once`
//! stay private — only `parse_args` uses them; `Command` / `Args` / `parse_args`
//! are `pub(crate)` for `main.rs`'s dispatcher.

pub(crate) enum Command {
    /// F213: stub that points to `autumn-op` (all op subcommands moved).
    /// `args` carries everything after the literal `op` token so we can
    /// suggest the equivalent autumn-op invocation.
    OpStub {
        args: Vec<String>,
    },
    Put {
        key: String,
        file: String,
    },
    /// F129: multipart upload of a large value via PutBegin/Chunk/Commit.
    /// Reads `file` (or stdin if file = "-"), splits into `chunk_size`
    /// byte chunks, and commits.
    PutStream {
        key: String,
        file: String,
        chunk_size: usize,
    },
    /// F129: streaming read. Walks the value via offset/length GetReqs
    /// and writes chunks to stdout (or `out` if provided).
    GetStream {
        key: String,
        chunk_size: u32,
        out: Option<String>,
    },
    /// F216-E verification: zero-copy PUT via ClusterClient::put_zc. Reads
    /// `file` into a Bytes, registers it for UCX zero-copy send (ucx build),
    /// writes via MSG_PUT_ZC (value sent as its own iovec, no client-side copy).
    PutZc {
        key: String,
        file: String,
    },
    Get {
        key: String,
    },
    /// F259: GET via MSG_GET_REDIRECT + EN direct read (byte-for-byte
    /// comparable against plain `get` for verification).
    DirectGet {
        key: String,
    },
    /// F216 verification: zero-copy GET via ClusterClient::get_into. heads
    /// the key, allocates a dest buffer, registers it for UCX zero-copy
    /// (ucx build only), reads the value straight into dest, writes to stdout.
    ZcGet {
        key: String,
    },
    Del {
        key: String,
    },
    Head {
        key: String,
    },
    Ls {
        prefix: String,
        start: String,
        limit: u32,
    },
    PerfCheck {
        threads: usize,
        duration_secs: u64,
        value_size: usize,
        baseline_file: String,
        threshold: f64,
        update_baseline: bool,
        partitions: usize,
        pipeline_depth: usize,
        /// F195: was env `AUTUMN_GROUP_COMMIT_CAP`; recorded in baseline.
        group_commit_cap: Option<usize>,
        /// Unified bulk size for both write + read phases. When > 0:
        /// write loop calls `put_many(N items)`; read loop calls
        /// `get_many_into(N items)`. Each `put_many` group becomes one
        /// MSG_BATCH_PUT frame per partition; `get_many_into` does
        /// client-side fan-out internally. Replaces the three earlier
        /// flags (`--batch-put` / `--batch-get` / `--put-many`) — all
        /// three drove the same code paths after the SDK consolidation.
        bulk: usize,
        /// F258-bench: per-thread start stagger (tid*ramp_ms) + connect
        /// warmup + barrier-aligned timed window. 0 = legacy behavior.
        ramp_ms: u64,
        /// F259: read phase uses get_direct (MSG_GET_REDIRECT + EN direct
        /// read) instead of get/get_into.
        direct_read: bool,
    },
    /// YCSB-equivalent mixed-workload benchmark. Unlike perf-check (pure
    /// write phase then pure read phase), this runs ONE mixed loop with a
    /// configurable read/write ratio and key-access distribution — so it
    /// reproduces the standard YCSB workloads:
    ///   A=--read-ratio 0.5   B=--read-ratio 0.95   C=--read-ratio 1.0
    ///   D=--read-ratio 0.95 (read-latest ≈ zipfian)   F=--rmw
    /// A LOAD phase inserts `records` keys per thread first (YCSB "load").
    Ycsb {
        threads: usize,
        duration_secs: u64,
        value_size: usize,
        partitions: usize,
        pipeline_depth: usize,
        /// Fraction of RUN-phase ops that are reads (rest are updates).
        read_ratio: f64,
        /// true = zipfian (hot-key skew, YCSB default); false = uniform.
        zipfian: bool,
        /// Keyspace size PER THREAD (YCSB recordcount, scoped per partition).
        records: u64,
        /// Workload F: each op is a read-modify-write (get then put).
        rmw: bool,
    },
}

pub(crate) struct Args {
    pub(crate) manager: String,
    pub(crate) command: Command,
    pub(crate) transport: autumn_transport::TransportKind,
    /// Per-thread regpool cap (pinned/registered bytes). `None` = library
    /// default (512 MiB/thread). Useful for perf-check tuning — large
    /// `--threads --pipeline-depth` 8 MiB workloads can pin many slabs
    /// in-flight and benefit from a higher cap; constrained hosts can
    /// shrink to fit. Clamped to [16 MiB, 64 GiB].
    pub(crate) ucx_regpool_cap_bytes: Option<usize>,
    /// F-NS-PRINCIPAL-UNIFIED: the key-prefix SCOPE every data-plane KV command
    /// (put/get/del/head/ls/put-stream/get-stream) writes/reads within — a whole
    /// namespace (`fs`, `gallery`) or an in-namespace sub-prefix (`mem/agent7`).
    /// REQUIRED for those commands (a write must declare its scope); perf-check/
    /// ycsb bind the `bench/perf` scope internally and don't need it.
    pub(crate) namespace: Option<String>,
    /// F-AUTHZ-BUILTIN: path to a file holding this client's authz credential
    /// (`<principal>\n<hex>`, from `autumn-op principal-create`). REQUIRED for KV
    /// commands when the target namespace is protected; omit on an authz-off
    /// cluster. The principal identity is read from the file.
    pub(crate) credential_file: Option<String>,
}

fn usage() -> ! {
    eprintln!("Usage: autumn-client --manager <ADDR> <COMMAND>");
    eprintln!();
    eprintln!("Data-plane commands (KV + bench):");
    eprintln!("  put <KEY> <FILE>                  Put key with value from file");
    eprintln!("  put-stream [--chunk-size N] <KEY> <FILE-or->>");
    eprintln!(
        "                                    Chunked stream put (default 4 MiB chunks; F186)"
    );
    eprintln!("  get <KEY>                         Get value for key");
    eprintln!("  get-stream [--chunk-size N] [--out FILE] <KEY>");
    eprintln!("                                    Chunked stream get (default 4 MiB chunks)");
    eprintln!("  del <KEY>                         Delete key");
    eprintln!("  head <KEY>                        Get key metadata (size)");
    eprintln!("  ls [--prefix P] [--start S] [--limit N]  List keys");
    eprintln!("  perf-check [--threads 256] [--duration 10] [--size 4096] [--baseline perf_baseline.json] [--threshold 0.8] [--update-baseline] [--partitions N] [--pipeline-depth K]   (zero-copy auto on --transport ucx)");
    eprintln!("                                    Quick write+read bench; warns if >threshold regression vs baseline");
    eprintln!("  ycsb [--threads 32] [--duration 30] [--size 1024] [--partitions N] [--pipeline-depth 16] [--read-ratio 0.5] [--key-dist zipfian|uniform] [--records 100000] [--rmw]");
    eprintln!("                                    YCSB-equivalent mixed workload (A=0.5 B=0.95 C=1.0 D=0.95 F=--rmw); LOAD then mixed RUN");
    eprintln!();
    eprintln!("Operator / admin commands moved to `autumn-op` (F213):");
    eprintln!("  bootstrap, set-stream-ec, force-ec-convert, split, merge,");
    eprintln!("  compact, gc, forcegc, register-node, format, info, policy-candidates");
    eprintln!("  Run `autumn-op --help` for the full list.");
    std::process::exit(1);
}

/// CLI value at `raw[i]` or print usage + exit — for a value that FOLLOWS a
/// flag (`i` already advanced past it). A bare `raw[i]` panics with "index out
/// of bounds" when the value was omitted (e.g. a trailing `--manager` /
/// `perf-check --threads`); this surfaces usage instead. (#3)
fn val(raw: &[String], i: usize) -> &str {
    match raw.get(i) {
        Some(s) => s.as_str(),
        None => usage(),
    }
}

pub(crate) fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().collect();
    let mut manager = String::from("127.0.0.1:9001");
    let mut transport = autumn_transport::TransportKind::Tcp;
    let mut ucx_regpool_cap_bytes: Option<usize> = None;
    let mut namespace: Option<String> = None;
    let mut credential_file: Option<String> = None;
    let mut i = 1;

    while i < raw.len() {
        match raw[i].as_str() {
            "--manager" => {
                i += 1;
                manager = val(&raw, i).to_owned();
                i += 1;
            }
            // F-NS-PRINCIPAL-UNIFIED: the key-prefix scope for data-plane KV cmds.
            "--namespace" | "--scope" => {
                i += 1;
                namespace = Some(val(&raw, i).to_owned());
                i += 1;
            }
            "--credential-file" => {
                i += 1;
                credential_file = Some(val(&raw, i).to_owned());
                i += 1;
            }
            "--transport" => {
                i += 1;
                transport = autumn_transport::parse_transport_flag(val(&raw, i)).unwrap_or_else(|bad| {
                    eprintln!("--transport must be `tcp` or `ucx`, got {bad:?}");
                    std::process::exit(2);
                });
                i += 1;
            }
            "--ucx-regpool-cap-bytes" => {
                i += 1;
                ucx_regpool_cap_bytes = Some(
                    val(&raw, i)
                        .parse()
                        .expect("--ucx-regpool-cap-bytes usize"),
                );
                i += 1;
            }
            "--help" | "-h" => usage(),
            _ => break,
        }
    }

    if i >= raw.len() {
        usage();
    }

    let subcmd = raw[i].as_str();
    i += 1;

    let command = match subcmd {
        // F213: op commands moved to autumn-op. Common typos and the
        // explicit `op` namespace prefix all route here.
        "op" | "bootstrap" | "set-stream-ec" | "force-ec-convert" | "split" | "merge"
        | "policy-candidates" | "policy_candidates" | "policy" | "compact" | "gc" | "forcegc"
        | "register-node" | "format" | "info" => {
            // Reconstruct the equivalent autumn-op invocation. For
            // bare `op`, the original args[i..] are the autumn-op
            // command + flags. For other typos (e.g. `autumn-client
            // split 1`), we re-prepend the matched subcommand so the
            // hint is `autumn-op split 1`.
            let mut args: Vec<String> = Vec::new();
            if subcmd != "op" {
                args.push(subcmd.to_string());
            }
            args.extend(raw[i..].iter().cloned());
            Command::OpStub { args }
        }
        "put" => {
            while i < raw.len() && raw[i].starts_with('-') {
                if raw[i] == "--nosync" {
                    warn_nosync_deprecated_once();
                }
                i += 1;
            }
            if i + 1 >= raw.len() {
                eprintln!("put requires <KEY> <FILE>");
                std::process::exit(1);
            }
            let key = raw[i].clone();
            let file = raw[i + 1].clone();
            Command::Put { key, file }
        }
        "put-zc" => {
            if i + 1 >= raw.len() {
                eprintln!("put-zc requires <KEY> <FILE>");
                std::process::exit(1);
            }
            Command::PutZc {
                key: raw[i].clone(),
                file: raw[i + 1].clone(),
            }
        }
        "put-stream" | "putstream" => {
            // put-stream [--chunk-size N] <KEY> <FILE-or-->
            let mut chunk_size: usize = 4 * 1024 * 1024; // 4 MiB default
            while i < raw.len() && raw[i].starts_with('-') {
                if raw[i] == "--chunk-size" && i + 1 < raw.len() {
                    chunk_size = raw[i + 1].parse().unwrap_or(chunk_size);
                    i += 2;
                    continue;
                }
                i += 1;
            }
            if i + 1 >= raw.len() {
                eprintln!("put-stream requires <KEY> <FILE-or-->");
                std::process::exit(1);
            }
            let key = raw[i].clone();
            let file = raw[i + 1].clone();
            Command::PutStream {
                key,
                file,
                chunk_size,
            }
        }
        "get-stream" | "getstream" => {
            // get-stream [--chunk-size N] [--out FILE] <KEY>
            let mut chunk_size: u32 = 4 * 1024 * 1024;
            let mut out: Option<String> = None;
            while i < raw.len() && raw[i].starts_with('-') {
                if raw[i] == "--chunk-size" && i + 1 < raw.len() {
                    chunk_size = raw[i + 1].parse().unwrap_or(chunk_size);
                    i += 2;
                    continue;
                }
                if raw[i] == "--out" && i + 1 < raw.len() {
                    out = Some(raw[i + 1].clone());
                    i += 2;
                    continue;
                }
                i += 1;
            }
            if i >= raw.len() {
                eprintln!("get-stream requires <KEY>");
                std::process::exit(1);
            }
            Command::GetStream {
                key: raw[i].clone(),
                chunk_size,
                out,
            }
        }
        "get" => {
            if i >= raw.len() {
                eprintln!("get requires <KEY>");
                std::process::exit(1);
            }
            Command::Get {
                key: raw[i].clone(),
            }
        }
        // (DirectGet parsed above in "direct-get".)
        "direct-get" => {
            if i >= raw.len() {
                eprintln!("direct-get requires <KEY>");
                std::process::exit(1);
            }
            Command::DirectGet {
                key: raw[i].clone(),
            }
        }
        "zc-get" => {
            if i >= raw.len() {
                eprintln!("zc-get requires <KEY>");
                std::process::exit(1);
            }
            Command::ZcGet {
                key: raw[i].clone(),
            }
        }
        "del" => {
            while i < raw.len() && raw[i].starts_with('-') {
                if raw[i] == "--nosync" {
                    warn_nosync_deprecated_once();
                }
                i += 1;
            }
            if i >= raw.len() {
                eprintln!("del requires <KEY>");
                std::process::exit(1);
            }
            Command::Del {
                key: raw[i].clone(),
            }
        }
        "head" => {
            if i >= raw.len() {
                eprintln!("head requires <KEY>");
                std::process::exit(1);
            }
            Command::Head {
                key: raw[i].clone(),
            }
        }
        "ls" => {
            let mut prefix = String::new();
            let mut start = String::new();
            let mut limit: u32 = 100;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--prefix" => {
                        i += 1;
                        prefix = val(&raw, i).to_owned();
                    }
                    "--start" => {
                        i += 1;
                        start = val(&raw, i).to_owned();
                    }
                    "--limit" => {
                        i += 1;
                        limit = val(&raw, i).parse().expect("--limit must be a number");
                    }
                    _ => {
                        if prefix.is_empty() {
                            prefix = val(&raw, i).to_owned();
                        }
                    }
                }
                i += 1;
            }
            Command::Ls {
                prefix,
                start,
                limit,
            }
        }
        "perf-check" => {
            let mut threads = 256usize;
            let mut duration_secs = 10u64;
            let mut value_size = 4096usize;
            let mut baseline_file = "perf_baseline.json".to_string();
            let mut threshold = 0.8f64;
            let mut update_baseline = false;
            let mut partitions_meta_from_flag: usize = 1;
            let mut pipeline_depth: usize = 1;
            // Experimental: when > 0, the write loop bundles N keys per
            // `put_many` call (client-side fan-out) instead of issuing one
            // `kv_put` per iteration. Tests whether writer_task's
            // tcp_sendmsg coalescing + read_loop bulk decode gives the
            // server-side "fat arrival" partition_loop needs to grow
            // batch_size. 0 = legacy per-op fan_out.
            let mut ramp_ms: u64 = 0;
            let mut direct_read = false;
            let mut bulk: usize = 0;
            // F195: was `AUTUMN_GROUP_COMMIT_CAP` env read at baseline-
            // write time. Now an explicit CLI flag — operators pass the
            // same value to autumn-ps (`--group-commit-cap N`) AND to
            // perf-check (`--group-commit-cap N`) so the baseline JSON
            // reflects the server config that was active.
            let mut group_commit_cap: Option<usize> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--zc" => {
                        // F216-E: removed. Zero-copy is now the DEFAULT on the
                        // UCX transport (writes always; reads when value >=
                        // UCX_ZC_READ_MIN_BYTES). Kept as a no-op so existing
                        // perf_check.sh / scripts don't hard-fail; on TCP it
                        // stays the regular path. Same spirit as --nosync.
                        warn_zc_flag_deprecated_once();
                    }
                    "--threads" | "-t" => {
                        i += 1;
                        threads = val(&raw, i).parse().expect("--threads must be a number");
                    }
                    "--duration" | "-d" => {
                        i += 1;
                        duration_secs = val(&raw, i).parse().expect("--duration must be a number");
                    }
                    "--size" => {
                        i += 1;
                        value_size = val(&raw, i).parse().expect("--size must be a number");
                    }
                    "--nosync" => {
                        warn_nosync_deprecated_once();
                    }
                    "--baseline" => {
                        i += 1;
                        baseline_file = val(&raw, i).to_owned();
                    }
                    "--threshold" => {
                        i += 1;
                        threshold = val(&raw, i).parse().expect("--threshold must be a float");
                    }
                    "--update-baseline" => {
                        update_baseline = true;
                    }
                    "--partitions" => {
                        i += 1;
                        partitions_meta_from_flag = val(&raw, i)
                            .parse()
                            .expect("--partitions must be a positive integer");
                        if partitions_meta_from_flag == 0 {
                            eprintln!("--partitions must be >= 1");
                            usage();
                        }
                    }
                    "--pipeline-depth" => {
                        i += 1;
                        pipeline_depth = val(&raw, i)
                            .parse()
                            .expect("--pipeline-depth must be a positive integer");
                        if pipeline_depth == 0 || pipeline_depth > 256 {
                            eprintln!("--pipeline-depth must be in [1, 256]");
                            usage();
                        }
                    }
                    "--bulk" => {
                        i += 1;
                        bulk = val(&raw, i)
                            .parse()
                            .expect("--bulk must be a non-negative integer");
                    }
                    "--direct-read" => {
                        direct_read = true;
                    }
                    "--ramp-ms" => {
                        // F258-bench: stagger thread start by tid*ramp_ms so
                        // UCX worker creation doesn't storm (host-level devx
                        // serialization); threads then warm up (connect) and
                        // align on a barrier before the timed window starts.
                        i += 1;
                        ramp_ms = val(&raw, i).parse().expect("--ramp-ms must be u64");
                    }
                    // Migration: print + abort. The three pre-consolidation
                    // flags drove the same path post-SDK-merge.
                    "--batch-put" | "--batch-get" | "--put-many" => {
                        eprintln!(
                            "{}: removed — use --bulk N (one knob driving both phases)",
                            val(&raw, i)
                        );
                        usage();
                    }
                    "--group-commit-cap" => {
                        i += 1;
                        group_commit_cap =
                            Some(val(&raw, i).parse().expect("--group-commit-cap must be a u64"));
                    }
                    other => {
                        eprintln!("unknown perf-check flag: {other}");
                        usage();
                    }
                }
                i += 1;
            }
            Command::PerfCheck {
                threads,
                duration_secs,
                value_size,
                baseline_file,
                threshold,
                update_baseline,
                partitions: partitions_meta_from_flag,
                pipeline_depth,
                group_commit_cap,
                bulk,
                ramp_ms,
                direct_read,
            }
        }
        "ycsb" => {
            let mut threads = 32usize;
            let mut duration_secs = 30u64;
            let mut value_size = 1024usize; // YCSB default record ~1 KiB
            let mut partitions: usize = 1;
            let mut pipeline_depth: usize = 16;
            let mut read_ratio = 0.5f64; // workload A
            let mut zipfian = true; // YCSB default distribution
            let mut records: u64 = 100_000; // per-thread keyspace
            let mut rmw = false;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--threads" | "-t" => {
                        i += 1;
                        threads = val(&raw, i).parse().expect("--threads must be a number");
                    }
                    "--duration" | "-d" => {
                        i += 1;
                        duration_secs = val(&raw, i).parse().expect("--duration must be a number");
                    }
                    "--size" => {
                        i += 1;
                        value_size = val(&raw, i).parse().expect("--size must be a number");
                    }
                    "--partitions" => {
                        i += 1;
                        partitions = val(&raw, i).parse().expect("--partitions must be >= 1");
                    }
                    "--pipeline-depth" => {
                        i += 1;
                        pipeline_depth = val(&raw, i).parse().expect("--pipeline-depth must be >= 1");
                    }
                    "--read-ratio" => {
                        i += 1;
                        read_ratio = val(&raw, i).parse().expect("--read-ratio must be a float");
                        if !(0.0..=1.0).contains(&read_ratio) {
                            eprintln!("--read-ratio must be in [0.0, 1.0]");
                            usage();
                        }
                    }
                    "--key-dist" => {
                        i += 1;
                        match val(&raw, i) {
                            "zipfian" => zipfian = true,
                            "uniform" => zipfian = false,
                            o => {
                                eprintln!("--key-dist must be zipfian|uniform, got {o}");
                                usage();
                            }
                        }
                    }
                    "--records" => {
                        i += 1;
                        records = val(&raw, i).parse().expect("--records must be a number");
                    }
                    "--rmw" => {
                        rmw = true;
                    }
                    other => {
                        eprintln!("unknown ycsb flag: {other}");
                        usage();
                    }
                }
                i += 1;
            }
            Command::Ycsb {
                threads,
                duration_secs,
                value_size,
                partitions,
                pipeline_depth,
                read_ratio,
                zipfian,
                records,
                rmw,
            }
        }
        other => {
            eprintln!("unknown command: {other}");
            usage();
        }
    };

    Args {
        manager,
        command,
        transport,
        ucx_regpool_cap_bytes,
        namespace,
        credential_file,
    }
}

/// F178 Phase 3: `--nosync` was removed because writes are now ALWAYS
/// durable via the per-extent fsync coalescer (Phase 1) + flush-time
/// quorum durability wait (Phase 2). The flag is kept as a parser
/// no-op so existing scripts (perf_check.sh, ad-hoc invocations) don't
/// hard-fail; callers always get the durable path. Logged once per
/// invocation so deprecation is visible.
fn warn_nosync_deprecated_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        eprintln!(
            "[autumn-client] note: --nosync was removed in F178 (LevelDB-style \
             coalescing makes writes always durable). Flag ignored, behaviour \
             unchanged from --sync."
        );
    });
}

fn warn_zc_flag_deprecated_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        eprintln!(
            "[autumn-client] note: --zc was removed (F216-E). Zero-copy is now \
             the DEFAULT on --transport ucx: writes always; reads when value \
             >= {} B. On --transport tcp the regular path is used. Flag ignored.",
            autumn_client::UCX_ZC_READ_MIN_BYTES
        );
    });
}
