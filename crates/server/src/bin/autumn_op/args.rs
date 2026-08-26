//! `autumn-op` CLI argument parsing: `Args` / `Command` + the hand-rolled
//! parser and its value-parse helpers. Split out of `main.rs` (2026-06-24,
//! behaviour-preserving). `usage()` / `parse_admin_flags` / `parse_byte_size`
//! / `parse_ec_flag` stay private — only `parse()` (and the unit tests) use
//! them; the rest is `pub(crate)` for `main.rs`'s dispatcher + bootstrap.

use anyhow::{bail, Context, Result};
use autumn_transport::TransportKind;

fn usage() -> ! {
    eprintln!("usage: autumn-op [--manager addr] [--json] [--admin-token-file F] <command>");
    eprintln!("  --admin-token-file F: authorize cluster-mutating ops (fence/remove/merge/create-stream/…)");
    eprintln!("    when the manager was started with --admin-token-file. Read-only ops ignore it.");
    eprintln!();
    eprintln!("read / observability commands:");
    eprintln!("  list-nodes                   show every EN's auto-state + override");
    eprintln!("  extent-health [--node ID] [--all]");
    eprintln!("                               per-slot health (default: only unhealthy)");
    eprintln!("  list-ec-markers              ConvertToEc inflight markers + coord state");
    eprintln!("  recovery-stats               in-flight + per-source/target counters");
    eprintln!("  audit-log [--op N] [--node N] [--since S] [--until U] [--limit L]");
    eprintln!("                               query operator action history");
    eprintln!("  info [--part PID] [--detail] show cluster snapshot (--detail = partition load)");
    eprintln!(
        "  policy-candidates            advisory split/merge/gc/compact/ec candidates"
    );
    eprintln!("  cluster-version              persisted cluster_version + manager wire interval (R1)");
    eprintln!();
    eprintln!("op observability:");
    eprintln!("  ops status <id>              live state of one op, incl. progress");
    eprintln!("  ops list [--active] [--kind K] [--limit N]   live ledger (leader-local)");
    eprintln!("  ops history [--kind K] [--since UNIX] [--limit N]");
    eprintln!("                               durable terminal history from etcd, newest first");
    eprintln!();
    eprintln!("upgrade versioning:");
    eprintln!("  upgrade-version [--to N]     bump cluster_version (default current+1); run ONLY");
    eprintln!("                               after every member binary is upgraded; not rollbackable");
    eprintln!();
    eprintln!("node-lifecycle admin commands:");
    eprintln!("  fence-node <id> --reason \"...\" --by alice [--force]");
    eprintln!("  maintenance <id> --reason \"...\" --by alice [--expire UNIX_TS]");
    eprintln!("  unfence <id> --by alice");
    eprintln!("  remove <id> --by alice");
    eprintln!();
    eprintln!("cluster / partition admin commands (moved from autumn-client):");
    eprintln!("  bootstrap [--replication 3+0] [--log-ec K+M] [--row-ec K+M]");
    eprintln!("                               (--presplit RETIRED — use `presplit --namespace <NS>` after bootstrap)");
    eprintln!("  set-stream-ec --stream <ID> --ec K+M");
    eprintln!("  force-ec-convert --extent <EXTID>");
    eprintln!("  split <PARTID> [--namespace <NS> --tenant <T> [--at <SUFFIX> | --at-hex <HEX>]] [--at-raw-hex <HEX>]");
    eprintln!("  presplit --namespace <fs|kvc|mem> --tenant <T> ...   (presplit EMPTY keyspace before loading)");
    eprintln!("           fs: --lanes <N> [--parts <P>] [--admin-token-file F to record] [--force to narrow declared lanes]");
    eprintln!("      fs:  --lanes <N> [--parts <P>] | --fs-inos <i,j,…> | --count <N>");
    eprintln!("           (--lanes = stripe width, declared; --parts = partitions to cut, MUST divide lanes; default P=N)");
    eprintln!("      kvc: --count <N> --hash-prefix <rel-prefix>       (split by content-hash first hex char)");
    eprintln!("           hash-prefix = rel prefix down to just before the hash, e.g. '<model>/vllm/v1/'");
    eprintln!("      mem: --agents <a,b,…>                            (split by agent boundary)");
    eprintln!("  merge <SURVIVOR_PARTID> <VICTIM_PARTID>");
    eprintln!("  rebalance [MAX_MOVES]        actively re-spread partitions across PS (0/absent = balance fully)");
    eprintln!("  compact <PARTID>");
    eprintln!("  gc [--ratio R] [--max-size B] [--stream-debt B] [--empty-only] <PARTID>");
    eprintln!("  forcegc <PARTID> <EXTID>...");
    // `register-node` removed; `format` is the single per-EN setup.
    // M1c: format is IDENTITY-ONLY — it no longer takes a
    // location (no --listen/--advertise/--shard-ports). The EN binary's own
    // `--advertise` self-registers the live location at every startup.
    eprintln!("  format <DIR>...");
    eprintln!(
        "                               format dir(s), register node identity, stamp cluster_id"
    );
    eprintln!();
    eprintln!("namespace registry (admin — needs --admin-token[-file]):");
    eprintln!("  namespace-create --name <NS> [--with-tenant <T>] [--presplit <hex,hex,...>] --admin-token <TOK>");
    eprintln!("                               register a namespace (--with-tenant marks it protected)");
    eprintln!("  namespace-delete --name <NS> [--force] --admin-token <TOK>");
    eprintln!("                               remove the registry row (refuses non-empty unless --force)");
    eprintln!("  namespace-list [--json]      list registered namespaces (name/prefix/owner/presplit/created)");
    eprintln!("  principal-list [--json]      list principals + their grants (read-only, no credentials)");
    std::process::exit(1);
}

/// The CLI value at `raw[i]`, or print usage + exit. Use for every value that
/// FOLLOWS a flag (`i` already advanced past the flag) or a required
/// positional: a bare `raw[i]` panics with "index out of bounds" when the
/// value was omitted (e.g. a trailing `--reason` or `split` with no PARTID) —
/// a confusing crash instead of the usage message.
fn val(raw: &[String], i: usize) -> &str {
    match raw.get(i) {
        Some(s) => s.as_str(),
        None => usage(),
    }
}

/// Parse a required positional as a number, printing a NAMED error + usage
/// instead of panicking with a raw `ParseIntError` backtrace. `--help`/`-h` in a
/// positional slot prints usage (so `autumn-op split --help` works). Missing arg
/// → usage.
fn num_arg<T: std::str::FromStr>(raw: &[String], i: usize, name: &str) -> T {
    let s = val(raw, i);
    if s == "--help" || s == "-h" {
        usage();
    }
    s.trim().parse().unwrap_or_else(|_| {
        eprintln!("autumn-op: {name} must be a number, got {s:?}");
        usage()
    })
}

/// read a secret (admin token / tenant credential) from a file,
/// trimming a trailing newline. Preferred over passing secrets on argv, which
/// leak via `ps` / `/proc/<pid>/cmdline` (coco P2). Fatal on read error.
fn read_secret_file(path: &str) -> String {
    match std::fs::read_to_string(path) {
        Ok(s) => s.trim_end_matches(['\n', '\r']).to_string(),
        Err(e) => {
            eprintln!("autumn-op: cannot read secret file {path}: {e}");
            std::process::exit(2);
        }
    }
}

/// D2: parse the `--presplit` value = a comma-separated list of HEX
/// split points into raw bytes. Empty / whitespace-only → no points. Each token
/// must be an even-length ASCII hex string (a raw split key). Bad hex → usage().
/// These are FROZEN for D8 (stored in the registry, not acted upon in SD-1).
fn parse_hex_split_points(spec: &str) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for tok in spec.split(',') {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        if !tok.is_ascii() || !tok.len().is_multiple_of(2) {
            eprintln!("autumn-op: --presplit point '{tok}' must be even-length ASCII hex");
            usage();
        }
        let mut bytes = Vec::with_capacity(tok.len() / 2);
        let b = tok.as_bytes();
        let mut j = 0;
        while j < b.len() {
            match u8::from_str_radix(&tok[j..j + 2], 16) {
                Ok(v) => bytes.push(v),
                Err(_) => {
                    eprintln!("autumn-op: --presplit point '{tok}' has non-hex chars");
                    usage();
                }
            }
            j += 2;
        }
        out.push(bytes);
    }
    out
}

pub(crate) struct Args {
    pub(crate) manager: String,
    pub(crate) json: bool,
    pub(crate) transport: TransportKind,
    /// shared admin secret for cluster-mutating ops
    /// (`--admin-token[-file]`). Applied to the client at connect via
    /// `set_admin_token`; read-only commands are unaffected. `None` = don't send
    /// a token (a token-less manager runs these bare; a token-ON manager refuses).
    pub(crate) admin_token: Option<String>,
    /// `--wait`: for the async op triggers (split/merge/rebalance/compact/gc/
    /// forcegc/force-ec-convert), block until the op reaches a terminal state
    /// and print/exit on its real outcome, instead of returning the op id.
    pub(crate) wait: bool,
    /// `--timeout SECS` for `--wait` (default 600). 0 keeps the default.
    pub(crate) wait_timeout: u64,
    pub(crate) cmd: Command,
}

/// (D4) — where `autumn-op split` cuts the partition.
///
/// The wire (SPLIT_PART) only ever carries a RAW byte key (partition layer is
/// namespace-agnostic, D5). This enum is the CLI-side INTENT; `resolve_at_key`
/// lowers it to the raw wire key. Per the §3.4 CLI refinement (细化三 term
/// rule: prefixes are an implementation detail, the user面 speaks only
/// namespace/tenant):
/// - `Median` — no explicit point; PS picks the median (legacy).
/// - `Namespaced { namespace, tenant, suffix }` — the normal operator form.
///   The cut key = `"{tenant}/{namespace}/" ++ suffix` (TENANT-FIRST). An EMPTY
///   `suffix` cuts exactly at the pair boundary `"{tenant}/{namespace}/"`
///   (splits one tenant's namespace off — a common op).
/// - `Raw(bytes)` — admin-only escape hatch (`--at-raw-hex`); the caller hand-
///   builds the whole prefix. Same posture as D7 ⑤ `raw()`: documented
///   admin-only, not the day-to-day path.
pub(crate) enum SplitPoint {
    Median,
    Namespaced {
        namespace: String,
        tenant: String,
        suffix: Vec<u8>,
    },
    Raw(Vec<u8>),
}

impl SplitPoint {
    /// Lower the CLI intent to the RAW wire key (`None` = PS median).
    pub(crate) fn resolve_at_key(&self) -> Option<Vec<u8>> {
        match self {
            SplitPoint::Median => None,
            SplitPoint::Namespaced {
                namespace,
                tenant,
                suffix,
            } => {
                // cut key = `{namespace}/` (+ `{tenant}/`
                // as an in-namespace sub-segment, e.g. mem/kvc; empty for fs).
                let mut key = format!("{namespace}/").into_bytes();
                if !tenant.is_empty() {
                    key.extend_from_slice(format!("{tenant}/").as_bytes());
                }
                key.extend_from_slice(suffix);
                Some(key)
            }
            SplitPoint::Raw(bytes) => Some(bytes.clone()),
        }
    }

    /// Human-readable rendering of the resolved cut point for messages.
    pub(crate) fn describe(&self) -> String {
        match self {
            SplitPoint::Median => "median (PS-selected)".to_string(),
            SplitPoint::Namespaced {
                namespace,
                tenant,
                suffix,
            } => {
                let base = if tenant.is_empty() {
                    format!("{namespace}/")
                } else {
                    format!("{namespace}/{tenant}/")
                };
                if suffix.is_empty() {
                    format!("pair boundary {base}")
                } else {
                    format!(
                        "{base} + suffix 0x{}",
                        suffix.iter().map(|b| format!("{b:02x}")).collect::<String>()
                    )
                }
            }
            SplitPoint::Raw(bytes) => format!(
                "raw 0x{}",
                bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
            ),
        }
    }
}

pub(crate) enum Command {
    // read / observability
    ListNodes,
    // cluster-df: aggregate capacity summary (Ceph `ceph df` style)
    Df,
    // R1 rolling upgrade
    ClusterVersion,
    UpgradeVersion {
        to: Option<u32>,
    },
    ExtentHealth {
        node_filter: Vec<u64>,
        include_healthy: bool,
    },
    ListEcMarkers,
    RecoveryStats,
    AuditLog {
        op: u8,
        node_id: u64,
        since: i64,
        until: i64,
        limit: u32,
    },
    // node-lifecycle admin
    Fence {
        node_id: u64,
        reason: String,
        by: String,
        force: bool,
    },
    Maintenance {
        node_id: u64,
        reason: String,
        by: String,
        expire: u64,
    },
    Unfence {
        node_id: u64,
        by: String,
    },
    Remove {
        node_id: u64,
        by: String,
    },
    // read / observability (migrated from autumn-client)
    Info {
        part: Option<u64>,
        detail: bool,
        /// `--full`: the legacy complete dump (all nodes/streams/extents +
        /// per-PS discard probes). Default `info` is now the SCALABLE compact
        /// overview (per-partition rollup, no per-extent array) for big clusters.
        full: bool,
    },
    PolicyCandidates,
    /// Full dashboard overview JSON (df + nodes + partitions + amp + advisories).
    /// Always emits JSON; consumed by the standalone dashboard (examples/dashboard).
    Overview,
    /// Headless control of the leader-fenced auto-policy controller.
    /// `action` = status | activate | deactivate | upsert | delete. The upsert
    /// fields (`switches`/`interval`/`cooldown`/`max_actions`/`desc`) define a
    /// custom policy; the rest are ignored by the read/select/delete actions.
    AutoPolicy {
        action: String,
        name: String,
        arm: bool,
        switches: String,
        interval: Option<u64>,
        cooldown: Option<u64>,
        max_actions: Option<u32>,
        desc: Option<String>,
    },
    /// Query the async op-ledger. `op_id != 0` = `ops status <id>`; `op_id == 0`
    /// = `ops list` filtered by `active` / `kind` / `limit`.
    Ops {
        op_id: u64,
        active: bool,
        kind: String,
        limit: u32,
    },
    /// Durable terminal history from etcd, as opposed to `Ops`' live ledger.
    OpsHistory {
        kind: String,
        since_unix: i64,
        limit: u32,
    },
    // cluster / partition admin (migrated from autumn-client)
    Bootstrap {
        replication: String,
        presplit: String,
        log_ec: Option<(u32, u32)>,
        row_ec: Option<(u32, u32)>,
    },
    SetStreamEc {
        stream_id: u64,
        ec_data: u32,
        ec_parity: u32,
    },
    ForceEcConvert {
        extent_id: u64,
    },
    Split {
        part_id: u64,
        /// (D4): where to cut. See `SplitPoint`.
        point: SplitPoint,
    },
    /// presplit a `{tenant}/{namespace}/` keyspace by the
    /// namespace's natural dimension. Explicit op (NOT bootstrap — the tenant
    /// must exist first).
    Presplit {
        namespace: String,
        tenant: String,
        rule: PresplitRule,
        /// step 4: admin token used to RECORD the applied cut
        /// points as protected boundaries (`--admin-token` / `--admin-token-file`).
        /// Optional — without it the cuts still land, but merge won't refuse to
        /// undo them (a warning says so).
        admin_token: Option<String>,
        /// UX-fix (M5): required to NARROW an fs's declared stripe
        /// geometry (`--lanes` smaller than the current declaration). Without it,
        /// a redeclare that lowers the lane count is refused — a stray
        /// `presplit --namespace fs --lanes 2` would otherwise silently halve
        /// every future file's stripe width fs-wide.
        force: bool,
    },
    Merge {
        survivor_part_id: u64,
        victim_part_id: u64,
        /// step 4: merge THROUGH a declared presplit
        /// boundary. Refused without this — erasing an operator-declared
        /// boundary is silent (for fs lanes: later large files stripe narrower,
        /// no error anywhere).
        force: bool,
    },
    /// actively re-spread partitions across the PS fleet.
    Rebalance {
        max_moves: u32,
    },
    Compact {
        part_id: u64,
    },
    Gc {
        part_id: u64,
        ratio: Option<f64>,
        max_size: Option<u64>,
        stream_debt: Option<u64>,
        empty_only: bool,
    },
    ForceGc {
        part_id: u64,
        extent_ids: Vec<u64>,
    },
    // `register-node` merged into `format`. Variant kept so
    // the parser can route the legacy spelling to a migration stub
    // (in run()) instead of failing at parse with "unknown subcommand".
    RegisterNode,
    /// M1c: format is IDENTITY-ONLY (mints/reuses `node_uuid` +
    /// per-dir `disk_uuid`, registers with an EMPTY location). It no longer
    /// carries `listen`/`advertise`/`shard_ports` — the EN binary's own
    /// `--advertise` self-registers the live address + shard ports at every
    /// startup (mirrors Kafka `kafka-storage.sh format` / Ceph `ceph-volume
    /// prepare`: mkfs mints identity only, the daemon reports its own network
    /// location on every boot). See docs/en_dynamic_shard_design.md §2.2.
    Format { dirs: Vec<String> },
    // ── data-plane authz tooling ──────────────────────────────────
    /// Generate an Ed25519 signing key (LOCAL — no manager). Prints the keyfile
    /// line (`<kid> <hex-seed>`) to stdout; redirect to `--auth-signing-key-file`.
    GenSigningKey {
        kid: u32,
    },
    /// create/rotate a PRINCIPAL account (admin — needs
    /// `--admin-token`). Returns the principal's permanent credential (shown once,
    /// as `<name>\n<hex>` for direct save to a credential file).
    PrincipalCreate {
        principal: String,
        /// Granted key prefixes (`--grant fs/`, repeatable). Normalized to end `/`.
        grants: Vec<String>,
        admin_token: String,
    },
    /// Remove a principal account (admin). Its current token still works until exp.
    PrincipalDelete {
        principal: String,
        admin_token: String,
    },
    /// Mint a short-TTL capability token from a principal credential. Prints the
    /// token (hex) to stdout.
    MintToken {
        principal: String,
        credential: String,
    },
    /// D2: register a namespace (admin — needs `--admin-token`).
    /// `--with-tenant <T>` marks it protected (owner tenant = T). `--presplit`
    /// takes comma-separated hex split points (frozen for D8; stored, not yet
    /// acted upon).
    NamespaceCreate {
        name: String,
        owner_tenant: Option<String>,
        presplit: Vec<Vec<u8>>,
        admin_token: String,
    },
    /// D2: delete a namespace registry row (admin). Refuses a non-empty
    /// namespace unless `--force` (the emptiness check is done client-side here,
    /// scanning the prefix — the manager has no KV data-plane client).
    NamespaceDelete {
        name: String,
        force: bool,
        admin_token: String,
    },
    /// D2: list the full namespace registry (rich rows). Read-only.
    NamespaceList,
    /// list every principal + its grants. Read-only, and
    /// never prints credential material.
    PrincipalList,
}

pub(crate) fn parse() -> Args {
    // `--wait` / `--timeout SECS` are position-INDEPENDENT (they apply to the
    // async op triggers regardless of whether they land before or after the
    // subcommand, matching operator muscle memory `autumn-op compact 7 --wait`).
    // Pre-filter them out so the per-subcommand parsers never see them.
    let mut wait = false;
    let mut wait_timeout = 600u64;
    let raw: Vec<String> = {
        let orig: Vec<String> = std::env::args().collect();
        let mut out = Vec::with_capacity(orig.len());
        let mut j = 0usize;
        while j < orig.len() {
            match orig[j].as_str() {
                "--wait" => {
                    wait = true;
                    j += 1;
                }
                "--timeout" => {
                    j += 1;
                    wait_timeout = orig
                        .get(j)
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or_else(|| usage());
                    j += 1;
                }
                _ => {
                    out.push(orig[j].clone());
                    j += 1;
                }
            }
        }
        out
    };
    let mut manager = "127.0.0.1:9001".to_string();
    let mut json = false;
    let mut transport = TransportKind::Tcp;
    let mut admin_token: Option<String> = None;
    let mut i = 1usize;
    while i < raw.len() {
        match raw[i].as_str() {
            "--manager" => {
                i += 1;
                manager = raw.get(i).cloned().unwrap_or_else(|| usage());
                i += 1;
            }
            "--json" => {
                json = true;
                i += 1;
            }
            // Must match the manager's transport — a TCP autumn-op cannot
            // talk to a UCX-only manager (the connect/RPC just hangs).
            "--transport" => {
                i += 1;
                let raw_t = raw.get(i).cloned().unwrap_or_else(|| usage());
                transport = autumn_transport::parse_transport_flag(&raw_t).unwrap_or_else(|bad| {
                    eprintln!("--transport must be `tcp` or `ucx`, got {bad:?}");
                    usage()
                });
                i += 1;
            }
            // global admin token, gating cluster-mutating ops.
            // (The per-command `--admin-token` on tenant/namespace/principal ops
            // is a separate, older struct-field path; this global flag drives the
            // payload-prefix path for fence/merge/create-stream/….)
            "--admin-token" => {
                i += 1;
                admin_token = Some(raw.get(i).cloned().unwrap_or_else(|| usage()));
                i += 1;
            }
            "--admin-token-file" => {
                i += 1;
                admin_token = Some(read_secret_file(&raw.get(i).cloned().unwrap_or_else(|| usage())));
                i += 1;
            }
            "--help" | "-h" => usage(),
            _ => break,
        }
    }
    if i >= raw.len() {
        usage();
    }
    let sub = raw[i].as_str();
    i += 1;
    let cmd = match sub {
        // read
        "list-nodes" => Command::ListNodes,
        "df" => Command::Df,
        // R1 rolling upgrade
        "cluster-version" => Command::ClusterVersion,
        "upgrade-version" => {
            let mut to: Option<u32> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--to" => {
                        i += 1;
                        if i >= raw.len() {
                            usage();
                        }
                        to = Some(val(&raw, i).parse().unwrap_or_else(|_| usage()));
                        i += 1;
                    }
                    _ => break,
                }
            }
            Command::UpgradeVersion { to }
        }
        "extent-health" => {
            let mut node_filter: Vec<u64> = Vec::new();
            let mut include_healthy = false;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--node" => {
                        i += 1;
                        node_filter.push(val(&raw, i).parse().unwrap_or_else(|_| usage()));
                        i += 1;
                    }
                    "--all" => {
                        include_healthy = true;
                        i += 1;
                    }
                    _ => break,
                }
            }
            Command::ExtentHealth {
                node_filter,
                include_healthy,
            }
        }
        "list-ec-markers" => Command::ListEcMarkers,
        "recovery-stats" => Command::RecoveryStats,
        "audit-log" => {
            let mut op = 0u8;
            let mut node_id = 0u64;
            let mut since = 0i64;
            let mut until = 0i64;
            let mut limit = 100u32;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--op" => {
                        i += 1;
                        op = val(&raw, i).parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    "--node" => {
                        i += 1;
                        node_id = val(&raw, i).parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    "--since" => {
                        i += 1;
                        since = val(&raw, i).parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    "--until" => {
                        i += 1;
                        until = val(&raw, i).parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    "--limit" => {
                        i += 1;
                        limit = val(&raw, i).parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    _ => break,
                }
            }
            Command::AuditLog {
                op,
                node_id,
                since,
                until,
                limit,
            }
        }
        // admin
        "fence-node" => {
            let node_id: u64 = raw
                .get(i)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| usage());
            i += 1;
            let (reason, by, force) = parse_admin_flags(&raw, &mut i);
            Command::Fence {
                node_id,
                reason,
                by,
                force,
            }
        }
        "maintenance" => {
            let node_id: u64 = raw
                .get(i)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| usage());
            i += 1;
            let mut reason = String::new();
            let mut by = String::new();
            let mut expire: u64 = 0;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--reason" => {
                        i += 1;
                        reason = val(&raw, i).to_owned();
                        i += 1;
                    }
                    "--by" => {
                        i += 1;
                        by = val(&raw, i).to_owned();
                        i += 1;
                    }
                    "--expire" => {
                        i += 1;
                        expire = val(&raw, i).parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    _ => break,
                }
            }
            Command::Maintenance {
                node_id,
                reason,
                by,
                expire,
            }
        }
        "unfence" => {
            let node_id: u64 = raw
                .get(i)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| usage());
            i += 1;
            let (_reason, by, _force) = parse_admin_flags(&raw, &mut i);
            Command::Unfence { node_id, by }
        }
        "remove" => {
            let node_id: u64 = raw
                .get(i)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| usage());
            i += 1;
            let (_reason, by, _force) = parse_admin_flags(&raw, &mut i);
            Command::Remove { node_id, by }
        }
        // ── authz tooling ────────────────────────────────────────────
        "gen-signing-key" => {
            let mut kid: u32 = 1;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--kid" => {
                        i += 1;
                        kid = val(&raw, i).parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    _ => break,
                }
            }
            Command::GenSigningKey { kid }
        }
        "principal-create" => {
            let mut principal = String::new();
            let mut grants: Vec<String> = Vec::new();
            let mut admin_token = String::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--principal" => {
                        i += 1;
                        principal = val(&raw, i).to_owned();
                        i += 1;
                    }
                    "--grant" => {
                        i += 1;
                        grants.push(val(&raw, i).to_owned());
                        i += 1;
                    }
                    "--admin-token" => {
                        i += 1;
                        admin_token = val(&raw, i).to_owned();
                        i += 1;
                    }
                    // Read the admin token from a FILE (avoids leaking it via
                    // argv / ps / /proc/<pid>/cmdline). Preferred over --admin-token.
                    "--admin-token-file" => {
                        i += 1;
                        admin_token = read_secret_file(val(&raw, i));
                        i += 1;
                    }
                    _ => break,
                }
            }
            // Validate at parse time (fail-fast, coco P3): the RPC needs a
            // non-empty admin token; catch a missing --admin-token[-file] here.
            if principal.is_empty() || grants.is_empty() || admin_token.is_empty() {
                usage();
            }
            Command::PrincipalCreate {
                principal,
                grants,
                admin_token,
            }
        }
        "principal-delete" => {
            let mut principal = String::new();
            let mut admin_token = String::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--principal" => {
                        i += 1;
                        principal = val(&raw, i).to_owned();
                        i += 1;
                    }
                    "--admin-token" => {
                        i += 1;
                        admin_token = val(&raw, i).to_owned();
                        i += 1;
                    }
                    "--admin-token-file" => {
                        i += 1;
                        admin_token = read_secret_file(val(&raw, i));
                        i += 1;
                    }
                    _ => break,
                }
            }
            if principal.is_empty() || admin_token.is_empty() {
                usage();
            }
            Command::PrincipalDelete {
                principal,
                admin_token,
            }
        }
        "mint-token" => {
            let mut principal = String::new();
            let mut credential = String::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--principal" | "--tenant" => {
                        i += 1;
                        principal = val(&raw, i).to_owned();
                        i += 1;
                    }
                    "--credential" => {
                        i += 1;
                        credential = val(&raw, i).to_owned();
                        i += 1;
                    }
                    // Read the (long-lived) credential from a FILE instead of argv.
                    "--credential-file" => {
                        i += 1;
                        credential = read_secret_file(val(&raw, i));
                        i += 1;
                    }
                    _ => break,
                }
            }
            if principal.is_empty() || credential.is_empty() {
                usage();
            }
            Command::MintToken { principal, credential }
        }
        // D2: namespace registry admin
        "namespace-create" => {
            let mut name = String::new();
            let mut owner_tenant: Option<String> = None;
            let mut presplit: Vec<Vec<u8>> = Vec::new();
            let mut admin_token = String::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--name" => {
                        i += 1;
                        name = val(&raw, i).to_owned();
                        i += 1;
                    }
                    // Convenience: mark the namespace PROTECTED with owner = T.
                    // (Full principal-create wrapping is deferred to a later
                    // sub-delivery — this only sets the owner marker.)
                    // `--tenant` is the canonical spelling (matches split /
                    // presplit / mint-token); `--with-tenant` kept as an alias.
                    "--tenant" | "--with-tenant" => {
                        i += 1;
                        owner_tenant = Some(val(&raw, i).to_owned());
                        i += 1;
                    }
                    // Comma-separated HEX split points (frozen for D8).
                    "--presplit" => {
                        i += 1;
                        presplit = parse_hex_split_points(val(&raw, i));
                        i += 1;
                    }
                    "--admin-token" => {
                        i += 1;
                        admin_token = val(&raw, i).to_owned();
                        i += 1;
                    }
                    "--admin-token-file" => {
                        i += 1;
                        admin_token = read_secret_file(val(&raw, i));
                        i += 1;
                    }
                    _ => break,
                }
            }
            if name.is_empty() || admin_token.is_empty() {
                usage();
            }
            Command::NamespaceCreate {
                name,
                owner_tenant,
                presplit,
                admin_token,
            }
        }
        "namespace-delete" => {
            let mut name = String::new();
            let mut force = false;
            let mut admin_token = String::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--name" => {
                        i += 1;
                        name = val(&raw, i).to_owned();
                        i += 1;
                    }
                    "--force" => {
                        force = true;
                        i += 1;
                    }
                    "--admin-token" => {
                        i += 1;
                        admin_token = val(&raw, i).to_owned();
                        i += 1;
                    }
                    "--admin-token-file" => {
                        i += 1;
                        admin_token = read_secret_file(val(&raw, i));
                        i += 1;
                    }
                    _ => break,
                }
            }
            if name.is_empty() || admin_token.is_empty() {
                usage();
            }
            Command::NamespaceDelete {
                name,
                force,
                admin_token,
            }
        }
        "namespace-list" => Command::NamespaceList,
        "principal-list" => Command::PrincipalList,
        // read
        "info" => {
            let mut part: Option<u64> = None;
            let mut detail = false;
            let mut full = false;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--part" => {
                        i += 1;
                        if i >= raw.len() {
                            eprintln!("--part requires a number");
                            usage();
                        }
                        part = Some(val(&raw, i).parse().unwrap_or_else(|_| {
                            eprintln!("--part requires a number");
                            usage()
                        }));
                        i += 1;
                    }
                    "--detail" => {
                        detail = true;
                        i += 1;
                    }
                    "--full" => {
                        full = true;
                        i += 1;
                    }
                    other => {
                        eprintln!("unknown info flag: {other}");
                        usage();
                    }
                }
            }
            if detail && part.is_none() {
                eprintln!("--detail requires --part <PID>");
                usage();
            }
            Command::Info { part, detail, full }
        }
        "policy-candidates" | "policy_candidates" | "policy" => Command::PolicyCandidates,
        "overview" => Command::Overview,
        // M2: auto-policy <status|activate <name> [--arm]|deactivate>
        "auto-policy" | "auto_policy" => {
            let action = if i < raw.len() {
                let a = raw[i].clone();
                i += 1;
                a
            } else {
                "status".to_string()
            };
            let mut name = String::new();
            let mut arm = false;
            let mut switches = String::new();
            let mut interval: Option<u64> = None;
            let mut cooldown: Option<u64> = None;
            let mut max_actions: Option<u32> = None;
            let mut desc: Option<String> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--arm" => arm = true,
                    "--switches" => {
                        switches = raw.get(i + 1).cloned().unwrap_or_default();
                        i += 1;
                    }
                    "--interval" => {
                        interval = raw.get(i + 1).and_then(|s| s.parse().ok());
                        i += 1;
                    }
                    "--cooldown" => {
                        cooldown = raw.get(i + 1).and_then(|s| s.parse().ok());
                        i += 1;
                    }
                    "--max" => {
                        max_actions = raw.get(i + 1).and_then(|s| s.parse().ok());
                        i += 1;
                    }
                    "--desc" => {
                        desc = raw.get(i + 1).cloned();
                        i += 1;
                    }
                    other if !other.starts_with('-') && name.is_empty() => {
                        name = other.to_string()
                    }
                    _ => {}
                }
                i += 1;
            }
            Command::AutoPolicy {
                action,
                name,
                arm,
                switches,
                interval,
                cooldown,
                max_actions,
                desc,
            }
        }
        // ops status <id> | ops list [--active] [--kind K] [--limit N]
        //   | ops history [--kind K] [--since UNIX] [--limit N]
        "ops" => {
            let action = raw.get(i).cloned().unwrap_or_else(|| "list".to_string());
            i += 1;
            let mut op_id = 0u64;
            let mut active = false;
            let mut kind = String::new();
            let mut limit = 0u32;
            let mut since_unix = 0i64;
            if action == "status" {
                op_id = raw.get(i).and_then(|s| s.parse().ok()).unwrap_or_else(|| {
                    eprintln!("ops status requires an op id");
                    usage()
                });
                i += 1;
            } else if action != "list" && action != "history" {
                // `ops <id>` shorthand for `ops status <id>`.
                match action.parse::<u64>() {
                    Ok(id) => op_id = id,
                    Err(_) => {
                        eprintln!(
                            "ops: expected `status <id>`, `list`, or `history`"
                        );
                        usage()
                    }
                }
            }
            while i < raw.len() {
                match raw[i].as_str() {
                    "--active" => {
                        active = true;
                        i += 1;
                    }
                    "--kind" => {
                        i += 1;
                        kind = raw.get(i).cloned().unwrap_or_default();
                        i += 1;
                    }
                    "--since" => {
                        i += 1;
                        since_unix = val(&raw, i).parse().unwrap_or(0);
                        i += 1;
                    }
                    "--limit" => {
                        i += 1;
                        limit = raw.get(i).and_then(|s| s.parse().ok()).unwrap_or(0);
                        i += 1;
                    }
                    _ => break,
                }
            }
            if action == "history" {
                Command::OpsHistory {
                    kind,
                    since_unix,
                    limit,
                }
            } else {
                Command::Ops {
                    op_id,
                    active,
                    kind,
                    limit,
                }
            }
        }
        // admin
        "bootstrap" => {
            let mut replication = String::from("3+0");
            // --presplit is RETIRED (see the arm below); it never changes.
            let presplit = String::from("1:normal");
            let mut log_ec: Option<(u32, u32)> = None;
            let mut row_ec: Option<(u32, u32)> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--replication" => {
                        i += 1;
                        replication = val(&raw, i).to_owned();
                        i += 1;
                    }
                    "--presplit" => {
                        // RETIRED. It cut the RAW keyspace at
                        // uniform hex points, which is namespace-blind — under
                        // Option 3 every key carries a `{ns}/` prefix, so those
                        // points landed nowhere near real keys and the presplit
                        // silently did nothing (BUG-BENCH-NS-UNREGISTERED: the
                        // benches' `--partitions N` measured one partition for
                        // every N). A cluster-wide single mode also has no correct
                        // setting once more than one application shares the
                        // cluster. Migration stub, house idiom: fail BEFORE doing
                        // any work rather than accepting a flag that lies.
                        eprintln!(
                            "autumn-op bootstrap --presplit is RETIRED.\n\
                             It cut the RAW keyspace, which is namespace-blind — under the\n\
                             `{{ns}}/` key layout those points miss every real key, so the\n\
                             presplit silently did nothing.\n\
                             Presplit per namespace AFTER bootstrap instead:\n\
                             \x20 autumn-op presplit --namespace fs    --lanes 24 --parts 6 --admin-token-file F\n\
                             \x20 autumn-op presplit --namespace bench --count 8            --admin-token-file F\n\
                             \x20 autumn-op presplit --namespace kvc   --count 8 --hash-prefix '<model>/vllm/v1/'"
                        );
                        std::process::exit(2);
                    }
                    "--log-ec" => {
                        i += 1;
                        log_ec = Some(parse_ec_flag(val(&raw, i)).unwrap_or_else(|e| {
                            eprintln!("--log-ec: {e}");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    "--row-ec" => {
                        i += 1;
                        row_ec = Some(parse_ec_flag(val(&raw, i)).unwrap_or_else(|e| {
                            eprintln!("--row-ec: {e}");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    _ => break,
                }
            }
            Command::Bootstrap {
                replication,
                presplit,
                log_ec,
                row_ec,
            }
        }
        "set-stream-ec" => {
            let mut stream_id: Option<u64> = None;
            let mut ec: Option<(u32, u32)> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--stream" => {
                        i += 1;
                        stream_id = Some(val(&raw, i).parse().unwrap_or_else(|_| {
                            eprintln!("--stream requires a numeric stream ID");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    "--ec" => {
                        i += 1;
                        ec = Some(parse_ec_flag(val(&raw, i)).unwrap_or_else(|e| {
                            eprintln!("--ec: {e}");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    _ => break,
                }
            }
            let stream_id = stream_id.unwrap_or_else(|| {
                eprintln!("set-stream-ec requires --stream <ID>");
                std::process::exit(1);
            });
            let (ec_data, ec_parity) = ec.unwrap_or_else(|| {
                eprintln!("set-stream-ec requires --ec K+M");
                std::process::exit(1);
            });
            Command::SetStreamEc {
                stream_id,
                ec_data,
                ec_parity,
            }
        }
        "force-ec-convert" => {
            let mut extent_id: Option<u64> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--extent" => {
                        i += 1;
                        if i >= raw.len() {
                            eprintln!("--extent requires a number");
                            usage();
                        }
                        extent_id = Some(val(&raw, i).parse().unwrap_or_else(|_| {
                            eprintln!("--extent requires a number");
                            usage()
                        }));
                        i += 1;
                    }
                    _ => break,
                }
            }
            let extent_id = extent_id.unwrap_or_else(|| {
                eprintln!("force-ec-convert requires --extent <EXTID>");
                std::process::exit(1);
            });
            Command::ForceEcConvert { extent_id }
        }
        "split" => {
            if i >= raw.len() {
                eprintln!("split requires <PARTID>");
                std::process::exit(1);
            }
            let part_id: u64 = num_arg(&raw, i, "PARTID");
            i += 1;
            // (D4). CLI user面 speaks namespace/tenant, never
            // raw prefix bytes (§3.4 CLI refinement / 细化三). Forms:
            //   split <PART>                                    → median (legacy)
            //   split <PART> --namespace NS --tenant T [--at S | --at-hex HEX]
            //        → cut at "NS/T/" ++ suffix; empty/omitted suffix = the
            //          pair boundary "NS/T/" itself
            //   split <PART> --at-raw-hex HEX                   → admin escape
            //        hatch: raw whole key, no ns/tenant assembly
            let mut namespace: Option<String> = None;
            let mut tenant: Option<String> = None;
            let mut suffix: Option<Vec<u8>> = None; // from --at / --at-hex
            let mut raw_key: Option<Vec<u8>> = None; // from --at-raw-hex
            while i < raw.len() {
                match raw[i].as_str() {
                    "--namespace" => {
                        i += 1;
                        namespace = Some(val(&raw, i).to_owned());
                        i += 1;
                    }
                    "--tenant" => {
                        i += 1;
                        tenant = Some(val(&raw, i).to_owned());
                        i += 1;
                    }
                    "--at" => {
                        if suffix.is_some() {
                            eprintln!("split: pass only one of --at / --at-hex");
                            std::process::exit(1);
                        }
                        i += 1;
                        suffix = Some(val(&raw, i).as_bytes().to_vec());
                        i += 1;
                    }
                    "--at-hex" => {
                        if suffix.is_some() {
                            eprintln!("split: pass only one of --at / --at-hex");
                            std::process::exit(1);
                        }
                        i += 1;
                        suffix = Some(parse_hex_key(val(&raw, i)));
                        i += 1;
                    }
                    "--at-raw-hex" => {
                        if raw_key.is_some() {
                            eprintln!("split: pass --at-raw-hex only once");
                            std::process::exit(1);
                        }
                        i += 1;
                        raw_key = Some(parse_hex_key(val(&raw, i)));
                        i += 1;
                    }
                    other => {
                        eprintln!("split: unexpected argument {other:?}");
                        std::process::exit(1);
                    }
                }
            }
            let point = build_split_point(namespace, tenant, suffix, raw_key);
            Command::Split { part_id, point }
        }
        "presplit" => {
            // presplit <NS>/<TENANT> by the ns's dimension.
            //   presplit --namespace fs  --tenant T (--fs-inos i,j,… | --count N)
            //   presplit --namespace kvc --tenant T --count N --hash-prefix '<model>/vllm/v1/'
            //   presplit --namespace mem --tenant T --agents a,b,…
            let mut namespace: Option<String> = None;
            let mut tenant: Option<String> = None;
            let mut count: Option<usize> = None;
            let mut fs_inos: Option<Vec<u64>> = None;
            let mut lanes: Option<usize> = None;
            let mut parts: Option<usize> = None;
            let mut hash_prefix: Option<String> = None;
            let mut agents: Option<Vec<String>> = None;
            let mut presplit_admin_token: Option<String> = None;
            let mut presplit_force = false;
            let num = |v: &str, what: &str| -> u64 {
                v.trim().parse().unwrap_or_else(|_| {
                    eprintln!("presplit: {what} must be a number, got {v:?}");
                    std::process::exit(1);
                })
            };
            while i < raw.len() {
                match raw[i].as_str() {
                    "--namespace" => { i += 1; namespace = Some(val(&raw, i).to_owned()); i += 1; }
                    "--tenant" => { i += 1; tenant = Some(val(&raw, i).to_owned()); i += 1; }
                    "--count" => { i += 1; count = Some(num(val(&raw, i), "--count") as usize); i += 1; }
                    "--fs-inos" => {
                        i += 1;
                        fs_inos = Some(val(&raw, i).split(',').map(|s| num(s, "--fs-inos entry")).collect());
                        i += 1;
                    }
                    "--lanes" => { i += 1; lanes = Some(num(val(&raw, i), "--lanes") as usize); i += 1; }
                    "--parts" => { i += 1; parts = Some(num(val(&raw, i), "--parts") as usize); i += 1; }
                    "--admin-token" => { i += 1; presplit_admin_token = Some(val(&raw, i).to_owned()); i += 1; }
                    "--admin-token-file" => { i += 1; presplit_admin_token = Some(read_secret_file(val(&raw, i))); i += 1; }
                    "--hash-prefix" => { i += 1; hash_prefix = Some(val(&raw, i).to_owned()); i += 1; }
                    "--force" => { i += 1; presplit_force = true; }
                    "--agents" => {
                        i += 1;
                        agents = Some(
                            val(&raw, i).split(',').map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()).collect(),
                        );
                        i += 1;
                    }
                    other => { eprintln!("presplit: unknown flag {other}"); std::process::exit(1); }
                }
            }
            let namespace = namespace.unwrap_or_else(|| {
                eprintln!("presplit requires --namespace <fs|kvc|mem>"); std::process::exit(1);
            });
            // `--tenant` is an OPTIONAL in-namespace
            // sub-segment (mem/kvc). fs has no tenant → default empty → cut at
            // `{namespace}/`. When given it is concatenated raw into the
            // `{namespace}/{tenant}/` cut key, so it MUST be a single lowercase
            // path segment — same guard `split` uses.
            let tenant = tenant.unwrap_or_default();
            if !tenant.is_empty() {
                reject_bad_segment("presplit", "tenant", &tenant);
            }
            let rule = match namespace.as_str() {
                "fs" => {
                    // `--lanes N` presplits fs into N LANE partitions
                    // (cut at `[0x03][1..N]`) so a striped large file's extents
                    // spread across them. Distinct from `--fs-inos`/`--count`
                    // (which cut by INODE, one file per partition).
                    if let Some(n) = lanes {
                        if !(2..=255).contains(&n) {
                            eprintln!("presplit --namespace fs --lanes N: N must be in 2..=255 (got {n})");
                            std::process::exit(1);
                        }
                        // `--parts` omitted = cut every lane boundary (one
                        // partition per lane), the pre-`--parts` behaviour.
                        let p = parts.unwrap_or(n);
                        if !(1..=n).contains(&p) {
                            eprintln!("presplit --parts P: P must be in 1..={n} (got {p})");
                            std::process::exit(1);
                        }
                        PresplitRule::FsLanes { lanes: n as u8, parts: p as u8 }
                    } else if let Some(inos) = fs_inos {
                        PresplitRule::Fs { inos }
                    } else if let Some(n) = count {
                        PresplitRule::Fs { inos: (1..n as u64).collect() } // ino 1..N-1 → N parts
                    } else {
                        eprintln!("presplit --namespace fs requires --lanes <N> | --fs-inos <i,j,…> | --count <N>");
                        std::process::exit(1);
                    }
                }
                "kvc" => {
                    let n = count.unwrap_or_else(|| {
                        eprintln!("presplit --namespace kvc requires --count <N>"); std::process::exit(1);
                    });
                    // No default: the content-hash is NOT directly under a fixed
                    // segment. The vLLM connector stores
                    //   kvc/{tenant}/{model}/vllm/v1/{hash}/{layer}
                    // (full_key -> {model}/{pool}/{fmt}/{hash}/{layer}, POOL=vllm,
                    // FMT=v1), so the hash sits under a per-MODEL prefix. A hardcoded
                    // "vllm/" would cut nowhere near real keys and silently defeat the
                    // presplit. Require the operator to pass the exact relative prefix
                    // down to (but excluding) the hash hex — inspect a live key first.
                    let hp = hash_prefix.unwrap_or_else(|| {
                        eprintln!(
                            "presplit --namespace kvc requires --hash-prefix <rel-prefix-to-hash>\n  \
                             the RELATIVE prefix from the namespace root down to just before the\n  \
                             content-hash hex. vLLM stores kvc/<tenant>/<model>/vllm/v1/<hash>/<layer>,\n  \
                             so pass e.g. --hash-prefix '<model-fingerprint>/vllm/v1/'. Find the exact\n  \
                             <model-fingerprint> with: autumn-client ls --prefix '<tenant>/kvc/'"
                        );
                        std::process::exit(1);
                    });
                    PresplitRule::Kvc { hash_prefix: hp.into_bytes(), count: n }
                }
                "mem" => {
                    let agents = agents.unwrap_or_else(|| {
                        eprintln!("presplit --namespace mem requires --agents <a,b,…>"); std::process::exit(1);
                    });
                    if agents.is_empty() {
                        eprintln!("presplit --namespace mem: --agents must be non-empty"); std::process::exit(1);
                    }
                    PresplitRule::Mem { agents }
                }
                _other => {
                    // Any registered namespace can be presplit by uniform hex.
                    let n = count.unwrap_or_else(|| {
                        eprintln!("presplit --namespace {_other} requires --count <N> (uniform hex split)");
                        std::process::exit(1);
                    });
                    PresplitRule::Hex { count: n }
                }
            };
            Command::Presplit { namespace, tenant, rule, admin_token: presplit_admin_token, force: presplit_force }
        }
        "merge" => {
            if raw.get(i).map(|s| s == "--help" || s == "-h").unwrap_or(false) {
                usage();
            }
            if i + 1 >= raw.len() {
                eprintln!("merge requires <SURVIVOR_PART_ID> <VICTIM_PART_ID> [--force]");
                std::process::exit(1);
            }
            let survivor_part_id = num_arg(&raw, i, "SURVIVOR_PART_ID");
            let victim_part_id = num_arg(&raw, i + 1, "VICTIM_PART_ID");
            // Reject any trailing token other than --force (was: scan the
            // whole argv for --force and ignore everything else).
            let mut force = false;
            for tok in &raw[i + 2..] {
                if tok == "--force" {
                    force = true;
                } else {
                    eprintln!("merge: unexpected argument {tok:?} (only --force is allowed after the two partition ids)");
                    usage();
                }
            }
            Command::Merge { survivor_part_id, victim_part_id, force }
        }
        "rebalance" => {
            // Optional [MAX_MOVES]; absent / 0 = move as many as needed to balance.
            // Reject trailing tokens — this is a cluster-level mutation, so a
            // typo'd arg must fail loudly rather than silently move partitions.
            if i + 1 < raw.len() {
                eprintln!("rebalance takes at most one arg: [MAX_MOVES]");
                std::process::exit(1);
            }
            let max_moves = if i < raw.len() {
                num_arg(&raw, i, "MAX_MOVES")
            } else {
                0
            };
            Command::Rebalance { max_moves }
        }
        "compact" => {
            if raw.get(i).map(|s| s == "--help" || s == "-h").unwrap_or(false) {
                usage();
            }
            if i >= raw.len() {
                eprintln!("compact requires <PARTID>");
                std::process::exit(1);
            }
            let part_id = num_arg(&raw, i, "PARTID");
            // Reject trailing tokens (compact takes exactly one arg).
            if let Some(extra) = raw.get(i + 1) {
                eprintln!("compact: unexpected argument {extra:?} (takes exactly one PARTID)");
                usage();
            }
            Command::Compact { part_id }
        }
        "gc" => {
            let mut ratio: Option<f64> = None;
            let mut max_size: Option<u64> = None;
            let mut stream_debt: Option<u64> = None;
            let mut empty_only = false;
            let mut part_id: Option<u64> = None;
            // One pass so flags and the PARTID appear in ANY order — the old
            // loop `break`d on the first non-flag, so `gc PART --ratio 0.4`
            // (exactly the documented form) silently dropped every flag after the
            // positional. Unknown tokens are rejected, not ignored (a mutation).
            while i < raw.len() {
                match raw[i].as_str() {
                    "--help" | "-h" => usage(),
                    "--ratio" => {
                        i += 1;
                        ratio = Some(val(&raw, i).parse().unwrap_or_else(|_| {
                            eprintln!("--ratio expects a float 0.0..=1.0");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    "--max-size" => {
                        i += 1;
                        max_size = Some(parse_byte_size(val(&raw, i)).unwrap_or_else(|e| {
                            eprintln!("--max-size: {e}");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    "--stream-debt" => {
                        i += 1;
                        stream_debt = Some(parse_byte_size(val(&raw, i)).unwrap_or_else(|e| {
                            eprintln!("--stream-debt: {e}");
                            std::process::exit(1);
                        }));
                        i += 1;
                    }
                    "--empty-only" => {
                        empty_only = true;
                        i += 1;
                    }
                    tok if tok.starts_with('-') => {
                        eprintln!("gc: unknown flag {tok:?}");
                        usage();
                    }
                    _ if part_id.is_none() => {
                        part_id = Some(num_arg(&raw, i, "PARTID"));
                        i += 1;
                    }
                    tok => {
                        eprintln!("gc: unexpected argument {tok:?} (PARTID already given)");
                        usage();
                    }
                }
            }
            let Some(part_id) = part_id else {
                eprintln!("gc requires <PARTID>");
                std::process::exit(1);
            };
            Command::Gc { part_id, ratio, max_size, stream_debt, empty_only }
        }
        "forcegc" => {
            if i >= raw.len() {
                eprintln!("forcegc requires <PARTID> <EXTID>...");
                std::process::exit(1);
            }
            let part_id: u64 = num_arg(&raw, i, "PARTID");
            i += 1;
            let mut extent_ids = Vec::new();
            while i < raw.len() {
                extent_ids.push(num_arg::<u64>(&raw, i, "EXTID"));
                i += 1;
            }
            if extent_ids.is_empty() {
                eprintln!("forcegc requires at least one <EXTID>");
                std::process::exit(1);
            }
            Command::ForceGc {
                part_id,
                extent_ids,
            }
        }
        "register-node" => {
            // route to the migration stub. Consume any remaining
            // arguments so the parser doesn't misinterpret them.
            while i < raw.len() {
                i += 1;
            }
            Command::RegisterNode
        }
        "format" => {
            let mut dirs = Vec::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    // M1c: format is identity-only now — these
                    // location flags moved to the EN binary itself (its own
                    // `--advertise`, self-registered at every startup). Hard
                    // error (not a silent ignore) so a not-yet-updated caller
                    // fails loudly instead of format silently keying off a
                    // register that never fires. Same-commit migration
                    // pattern as `--disk-id` / `--shards`.
                    "--listen" | "--advertise" | "--shard-ports" => {
                        eprintln!(
                            "error: format's `{}` was removed in M1c — \
                             format now registers IDENTITY ONLY (node_uuid + disk_uuids), \
                             no location. Pass `--advertise HOST:PORT` to \
                             `autumn-extent-node` itself instead; it self-registers its \
                             live address + shard ports at every startup.",
                            raw[i]
                        );
                        std::process::exit(2);
                    }
                    _ => dirs.push(raw[i].clone()),
                }
                i += 1;
            }
            if dirs.is_empty() {
                eprintln!("format requires <DIR>...");
                std::process::exit(1);
            }
            Command::Format { dirs }
        }
        "--help" | "-h" | "help" => usage(),
        // Name the unknown subcommand before dumping the usage wall.
        other => {
            eprintln!("autumn-op: unknown command {other:?}");
            usage()
        }
    };
    Args {
        manager,
        json,
        transport,
        admin_token,
        wait,
        wait_timeout,
        cmd,
    }
}

fn parse_admin_flags(raw: &[String], i: &mut usize) -> (String, String, bool) {
    let mut reason = String::new();
    let mut by = String::new();
    let mut force = false;
    while *i < raw.len() {
        match raw[*i].as_str() {
            "--reason" => {
                *i += 1;
                reason = val(raw, *i).to_owned();
                *i += 1;
            }
            "--by" => {
                *i += 1;
                by = val(raw, *i).to_owned();
                *i += 1;
            }
            "--force" => {
                force = true;
                *i += 1;
            }
            _ => break,
        }
    }
    (reason, by, force)
}

/// (D4) / coco P2: a namespace/tenant CLI segment must be a
/// SINGLE path segment so `{ns}/{tenant}/ ++ suffix` is 1:1 with what the
/// operator typed. The namespace convention pins the charset to `[a-z0-9._-]+`
/// (non-empty, no `/`). Pure so it is unit-testable; `reject_bad_segment` is
/// the fail-loud wrapper.
pub(crate) fn valid_segment(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-'))
}

fn reject_bad_segment(cmd: &str, kind: &str, s: &str) {
    if !valid_segment(s) {
        eprintln!(
            "{cmd}: --{kind} {s:?} is not a valid segment — must be non-empty and \
             match [a-z0-9._-]+ (no '/')."
        );
        std::process::exit(1);
    }
}

/// (D4): validate the split-point flag combination and lift it
/// into a `SplitPoint`. Exits with a clear message on a contradictory combo
/// (an operator naming a cut point must fail loudly, not split at garbage).
///
/// Rules:
/// - `--at-raw-hex` is the admin escape hatch and is mutually exclusive with
///   every ns/tenant/suffix flag.
/// - `--namespace` and `--tenant` are both-or-neither.
/// - `--at` / `--at-hex` (the suffix) require `--namespace` + `--tenant`
///   (a suffix is relative to a pair; a bare suffix is meaningless — use
///   `--at-raw-hex` for a whole raw key).
/// - Nothing → `Median`.
pub(crate) fn build_split_point(
    namespace: Option<String>,
    tenant: Option<String>,
    suffix: Option<Vec<u8>>,
    raw_key: Option<Vec<u8>>,
) -> SplitPoint {
    if let Some(bytes) = raw_key {
        if namespace.is_some() || tenant.is_some() || suffix.is_some() {
            eprintln!(
                "split: --at-raw-hex is an escape hatch and cannot be combined \
                 with --namespace/--tenant/--at/--at-hex"
            );
            std::process::exit(1);
        }
        return SplitPoint::Raw(bytes);
    }
    // `--namespace` alone is enough (fs has no tenant);
    // `--tenant` is an OPTIONAL in-namespace sub-segment (mem/kvc).
    match namespace {
        Some(namespace) => {
            // Each segment MUST be a single path segment (`[a-z0-9._-]+`, no `/`),
            // else the (ns, tenant, suffix) triple is not 1:1 with the assembled
            // raw key. A key that needs other bytes uses `--at-raw-hex`.
            reject_bad_segment("split", "namespace", &namespace);
            let tenant = tenant.unwrap_or_default();
            if !tenant.is_empty() {
                reject_bad_segment("split", "tenant", &tenant);
            }
            SplitPoint::Namespaced {
                namespace,
                tenant,
                // Omitted suffix = empty = cut exactly at the namespace/pair boundary.
                suffix: suffix.unwrap_or_default(),
            }
        }
        None => {
            if suffix.is_some() || tenant.is_some() {
                eprintln!(
                    "split: --at/--at-hex/--tenant require --namespace <ns> \
                     (a suffix/tenant is relative to a namespace). For a whole raw \
                     key use --at-raw-hex."
                );
                std::process::exit(1);
            }
            SplitPoint::Median
        }
    }
}

/// (D4): decode a `--at-hex <HEX>` value into raw key bytes.
/// Even-length ASCII hex; exits with a clear message on malformed input (an
/// operator naming a split point wants to fail loudly, not split at garbage).
pub(crate) fn parse_hex_key(s: &str) -> Vec<u8> {
    if !s.is_ascii() || !s.len().is_multiple_of(2) {
        eprintln!("--at-hex expects an even-length ASCII hex string, got {s:?}");
        std::process::exit(1);
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16).unwrap_or_else(|_| {
                eprintln!("--at-hex: bad hex byte {:?}", &s[i..i + 2]);
                std::process::exit(1);
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// helpers (migrated from autumn_client.rs)
// ---------------------------------------------------------------------------

/// Assemble `split_points.len() + 1` contiguous `(start_key, end_key)`
/// ranges from ordered split points — first range starts unbounded (`[]`),
/// last ends unbounded. Shared tail of `hex_split_ranges` /
/// `fuse_split_ranges` (only their split-point construction differs).
fn ranges_from_split_points(split_points: Vec<Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let n = split_points.len() + 1;
    let mut ranges = Vec::with_capacity(n);
    for i in 0..n {
        let start_key = if i == 0 {
            vec![]
        } else {
            split_points[i - 1].clone()
        };
        let end_key = if i == n - 1 {
            vec![]
        } else {
            split_points[i].clone()
        };
        ranges.push((start_key, end_key));
    }
    ranges
}

pub(crate) fn hex_split_ranges(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    if n <= 1 {
        return vec![(vec![], vec![])];
    }
    let start: u64 = 0x00000000;
    let end: u64 = 0xFFFFFFFF;
    let size = (end - start) / n as u64;

    let mut split_points: Vec<Vec<u8>> = Vec::new();
    for i in 1..n {
        let point = start + size * i as u64;
        let hex_str = format!("{:08x}", point);
        split_points.push(hex_str.into_bytes());
    }

    ranges_from_split_points(split_points)
}

/// Split keys aimed at the autumn-fuse keyspace
/// (`crates/fuse/src/key.rs` — `[0x01]inode_meta`, `[0x02]dirent`,
/// `[0x03]file_extent`, `[0x04]super`). The bulk of fuse data is
/// the `[0x03][ino BE][logical_off BE]` file extents — they outsize
/// the other prefixes by orders of magnitude on real model-serving
/// workloads (sglang / vllm checkpoints).
///
/// We split on the BOTTOM byte of `ino` (byte index 8 of the key —
/// `[0x03][7 high zero bytes][low byte]`). Rationale:
/// - Sequential inode allocation steps through low bytes
///   `0x00, 0x01, ..., 0xFF, 0x00, ...` → uniform round-robin
///   across N partitions for any cluster size.
/// - High bytes only change once inode count exceeds 256 / 65k /
///   16M — they're effectively zero for the lifetime of a
///   practical fuse fileset, so splitting on them puts everything
///   in partition 0.
/// - A single file's extents all share one inode → land in one
///   partition; concurrent reads of DIFFERENT files distribute,
///   which is the read-scatter shape sglang wants.
///
/// Partition 0 also absorbs `[0x01]` inode-meta + `[0x02]` dirent
/// (both prefix-sort before `[0x03]`); partition N-1 absorbs
/// `[0x04]` superblock (sorts after every `[0x03]` key) and any
/// non-fuse prefix ≥ 0x05. These are tiny next to file data.
///
/// **SUPERSEDED for real workloads by `autumn-op presplit --namespace fs`.**
/// This produces RAW `[0x03][ino low byte]` split
/// points with NO `{tenant}/{namespace}/` prefix, so under SD-2/SD-3 tenant-first
/// keys (`{t}/fs/[0x03][ino]`) it does NOT match any real wire key — every file
/// lands in one partition. It also steps the ino LOW byte (0x20/0x40/…), but
/// real inodes 4–8 are all < 0x20, so byte-stepping never splits them. Kept only
/// as the namespace-blind bootstrap fallback (`AUTUMN_BOOTSTRAP_PRESPLIT`); use
/// `presplit` after the tenant exists.
pub(crate) fn fuse_split_ranges(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    if n <= 1 {
        return vec![(vec![], vec![])];
    }
    let n = n.min(256); // 1 byte → at most 256 buckets
    let mut split_points: Vec<Vec<u8>> = Vec::with_capacity(n - 1);
    for i in 1..n {
        // Proportional boundary, NOT a floor stride: `(i * 256) / n` keeps
        // every bucket within ±1 low-byte even when n does not divide 256.
        // A `256 / n` floor stride let the LAST bucket absorb the entire
        // remainder (n=129 → stride=1 → bucket 128 spans [128,256), half the
        // keyspace — defeating the round-robin spread). Identical to the old
        // value for divisors of 256 (n=8 → 32,64,…,224), so behaviour for the
        // common pre-split sizes is unchanged.
        let byte = ((i * 256) / n) as u8; // 0x20, 0x40, … for N=8
        split_points.push(vec![0x03, 0, 0, 0, 0, 0, 0, 0, byte]);
    }
    ranges_from_split_points(split_points)
}

// ── Per-namespace presplit rules ─────────────────────────────────────────────
// Presplit a `{tenant}/{namespace}/` keyspace by the namespace's NATURAL
// high-entropy dimension (a raw-byte uniform split is namespace-blind — after
// SD-2/SD-3 all real keys sit in the `fs/`/`kvc/`/`mem/` byte sliver, so uniform
// splitting collapses everything into a couple of partitions). Each rule returns
// SUFFIXES relative to `{tenant}/{namespace}/`, ASCENDING; `cmd_presplit`
// prepends the tenant-first `{tenant}/{namespace}/` prefix to get raw wire cut
// points and splits the owning partition at each.

/// Per-namespace presplit rule (key structure ⇒ split dimension).
#[derive(Debug, Clone)]
pub(crate) enum PresplitRule {
    /// `fs` = split by INODE. The fs data key is `[0x03][ino BE][off BE]`, so a
    /// cut at `[0x03][ino BE]` puts every extent of inodes `< ino` on the left.
    /// Splitting at a sequence of inodes gives each inode its own partition
    /// (retires the old low-byte 0x20/0x40 stepping — inodes 4–8 are all < 0x20,
    /// so byte-stepping never split them). `inos` MUST be ascending + distinct.
    Fs { inos: Vec<u64> },
    /// `fs --lanes N`: split fs into N **lane** partitions, cutting
    /// at `[0x03][1]..[0x03][N-1]` (the static, ino-independent lane boundaries).
    /// A striped large file's extent at offset `off` lands on lane
    /// `(off/unit)%N` under key `[0x03][lane][ino][off]`, so its extents spread
    /// across these N partitions → parallel write/read. Lane 0 (`[start,[0x03][1])`)
    /// also holds the 0x01/0x02 metadata + any legacy non-striped `[0x03][ino…]`.
    FsLanes { lanes: u8, parts: u8 },
    /// `kvc` = split by CONTENT HASH (sha256 hexdigest, lowercase hex, uniform).
    /// The high-entropy dimension is at the TAIL, under `hash_prefix` segments, so
    /// `count` buckets split the FIRST hex char proportionally. `hash_prefix` is
    /// the RELATIVE prefix from the namespace root down to just before the hash
    /// hex — it is per-MODEL, so there is no fixed default: the vLLM connector
    /// stores `{model}/vllm/v1/{hash}/{layer}` (POOL=vllm, FMT=v1), i.e. the hash
    /// is under `{model}/vllm/v1/`, NOT directly under `vllm/`. Needs the model
    /// online (its fingerprint prefix known — inspect a live key to find it).
    Kvc { hash_prefix: Vec<u8>, count: usize },
    /// `mem` = split by AGENT. The mem key is `{agent}/…`, so cut at `{agent}/`
    /// for each explicit boundary. `agents` are the boundary agent names
    /// (already in their key form — simple names are identity).
    Mem { agents: Vec<String> },
    /// ANY other namespace = split by UNIFORM HEX over the 8-hex-char space, the
    /// namespace-relative replacement for the retired `bootstrap --presplit
    /// N:hexstring`. That flag cut the RAW keyspace, so under Option 3 its points
    /// (`2aaaaaaa`, `aaaaaaa8`, …) sat nowhere near a namespaced key and the
    /// presplit silently did nothing — for the benches, `bench_user_starts` kept
    /// only partitions starting with `bench/perf/` and got none, so every
    /// `--partitions N` run measured ONE partition (BUG-BENCH-NS-UNREGISTERED).
    /// Cutting relative to the namespace prefix puts the points where the keys
    /// actually are.
    Hex { count: usize },
}

/// The 16 hex digits in ASCENDING byte order (`'0'..'9'` = 0x30–0x39 then
/// `'a'..'f'` = 0x61–0x66 — both ascending, and `'9' < 'a'`), matching the
/// sha256 hexdigest charset.
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Compute the ASCENDING relative split suffixes for a presplit rule. Empty =
/// nothing to split (0/1 buckets). Returns an error on a malformed rule.
pub(crate) fn presplit_suffixes(rule: &PresplitRule) -> Result<Vec<Vec<u8>>> {
    match rule {
        PresplitRule::Fs { inos } => {
            // Cut at `[0x03][ino BE]` for each inode.
            let mut prev: Option<u64> = None;
            let mut out = Vec::with_capacity(inos.len());
            for &ino in inos {
                if let Some(p) = prev {
                    if ino <= p {
                        bail!("fs presplit inodes must be strictly ascending (got {p} then {ino})");
                    }
                }
                prev = Some(ino);
                let mut s = Vec::with_capacity(9);
                s.push(0x03u8); // PREFIX_EXTENT
                s.extend_from_slice(&ino.to_be_bytes());
                out.push(s);
            }
            Ok(out)
        }
        PresplitRule::FsLanes { lanes, parts } => {
            // LANES are the key layout (how many stripes a file is cut into);
            // PARTS is how many partitions we actually create. They are separate
            // decisions: a file is striped across `lanes` regardless, and the
            // lanes simply share partitions until someone cuts more.
            //
            // Cut at `[0x03][k * lanes/parts]` for k = 1..parts-1, so each
            // partition owns a contiguous run of EXACTLY `lanes/parts` lanes.
            // These boundaries are STATIC (independent of any ino), which is
            // what lets fs be pre-split before a single file exists.
            //
            // `parts` MUST divide `lanes` — otherwise the runs are uneven (some
            // partitions own 2 lanes, some 1) and every file lands lopsided, so
            // this is rejected rather than rounded.
            if *lanes < 2 || *parts < 2 {
                return Ok(vec![]);
            }
            if *parts > *lanes {
                bail!(
                    "presplit --lanes {lanes} --parts {parts}: parts cannot exceed lanes \
                     (there are only {} lane boundaries to cut at)",
                    lanes - 1
                );
            }
            if lanes % parts != 0 {
                bail!(
                    "presplit --lanes {lanes} --parts {parts}: parts must DIVIDE lanes, else \
                     partitions own uneven lane runs and every striped file lands lopsided. \
                     Divisors of {lanes}: {}",
                    (1..=*lanes).filter(|p| lanes % p == 0).map(|p| p.to_string())
                        .collect::<Vec<_>>().join(", ")
                );
            }
            let step = lanes / parts;
            Ok((1..*parts).map(|k| vec![0x03u8, k * step]).collect())
        }
        PresplitRule::Kvc { hash_prefix, count } => {
            if *count < 2 {
                return Ok(vec![]); // 0/1 buckets → no cut
            }
            let n = (*count).min(16); // one hex char → at most 16 buckets
            let mut out = Vec::with_capacity(n - 1);
            for i in 1..n {
                // Proportional boundary over the 16 hex digits (same ±1 spread
                // rationale as fuse_split_ranges).
                let pos = (i * 16) / n; // 1..15
                let mut s = hash_prefix.clone();
                s.push(HEX_DIGITS[pos]);
                out.push(s);
            }
            Ok(out)
        }
        PresplitRule::Hex { count } => {
            // Mirror `hex_split_ranges`' point choice, but as RELATIVE suffixes:
            // evenly spaced over the 32-bit space rendered as 8 lowercase hex
            // chars, which is the shape the bench key builder produces.
            if *count < 2 {
                return Ok(vec![]);
            }
            let span = 0xFFFF_FFFFu64 / *count as u64;
            Ok((1..*count)
                .map(|i| format!("{:08x}", span * i as u64).into_bytes())
                .collect())
        }
        PresplitRule::Mem { agents } => {
            // Cut at `{agent}/` for each boundary; the agent segment ends at '/'.
            let mut out: Vec<Vec<u8>> = Vec::with_capacity(agents.len());
            for a in agents {
                if a.is_empty() {
                    bail!("mem presplit agent boundary must be non-empty");
                }
                let mut s = a.as_bytes().to_vec();
                s.push(b'/');
                out.push(s);
            }
            out.sort();
            out.dedup();
            Ok(out)
        }
    }
}

/// UX-fix (M4): the FULL declared boundary grid to RECORD as sacred,
/// which for `FsLanes` is EVERY lane boundary (`lanes-1`), not just the
/// `parts-1` we physically cut this run.
///
/// Why record more than we cut: a `--lanes 24 --parts 6` run creates 6
/// partitions but the LOGICAL grid is 24 lanes. A partition owning lanes [0,4)
/// holds no cut point today, so a later auto-split of it would fall back to a
/// PS median — cutting INSIDE a lane (usually inside one inode's extents),
/// breaking the whole-lane-ownership invariant. Recording all 24-lane
/// boundaries makes auto-split SNAP to the next lane boundary instead, and makes
/// the merge guard protect the lane grid an operator actually declared. This is
/// the same "record INTENT, not just applied" philosophy `cmd_presplit` already
/// applies to the parts grid. For non-fs rules the record grid == the cut grid.
pub(crate) fn presplit_record_suffixes(rule: &PresplitRule) -> Result<Vec<Vec<u8>>> {
    match rule {
        PresplitRule::FsLanes { lanes, .. } => {
            if *lanes < 2 {
                return Ok(vec![]);
            }
            // ALL lane boundaries `[0x03][lane]` for lane = 1..lanes.
            Ok((1..*lanes).map(|lane| vec![0x03u8, lane]).collect())
        }
        // Every other rule cuts exactly its declared grid, so record == cut.
        other => presplit_suffixes(other),
    }
}

fn parse_byte_size(s: &str) -> Result<u64> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        bail!("empty byte-size value");
    }
    let bytes = trimmed.as_bytes();
    let mut split = bytes.len();
    for (i, &b) in bytes.iter().enumerate() {
        if !b.is_ascii_digit() && b != b'.' {
            split = i;
            break;
        }
    }
    let (num, unit) = trimmed.split_at(split);
    let num: f64 = num.parse().context("byte-size numeric prefix")?;
    let mul: f64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0_f64.powi(4),
        other => bail!("unknown byte-size suffix {other:?}"),
    };
    let n = (num * mul).round();
    if n.is_sign_negative() || !n.is_finite() {
        bail!("byte-size out of range");
    }
    Ok(n as u64)
}

pub(crate) fn parse_replication(s: &str) -> Result<u32> {
    let n_str = s.split('+').next().unwrap_or(s);
    let n: u32 = n_str.parse().context("parse replica count")?;
    if n == 0 {
        bail!("replication count must be >= 1");
    }
    Ok(n)
}

fn parse_ec_flag(s: &str) -> Result<(u32, u32)> {
    let parts: Vec<&str> = s.splitn(2, '+').collect();
    if parts.len() != 2 {
        bail!("EC shape must be K+M (e.g. '3+1'), got '{s}'");
    }
    let k: u32 = parts[0]
        .parse()
        .with_context(|| format!("parse K in EC '{s}'"))?;
    let m: u32 = parts[1]
        .parse()
        .with_context(|| format!("parse M in EC '{s}'"))?;
    if k == 0 || m == 0 {
        bail!("EC K and M must both be >= 1, got '{s}'");
    }
    Ok((k, m))
}

#[cfg(test)]
mod tests {
    use super::{parse_hex_split_points, SplitPoint};

    // ── CLI --presplit hex parsing ────────────────────────────
    #[test]
    fn parse_hex_split_points_decodes_comma_separated_hex() {
        assert_eq!(parse_hex_split_points(""), Vec::<Vec<u8>>::new());
        assert_eq!(parse_hex_split_points("   "), Vec::<Vec<u8>>::new());
        assert_eq!(parse_hex_split_points("0102"), vec![vec![0x01u8, 0x02]]);
        assert_eq!(
            parse_hex_split_points("00,ff, dead"),
            vec![vec![0x00u8], vec![0xff], vec![0xde, 0xad]]
        );
        // trailing/empty segments are skipped, not turned into empty points.
        assert_eq!(parse_hex_split_points("ab,,cd,"), vec![vec![0xab], vec![0xcd]]);
    }

    // ── CLI split-point assembly ──────────────────────

    #[test]
    fn split_point_median_resolves_to_none() {
        assert_eq!(SplitPoint::Median.resolve_at_key(), None);
    }

    #[test]
    fn split_point_namespaced_with_suffix_assembles_prefix() {
        // NS-FIRST; tenant is an in-namespace sub-segment.
        let p = SplitPoint::Namespaced {
            namespace: "kvc".into(),
            tenant: "acme".into(),
            suffix: b"vllm/v1/80".to_vec(),
        };
        assert_eq!(
            p.resolve_at_key().unwrap(),
            b"kvc/acme/vllm/v1/80".to_vec()
        );
    }

    #[test]
    fn split_point_empty_tenant_cuts_at_namespace_boundary() {
        // fs has no tenant → cut at the namespace boundary `fs/`.
        let p = SplitPoint::Namespaced {
            namespace: "fs".into(),
            tenant: String::new(),
            suffix: vec![],
        };
        assert_eq!(p.resolve_at_key().unwrap(), b"fs/".to_vec());
    }

    #[test]
    fn split_point_empty_suffix_is_pair_boundary() {
        // With a tenant sub-segment, empty suffix cuts at "ns/tenant/".
        let p = SplitPoint::Namespaced {
            namespace: "kvc".into(),
            tenant: "acme".into(),
            suffix: vec![],
        };
        assert_eq!(p.resolve_at_key().unwrap(), b"kvc/acme/".to_vec());
    }

    #[test]
    fn split_point_hex_suffix_carries_binary_bytes() {
        // --at-hex path: a binary suffix (e.g. an fs inode prefix) rides on
        // the `{ns}/` assembly (fs has no tenant sub-segment).
        let suffix = super::parse_hex_key("0103ff00");
        assert_eq!(suffix, vec![0x01, 0x03, 0xff, 0x00]);
        let p = SplitPoint::Namespaced {
            namespace: "fs".into(),
            tenant: String::new(),
            suffix,
        };
        let mut expected = b"fs/".to_vec();
        expected.extend_from_slice(&[0x01, 0x03, 0xff, 0x00]);
        assert_eq!(p.resolve_at_key().unwrap(), expected);
    }

    #[test]
    fn valid_segment_enforces_single_path_segment() {
        // coco P2: the segment charset must be [a-z0-9._-]+ so the
        // (ns, tenant, suffix) triple is 1:1 with the assembled raw key.
        assert!(super::valid_segment("kvc"));
        assert!(super::valid_segment("acme"));
        assert!(super::valid_segment("default"));
        assert!(super::valid_segment("model-cfg_456cf7.0"));
        // Rejected: empty, embedded '/', uppercase, whitespace, other bytes.
        assert!(!super::valid_segment(""));
        assert!(!super::valid_segment("acme/prod")); // the collision case
        assert!(!super::valid_segment("Acme"));
        assert!(!super::valid_segment("a b"));
        assert!(!super::valid_segment("a:b"));
    }

    #[test]
    fn split_point_raw_escape_hatch_passes_through() {
        let p = SplitPoint::Raw(vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(p.resolve_at_key().unwrap(), vec![0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn build_split_point_classification() {
        // Nothing → median.
        assert!(matches!(
            super::build_split_point(None, None, None, None),
            SplitPoint::Median
        ));
        // ns + tenant, no suffix → pair-boundary namespaced (empty suffix).
        match super::build_split_point(Some("a".into()), Some("b".into()), None, None) {
            SplitPoint::Namespaced { suffix, .. } => assert!(suffix.is_empty()),
            _ => panic!("expected namespaced"),
        }
        // ns + tenant + suffix → namespaced with suffix.
        match super::build_split_point(
            Some("a".into()),
            Some("b".into()),
            Some(b"s".to_vec()),
            None,
        ) {
            SplitPoint::Namespaced { suffix, .. } => assert_eq!(suffix, b"s".to_vec()),
            _ => panic!("expected namespaced"),
        }
        // raw escape hatch alone → raw.
        assert!(matches!(
            super::build_split_point(None, None, None, Some(vec![1, 2, 3])),
            SplitPoint::Raw(_)
        ));
    }

    #[test]
    fn parse_byte_size_accepts_plain_integer() {
        assert_eq!(
            super::parse_byte_size("16777216").unwrap(),
            16 * 1024 * 1024
        );
        assert_eq!(super::parse_byte_size("0").unwrap(), 0);
    }

    #[test]
    fn parse_byte_size_accepts_suffixes() {
        assert_eq!(super::parse_byte_size("512K").unwrap(), 512 * 1024);
        assert_eq!(super::parse_byte_size("16M").unwrap(), 16 * 1024 * 1024);
        assert_eq!(super::parse_byte_size("16MiB").unwrap(), 16 * 1024 * 1024);
        assert_eq!(super::parse_byte_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(super::parse_byte_size("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(
            super::parse_byte_size("2T").unwrap(),
            2u64 * 1024 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn parse_ec_flag_accepts_valid_shapes() {
        assert_eq!(super::parse_ec_flag("3+1").unwrap(), (3, 1));
        assert_eq!(super::parse_ec_flag("4+2").unwrap(), (4, 2));
        assert!(super::parse_ec_flag("3").is_err());
        assert!(super::parse_ec_flag("0+1").is_err());
        assert!(super::parse_ec_flag("3+0").is_err());
    }

    #[test]
    fn parse_replication_extracts_first_n() {
        assert_eq!(super::parse_replication("3+0").unwrap(), 3);
        assert_eq!(super::parse_replication("3").unwrap(), 3);
        assert!(super::parse_replication("0+0").is_err());
    }

    #[test]
    fn hex_split_ranges_partitions_full_space() {
        let ranges = super::hex_split_ranges(1);
        assert_eq!(ranges.len(), 1);
        assert!(ranges[0].0.is_empty() && ranges[0].1.is_empty());

        let ranges = super::hex_split_ranges(4);
        assert_eq!(ranges.len(), 4);
        assert!(ranges[0].0.is_empty());
        assert!(ranges[3].1.is_empty());
        for i in 0..3 {
            assert_eq!(ranges[i].1, ranges[i + 1].0);
        }
    }

    #[test]
    fn fuse_split_ranges_single_partition_is_full_space() {
        let ranges = super::fuse_split_ranges(1);
        assert_eq!(ranges.len(), 1);
        assert!(ranges[0].0.is_empty() && ranges[0].1.is_empty());
    }

    // ── Per-namespace presplit rules ──────────────────────────────────────────────

    /// The full tenant-first wire cut point = `{tenant}/{ns}/ ++ suffix`.
    fn wire(tenant: &str, ns: &str, suffix: &[u8]) -> Vec<u8> {
        let mut k = format!("{tenant}/{ns}/").into_bytes();
        k.extend_from_slice(suffix);
        k
    }

    #[test]
    fn presplit_fs_cuts_at_inode_boundaries() {
        use super::PresplitRule;
        // Retires the low-byte stepping: inodes 4–8 are all < 0x20 but each gets
        // its own cut here.
        let suffixes =
            super::presplit_suffixes(&PresplitRule::Fs { inos: vec![4, 5, 6, 7, 8] }).unwrap();
        assert_eq!(suffixes.len(), 5);
        // suffix = [0x03][ino BE 8]
        assert_eq!(suffixes[0], vec![0x03, 0, 0, 0, 0, 0, 0, 0, 4]);
        assert_eq!(suffixes[4], vec![0x03, 0, 0, 0, 0, 0, 0, 0, 8]);
        // ascending on the wire, so limit-scans + PS range checks stay sane.
        let pts: Vec<Vec<u8>> = suffixes.iter().map(|s| wire("default", "fs", s)).collect();
        for w in pts.windows(2) {
            assert!(w[0] < w[1], "cut points must be ascending");
        }
        // An extent of inode 5 (`[0x03][5][off]`) sorts >= the `[0x03][5]` cut
        // and < the `[0x03][6]` cut → lands in inode 5's own partition.
        let ext5 = wire("default", "fs", &[0x03, 0, 0, 0, 0, 0, 0, 0, 5, /*off*/ 0, 0, 0, 0, 0, 0, 0, 0]);
        assert!(ext5 >= pts[1] && ext5 < pts[2]);
    }

    #[test]
    fn presplit_fs_rejects_non_ascending() {
        use super::PresplitRule;
        assert!(super::presplit_suffixes(&PresplitRule::Fs { inos: vec![5, 4] }).is_err());
        assert!(super::presplit_suffixes(&PresplitRule::Fs { inos: vec![4, 4] }).is_err());
    }

    #[test]
    fn presplit_fs_lanes_cuts_at_lane_boundaries() {
        use super::PresplitRule;
        // 4 lanes → cut at [0x03][1], [0x03][2], [0x03][3] → 4 partitions.
        let s = super::presplit_suffixes(&PresplitRule::FsLanes { lanes: 4, parts: 4 }).unwrap();
        assert_eq!(s, vec![vec![0x03, 1], vec![0x03, 2], vec![0x03, 3]]);
        // A striped extent on lane 2 (wire [0x03][2][ino][off]) sorts into the
        // partition [[0x03][2], [0x03][3]).
        let ext_lane2 = wire("default", "fs", &[0x03, 2, /*ino*/ 0, 0, 0, 0, 0, 0, 0, 9, /*off*/ 0, 0, 0, 0, 0, 0, 0, 0]);
        let cut2 = wire("default", "fs", &s[1]); // [0x03][2]
        let cut3 = wire("default", "fs", &s[2]); // [0x03][3]
        assert!(ext_lane2 >= cut2 && ext_lane2 < cut3);
        // < 2 lanes → no cut.
        assert!(super::presplit_suffixes(&PresplitRule::FsLanes { lanes: 1, parts: 1 }).unwrap().is_empty());
    }

    #[test]
    fn presplit_kvc_splits_hash_first_hex_char_proportionally() {
        use super::PresplitRule;
        // Realistic vLLM prefix: the hash sits under `{model}/vllm/v1/`, NOT
        // directly under `vllm/` (see PresplitRule::Kvc doc / P1 review fix).
        let hp = b"qwen3-8b/vllm/v1/";
        let s = super::presplit_suffixes(&PresplitRule::Kvc {
            hash_prefix: hp.to_vec(),
            count: 4,
        })
        .unwrap();
        // 4 buckets → boundaries at hex positions 4,8,12 → '4','8','c'.
        let sfx = |c: u8| { let mut v = hp.to_vec(); v.push(c); v };
        assert_eq!(s, vec![sfx(b'4'), sfx(b'8'), sfx(b'c')]);
        // A sha256-hexdigest key starting '9' lands in the 3rd bucket [8, c).
        let k9 = wire("t", "kvc", &{ let mut v = hp.to_vec(); v.extend_from_slice(b"9abc..."); v });
        let p8 = wire("t", "kvc", &sfx(b'8'));
        let pc = wire("t", "kvc", &sfx(b'c'));
        assert!(k9 >= p8 && k9 < pc);
        // count < 2 → no cut.
        assert!(super::presplit_suffixes(&PresplitRule::Kvc { hash_prefix: hp.to_vec(), count: 1 })
            .unwrap()
            .is_empty());
    }

    #[test]
    fn presplit_mem_cuts_at_agent_boundaries_sorted() {
        use super::PresplitRule;
        let s = super::presplit_suffixes(&PresplitRule::Mem {
            agents: vec!["m".into(), "b".into(), "z".into()], // out of order on input
        })
        .unwrap();
        assert_eq!(s, vec![b"b/".to_vec(), b"m/".to_vec(), b"z/".to_vec()]); // sorted
        // agent "chatbot" (starts 'c') sorts >= "b/" and < "m/" → bucket [b, m).
        let kc = wire("t", "mem", b"chatbot/ep/1");
        let pb = wire("t", "mem", b"b/");
        let pm = wire("t", "mem", b"m/");
        assert!(kc >= pb && kc < pm);
        // empty agent name rejected.
        assert!(super::presplit_suffixes(&PresplitRule::Mem { agents: vec!["".into()] }).is_err());
    }

    #[test]
    fn fuse_split_ranges_n8_keys_are_inode_bottom_byte_boundaries() {
        let ranges = super::fuse_split_ranges(8);
        assert_eq!(ranges.len(), 8);
        // First range starts at empty, last range ends at empty.
        assert!(ranges[0].0.is_empty());
        assert!(ranges[7].1.is_empty());
        // Contiguous: each range's end == next range's start.
        for i in 0..7 {
            assert_eq!(ranges[i].1, ranges[i + 1].0);
        }
        // Each split key is [0x03, 0,0,0,0,0,0,0, byte_i] with byte_i
        // stepping 0x20 → 0x40 → ... → 0xE0.
        for i in 0..7 {
            let split_key = &ranges[i].1;
            assert_eq!(split_key.len(), 9);
            assert_eq!(split_key[0], 0x03);
            for b in &split_key[1..8] {
                assert_eq!(*b, 0u8);
            }
            assert_eq!(split_key[8], ((i + 1) * 32) as u8);
        }
    }

    #[test]
    fn fuse_split_ranges_distributes_sequential_inodes_round_robin() {
        use std::collections::HashMap;
        let n = 8;
        let ranges = super::fuse_split_ranges(n);
        let mut hits: HashMap<usize, usize> = HashMap::new();
        // Walk 256 sequential inodes (ino=0..=255 — all bottom-byte
        // values exactly once) and check the bottom-byte split places
        // them uniformly across 8 partitions.
        for ino in 0u64..256 {
            let mut key = vec![0x03];
            key.extend_from_slice(&ino.to_be_bytes());
            key.extend_from_slice(&0u64.to_be_bytes()); // logical_off = 0
            // Find the partition whose range contains `key`.
            let mut found = None;
            for (idx, (start, end)) in ranges.iter().enumerate() {
                let after_start = start.is_empty() || key.as_slice() >= start.as_slice();
                let before_end = end.is_empty() || key.as_slice() < end.as_slice();
                if after_start && before_end {
                    found = Some(idx);
                    break;
                }
            }
            *hits.entry(found.unwrap()).or_insert(0) += 1;
        }
        // Each of the 8 partitions should get exactly 32 inodes.
        for p in 0..n {
            assert_eq!(*hits.get(&p).unwrap_or(&0), 32, "partition {p}");
        }
    }

    #[test]
    fn fuse_split_ranges_non_divisor_n_stays_balanced() {
        // Reproduce-first guard for the floor-stride skew: with the old
        // `256 / n` stride, a non-divisor n let the LAST bucket absorb the
        // whole remainder (n=6 → bucket 5 got 46 inodes vs 42 for the rest).
        // The proportional `(i*256)/n` boundary keeps every bucket within ±1.
        let n = 6;
        let ranges = super::fuse_split_ranges(n);
        assert_eq!(ranges.len(), n);
        let mut hits = vec![0usize; n];
        for ino in 0u64..256 {
            let mut key = vec![0x03];
            key.extend_from_slice(&ino.to_be_bytes());
            key.extend_from_slice(&0u64.to_be_bytes());
            let idx = ranges
                .iter()
                .position(|(s, e)| {
                    (s.is_empty() || key.as_slice() >= s.as_slice())
                        && (e.is_empty() || key.as_slice() < e.as_slice())
                })
                .unwrap();
            hits[idx] += 1;
        }
        let (min, max) = (*hits.iter().min().unwrap(), *hits.iter().max().unwrap());
        assert!(
            max - min <= 1,
            "non-divisor n=6 must stay balanced; got per-bucket {hits:?} (spread {})",
            max - min
        );
        assert_eq!(hits.iter().sum::<usize>(), 256);
    }

    #[test]
    fn fuse_split_ranges_inode_meta_and_dirent_live_in_partition_zero() {
        let ranges = super::fuse_split_ranges(8);
        // [0x01]inode_meta + [0x02]dirent both prefix-sort before
        // [0x03]extents, so they MUST land in partition 0.
        let inode_meta_key = vec![0x01, 0, 0, 0, 0, 0, 0, 0, 0x42];
        let dirent_key = vec![0x02, 0, 0, 0, 0, 0, 0, 0, 0x01, b'x'];
        assert!(ranges[0].0.is_empty());
        assert!(inode_meta_key.as_slice() < ranges[0].1.as_slice());
        assert!(dirent_key.as_slice() < ranges[0].1.as_slice());
    }
}

#[cfg(test)]
mod lane_parts_tests {
    use super::{presplit_suffixes, PresplitRule};

    #[test]
    fn record_grid_is_the_full_lane_grid_not_just_the_cut_grid() {
        use super::{presplit_record_suffixes, presplit_suffixes, PresplitRule};
        // 24 lanes over 6 parts: we CUT 5 points (the parts grid) …
        let cut = presplit_suffixes(&PresplitRule::FsLanes { lanes: 24, parts: 6 }).unwrap();
        assert_eq!(cut.len(), 5);
        // … but RECORD all 23 lane boundaries (M4): a partition owning lanes
        // [0,4) then holds a declared point so auto-split snaps to a lane
        // boundary instead of a median inside a lane.
        let rec = presplit_record_suffixes(&PresplitRule::FsLanes { lanes: 24, parts: 6 }).unwrap();
        assert_eq!(rec.len(), 23);
        assert_eq!(rec[0], vec![0x03, 1]);
        assert_eq!(rec[22], vec![0x03, 23]);
        // The cut grid is a subset of the record grid.
        for c in &cut {
            assert!(rec.contains(c), "cut point {c:?} must be in the recorded grid");
        }
        // Non-fs rules record exactly what they cut.
        let hexc = presplit_suffixes(&PresplitRule::Hex { count: 4 }).unwrap();
        let hexr = presplit_record_suffixes(&PresplitRule::Hex { count: 4 }).unwrap();
        assert_eq!(hexc, hexr);
    }

    #[test]
    fn hex_rule_cuts_relative_to_the_namespace() {
        use super::{presplit_suffixes, PresplitRule};
        let s = presplit_suffixes(&PresplitRule::Hex { count: 4 }).unwrap();
        // RELATIVE suffixes — the caller prepends `{ns}/{tenant}/`, which is the
        // whole point: the retired bootstrap presplit emitted these as ABSOLUTE
        // raw keys, where no namespaced key ever reaches them.
        assert_eq!(s.len(), 3);
        assert_eq!(s[0], b"3fffffff".to_vec());
        assert_eq!(s[1], b"7ffffffe".to_vec());
        assert_eq!(s[2], b"bffffffd".to_vec());
        // Ascending (required by the presplit orchestration) and fixed width, so
        // they sort as bytes exactly as they sort numerically.
        assert!(s.windows(2).all(|w| w[0] < w[1]));
        assert!(s.iter().all(|p| p.len() == 8));
        // Degenerate counts cut nothing.
        assert!(presplit_suffixes(&PresplitRule::Hex { count: 1 }).unwrap().is_empty());
        assert!(presplit_suffixes(&PresplitRule::Hex { count: 0 }).unwrap().is_empty());
    }

    #[test]
    fn parts_must_divide_lanes() {
        // 24 lanes over 4 parts → each partition owns exactly 6 lanes.
        let s = presplit_suffixes(&PresplitRule::FsLanes { lanes: 24, parts: 4 }).unwrap();
        assert_eq!(s, vec![vec![0x03, 6], vec![0x03, 12], vec![0x03, 18]]);
        // Every divisor of 24 works and yields parts-1 cuts, evenly spaced.
        for p in [2u8, 3, 4, 6, 8, 12, 24] {
            let cuts = presplit_suffixes(&PresplitRule::FsLanes { lanes: 24, parts: p }).unwrap();
            assert_eq!(cuts.len() as u8, p - 1, "parts={p}");
            let step = 24 / p;
            for (k, c) in cuts.iter().enumerate() {
                assert_eq!(c[1], (k as u8 + 1) * step, "parts={p} cut {k}");
            }
        }
        // A non-divisor is REJECTED, not rounded: uneven lane runs make every
        // striped file land lopsided.
        let err = presplit_suffixes(&PresplitRule::FsLanes { lanes: 24, parts: 5 })
            .unwrap_err()
            .to_string();
        assert!(err.contains("must DIVIDE"), "{err}");
        assert!(err.contains("1, 2, 3, 4, 6, 8, 12, 24"), "should list divisors: {err}");
        // parts > lanes is impossible — only lanes-1 boundaries exist.
        assert!(presplit_suffixes(&PresplitRule::FsLanes { lanes: 4, parts: 8 }).is_err());
        // Degenerate = no cuts (striping still applies; it's placement that's a no-op).
        assert!(presplit_suffixes(&PresplitRule::FsLanes { lanes: 24, parts: 1 }).unwrap().is_empty());
        assert!(presplit_suffixes(&PresplitRule::FsLanes { lanes: 1, parts: 1 }).unwrap().is_empty());
    }
}
