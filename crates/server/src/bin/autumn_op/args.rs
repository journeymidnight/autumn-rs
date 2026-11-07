//! `autumn-op` CLI argument parsing. The global flags (`OpParams` +
//! `ClientParams`) are taken anywhere on the line by
//! `autumn_params::parse_or_exit`; the command and its own flags are parsed
//! here from what is left.

use anyhow::{bail, Context, Result};
use autumn_params::op::OpParams;
use autumn_params::{Positional, Section};
use autumn_transport::TransportKind;

/// The command list; `--help` and every usage error print it with the
/// global flags after it.
const USAGE: &str = "\
usage: autumn-op [options] <COMMAND>
Global options may stand before or after the command.

read / observability commands:
  status                       the fleet against its expected members: managers,
                               PS ready, EN online, extents, rebuilds, sample time
  list-nodes                   show every EN's auto-state + override
  health [--detail N]          sealed extents clean / degraded / unavailable, slots
                               by state, and the N worst extents (default 10)
  extent-health [--node ID] [--all]
                               per-slot health (default: only unhealthy)
  list-ec-markers              ConvertToEc inflight markers + coord state
  recovery-stats               in-flight + per-source/target counters
  audit-log [--op N] [--node N] [--since S] [--until U] [--limit L]
                               query operator action history
  info [--part PID] [--detail] show cluster snapshot (--detail = partition load)
  info --file-sd               Prometheus file_sd JSON: every PS / EN /metrics endpoint
  policy-candidates            advisory split/merge/gc/compact/ec candidates

op observability:
  ops status <id>              live state of one op, incl. progress
  ops list [--active] [--kind K] [--limit N]   live ledger (leader-local)
  ops history [--kind K] [--since UNIX] [--limit N]
                               durable terminal history from etcd, newest first

node-lifecycle admin commands:
  fence-node <id> --reason \"...\" --by alice [--force]
  maintenance <id> --reason \"...\" --by alice [--expire UNIX_TS]
  unfence <id> --by alice
  remove <id> --by alice
  ps-remove <ps_id> --by alice
                               drop a stopped, evicted PS from the expected fleet
  manager-remove <manager_id> --by alice
                               drop a stopped manager from the expected fleet

runtime parameters (override a flag on every running manager / PS / EN):
  config keys                  every runtime parameter, its default and range
  config list                  the overrides in force
  config get <role.flag>
  config set <role.flag> <value> --by alice
                               e.g. ps.gc-rate-bytes-per-sec 32M; applied within
                               --config-poll-secs (30 s) on every PS / EN
  config unset <role.flag> --by alice
                               back to each process's flag value

cluster / partition admin commands (moved from autumn-client):
  bootstrap [--replication 3+0] [--log-ec K+M] [--row-ec K+M]
                               one partition over the whole keyspace; no namespace is
                               registered — create them with namespace-create
  set-stream-ec --stream <ID> --ec K+M
  force-ec-convert --extent <EXTID>
  repair [--cancel] <EXTID>... | [--cancel] --node <NODE_ID>
                               rebuild degraded copies on other nodes now, no fence
                               (--cancel: withdraw standing repair requests)
  scrub <EXTID>... | --part <PART_ID> | --all
                               check sealed copies against their checksums on the
                               nodes that hold them; rot is isolated and rebuilt
  split <PARTID> [--namespace <NS[/SUB]> [--at <SUFFIX> | --at-hex <HEX>]] [--at-raw-hex <HEX>]
  presplit --namespace <NS[/SUB]> --parts <N> [--force]
                               cut more later (namespace-create cuts first); presplit EMPTY
                               keyspace before loading. --parts N by NS:
      fs:    over its 24 stripe lanes, N divides 24   (--force to narrow a wider declared stripe)
      kvc:   at its hash buckets kvc/<hex>, N <= 16
      other: uniformly over hex under the scope
  merge <SURVIVOR_PARTID> <VICTIM_PARTID>
  rebalance [MAX_MOVES]        actively re-spread partitions across PS (0/absent = balance fully)
  compact <PARTID>
  gc [--ratio R] [--max-size B] [--stream-debt B] [--dead-bytes B] [--empty-only] <PARTID>
                               --dead-bytes: an extent with at least this many dead bytes
                               qualifies whatever its ratio (3 GiB in a 16 GiB extent is 0.195)
  forcegc <PARTID> <EXTID>...
  format <DIR>...
                               format dir(s), register node identity, stamp cluster_id
  gen-cluster-secret           print a new cluster secret (local, no manager)

namespace registry (admin):
  namespace-create --name <NS> [--parts <N>]
                               register a namespace, cut at <NS>/ and into N partitions (as
                               presplit; none = 1, auto-split needs auto-policy aggressive);
                               needs the PS up
  namespace-delete --name <NS> [--force]
                               remove the registry row (refuses non-empty unless --force)
  namespace-list [--json]      list registered namespaces (name/prefix/presplit/created)
  principal-list [--json]      list principals + their grants (read-only, no credentials)
  mint-token [--principal P] (--credential HEX | --credential-file F)
                               mint a capability token (the file names its principal)
";

fn usage() -> ! {
    eprint!("{}", help());
    std::process::exit(1);
}

fn help() -> String {
    autumn_params::help_text(USAGE, &[&OpParams::default() as &dyn Section, autumn_client::params()])
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

/// `mint-token`'s principal and raw credential: `--credential <hex>` (with
/// `--principal`) wins; otherwise the credential `--credential-file` names
/// (a global flag, so it may sit before or after the subcommand, in either
/// form that names a principal — what `principal-create`
/// prints, or `<principal>\n<hex>`). One `--credential-file` serves both
/// mint-token and the data plane, so an explicit hex does not conflict with it.
/// The file's principal fills an absent `--principal` and must match a given
/// one. The file used to be read as one hex string, which failed on both forms.
fn mint_token_credential(
    principal: String,
    hex: Option<&str>,
    file: Option<(String, Vec<u8>)>,
) -> Result<(String, Vec<u8>), String> {
    match (hex, file) {
        (Some(""), _) => Err("--credential is empty".into()),
        (Some(h), _) if !principal.is_empty() => {
            let credential = crate::hex_decode(h).map_err(|e| format!("--credential: {e:#}"))?;
            Ok((principal, credential))
        }
        (Some(_), _) => Err("--credential needs --principal".into()),
        (None, Some((named, secret))) => {
            if !principal.is_empty() && principal != named {
                return Err(format!(
                    "--principal {principal} does not match the file's principal {named}"
                ));
            }
            Ok((named, secret))
        }
        (None, None) => Err("--credential or --credential-file required".into()),
    }
}

pub(crate) struct Args {
    pub(crate) manager: String,
    pub(crate) json: bool,
    pub(crate) transport: TransportKind,
    /// `--cluster-secret-file`: installed before connecting, since every
    /// connection autumn-op opens declares the Admin role and must prove it.
    /// `None` = no secret, so only commands that never connect work.
    pub(crate) cluster_secret_file: Option<std::path::PathBuf>,
    /// data-plane credential (`--credential-file`), as
    /// `(principal, secret)`. The cluster secret authorizes CONTROL-plane RPCs;
    /// it does nothing for a partition-server key check. Some admin subcommands do touch
    /// the data plane — `presplit --namespace fs` reads the declared stripe
    /// geometry before overwriting it — and under PROTECT-EVERYTHING every key
    /// needs a capability token, so those need a credential too. `None` = connect
    /// without one (fine on an authz-off cluster, and for every command that
    /// only talks to the manager).
    pub(crate) credential: Option<(String, Vec<u8>)>,
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
/// lowers it to the raw wire key. Per the §3.4 CLI refinement (refinement 3's
/// term rule: prefixes are an implementation detail, the user-facing surface
/// speaks namespace + suffix):
/// - `Median` — no explicit point; PS picks the median (legacy).
/// - `Namespaced { scope, suffix }` — the normal operator form. `scope` is a
///   namespace or an in-namespace sub-scope (`bench/perf`), as `autumn-client
///   --namespace` takes it. The cut key = `"{scope}/" ++ suffix`; an EMPTY
///   `suffix` cuts exactly at `"{scope}/"`.
/// - `Raw(bytes)` — admin-only escape hatch (`--at-raw-hex`); the caller hand-
///   builds the whole prefix. Same posture as D7 ⑤ `raw()`: documented
///   admin-only, not the day-to-day path.
pub(crate) enum SplitPoint {
    Median,
    Namespaced {
        scope: String,
        suffix: Vec<u8>,
    },
    Raw(Vec<u8>),
}

impl SplitPoint {
    /// Lower the CLI intent to the RAW wire key (`None` = PS median).
    pub(crate) fn resolve_at_key(&self) -> Option<Vec<u8>> {
        match self {
            SplitPoint::Median => None,
            SplitPoint::Namespaced { scope, suffix } => {
                let mut key = format!("{scope}/").into_bytes();
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
            SplitPoint::Namespaced { scope, suffix } => {
                let base = format!("{scope}/");
                if suffix.is_empty() {
                    format!("scope boundary {base}")
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

pub(crate) enum ConfigCmd {
    /// Every runtime parameter (local, from the declarations).
    Keys,
    List,
    Get { key: String },
    Set { key: String, value: String, by: String },
    Unset { key: String, by: String },
}

pub(crate) enum Command {
    // read / observability
    ListNodes,
    // cluster-df: aggregate capacity summary (Ceph `ceph df` style)
    Df,
    ExtentHealth {
        node_filter: Vec<u64>,
        include_healthy: bool,
    },
    /// Extent health summary; `max_problems` worst extents are named.
    Health {
        max_problems: u32,
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
    PsRemove {
        ps_id: u64,
        by: String,
    },
    /// Runtime parameter overrides.
    Config(ConfigCmd),
    Status,
    ManagerRemove {
        manager_id: u64,
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
        /// `--file-sd`: the PS and EN `/metrics` endpoints as Prometheus
        /// `file_sd` JSON, instead of the snapshot.
        file_sd: bool,
    },
    PolicyCandidates,
    /// Full dashboard overview JSON (df + nodes + partitions + amp + advisories).
    /// Always emits JSON; consumed by the standalone dashboard (crates/server/src/bin/autumn_dashboard).
    Overview,
    /// Headless control of the leader-fenced auto-policy controller.
    /// `action` = status | start | deactivate | upsert | delete. The upsert
    /// fields (`switches`/`interval`/`cooldown`/`max_actions`/`desc`) define a
    /// custom policy; the rest are ignored by the read/select/delete actions.
    AutoPolicy {
        action: String,
        name: String,
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
        log_ec: Option<(u32, u32)>,
        row_ec: Option<(u32, u32)>,
    },
    SetStreamEc {
        stream_id: u64,
        ec_data: u32,
        ec_parity: u32,
    },
    /// Rebuild degraded slots elsewhere now: of these extents, or (empty) of
    /// every extent with a degraded slot on `node`.
    Repair {
        extents: Vec<u64>,
        node: Option<u64>,
        /// Withdraw standing requests instead of making them.
        cancel: bool,
    },
    /// Check sealed copies against their recorded checksums on the nodes that
    /// hold them: these extents, or every extent of `part`, or (both empty —
    /// the parser requires `--all` for that) every sealed extent in the cluster.
    Scrub {
        extents: Vec<u64>,
        part: Option<u64>,
    },
    ForceEcConvert {
        extent_id: u64,
    },
    Split {
        part_id: u64,
        /// (D4): where to cut. See `SplitPoint`.
        point: SplitPoint,
    },
    /// presplit a `{scope}/` keyspace by its namespace's natural dimension.
    /// `scope` is a namespace or an in-namespace sub-scope (`bench/perf`).
    /// Explicit op (NOT bootstrap — the namespace must exist first).
    Presplit {
        scope: String,
        rule: PresplitRule,
        /// UX-fix (M5): required to NARROW an fs's declared stripe
        /// geometry (24 lanes is less than a wider declaration an older fs may
        /// carry). Without it, a redeclare that lowers the lane count is refused:
        /// it would silently narrow every future file's stripe width fs-wide.
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
        dead_bytes: Option<u64>,
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
    /// Format is IDENTITY-ONLY (mints/reuses `node_uuid` +
    /// per-dir `disk_uuid`, registers with an EMPTY location). It no longer
    /// carries `listen`/`advertise`/`shard_ports` — the EN binary's own
    /// `--advertise` self-registers the live address + shard ports at every
    /// startup (mirrors Kafka `kafka-storage.sh format` / Ceph `ceph-volume
    /// prepare`: mkfs mints identity only, the daemon reports its own network
    /// location on every boot). See docs/en_dynamic_shard_design.md §2.2.
    Format {
        dirs: Vec<String>,
    },
    // ── data-plane authz tooling ──────────────────────────────────
    /// Generate an Ed25519 signing key (LOCAL — no manager). Prints the keyfile
    /// line (`<kid> <hex-seed>`) to stdout; redirect to `--auth-signing-key-file`.
    GenSigningKey {
        kid: u32,
    },
    /// Generate a cluster secret (LOCAL — no manager): 64 hex characters on
    /// stdout; redirect to the file every `--cluster-secret-file` names.
    GenClusterSecret,
    /// create/rotate a PRINCIPAL account (admin). Returns the principal's permanent credential (shown once,
    /// as `<name>\n<hex>` for direct save to a credential file).
    PrincipalCreate {
        principal: String,
        /// Granted key prefixes (`--grant fs/`, repeatable). Normalized to end `/`.
        grants: Vec<String>,
    },
    /// Remove a principal account (admin). Its current token still works until exp.
    PrincipalDelete {
        principal: String,
    },
    /// Mint a short-TTL capability token from a principal credential. Prints the
    /// token (hex) to stdout.
    MintToken {
        principal: String,
        credential: Vec<u8>,
    },
    /// D2: register a namespace (admin), then cut it at its head and into
    /// `--parts N` partitions (`rule`, None for one partition).
    NamespaceCreate {
        name: String,
        rule: Option<PresplitRule>,
        force: bool,
    },
    /// D2: delete a namespace registry row (admin). Refuses a non-empty
    /// namespace unless `--force` (the emptiness check is done client-side here,
    /// scanning the prefix — the manager has no KV data-plane client).
    NamespaceDelete {
        name: String,
        force: bool,
    },
    /// D2: list the full namespace registry (rich rows). Read-only.
    NamespaceList,
    /// list every principal + its grants. Read-only, and
    /// never prints credential material.
    PrincipalList,
}

/// The data-plane credential of `--credential-file`, read up front so a bad
/// file fails before any connection.
fn read_credential(path: &str) -> (String, Vec<u8>) {
    match autumn_client::read_credential_file(path) {
        Ok((principal, secret)) if !principal.is_empty() => (principal, secret),
        Ok(_) => {
            eprintln!("--credential-file {path}: missing principal name (expected '<principal>\n<hex>')");
            std::process::exit(2);
        }
        Err(e) => {
            eprintln!("{e:#}");
            std::process::exit(2);
        }
    }
}

pub(crate) fn parse() -> Args {
    // Global flags and client parameters may stand anywhere on the line
    // (`autumn-op compact 7 --wait`); what is left is the command.
    let g = OpParams::default();
    let raw = autumn_params::parse_or_exit(
        USAGE,
        &[&g as &dyn Section, autumn_client::params()],
        Positional::Anywhere,
    );
    match raw.first().map(String::as_str) {
        None => usage(),
        Some("--help" | "-h") => {
            print!("{}", help());
            std::process::exit(0);
        }
        Some(_) => {}
    }
    let credential = g.credential_file.get().as_deref().map(read_credential);
    let mut i = 0usize;
    let sub = raw[i].as_str();
    i += 1;
    let cmd = match sub {
        // read
        "list-nodes" => Command::ListNodes,
        "status" => Command::Status,
        "df" => Command::Df,
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
        "health" => {
            let mut max_problems = 10u32;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--detail" => {
                        i += 1;
                        max_problems = val(&raw, i).parse().unwrap_or_else(|_| usage());
                        i += 1;
                    }
                    // An unknown token must not silently become a plain
                    // `health` with exit 0.
                    other => {
                        eprintln!("health: unexpected argument {other:?}");
                        usage()
                    }
                }
            }
            Command::Health { max_problems }
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
        "config" => {
            let sub = val(&raw, i).to_owned();
            i += 1;
            let cmd = match sub.as_str() {
                "keys" => ConfigCmd::Keys,
                "list" => ConfigCmd::List,
                "get" => ConfigCmd::Get {
                    key: val(&raw, i).to_owned(),
                },
                "set" => {
                    let key = val(&raw, i).to_owned();
                    let value = val(&raw, i + 1).to_owned();
                    i += 2;
                    let (_reason, by, _force) = parse_admin_flags(&raw, &mut i);
                    ConfigCmd::Set { key, value, by }
                }
                "unset" => {
                    let key = val(&raw, i).to_owned();
                    i += 1;
                    let (_reason, by, _force) = parse_admin_flags(&raw, &mut i);
                    ConfigCmd::Unset { key, by }
                }
                _ => usage(),
            };
            Command::Config(cmd)
        }
        "ps-remove" => {
            let ps_id: u64 = raw
                .get(i)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| usage());
            i += 1;
            let (_reason, by, _force) = parse_admin_flags(&raw, &mut i);
            Command::PsRemove { ps_id, by }
        }
        "manager-remove" => {
            let manager_id: u64 = raw
                .get(i)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| usage());
            i += 1;
            let (_reason, by, _force) = parse_admin_flags(&raw, &mut i);
            Command::ManagerRemove { manager_id, by }
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
        "gen-cluster-secret" => Command::GenClusterSecret,
        "principal-create" => {
            let mut principal = String::new();
            let mut grants: Vec<String> = Vec::new();
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
                    _ => break,
                }
            }
            if principal.is_empty() || grants.is_empty() {
                usage();
            }
            Command::PrincipalCreate {
                principal,
                grants,
            }
        }
        "principal-delete" => {
            let mut principal = String::new();
            while i < raw.len() {
                match raw[i].as_str() {
                    "--principal" => {
                        i += 1;
                        principal = val(&raw, i).to_owned();
                        i += 1;
                    }
                    _ => break,
                }
            }
            if principal.is_empty() {
                usage();
            }
            Command::PrincipalDelete { principal }
        }
        "mint-token" => {
            let mut principal = String::new();
            let mut credential_hex: Option<String> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--principal" => {
                        i += 1;
                        principal = val(&raw, i).to_owned();
                        i += 1;
                    }
                    "--credential" => {
                        i += 1;
                        credential_hex = Some(val(&raw, i).to_owned());
                        i += 1;
                    }
                    _ => break,
                }
            }
            match mint_token_credential(principal, credential_hex.as_deref(), credential.clone()) {
                Ok((principal, credential)) => Command::MintToken {
                    principal,
                    credential,
                },
                Err(e) => {
                    eprintln!("autumn-op mint-token: {e}");
                    std::process::exit(2);
                }
            }
        }
        // D2: namespace registry admin
        "namespace-create" => {
            let mut name = String::new();
            let mut flags = RuleFlags::default();
            while i < raw.len() {
                if flags.take("namespace-create", &raw, &mut i) {
                    continue;
                }
                match raw[i].as_str() {
                    "--name" => {
                        i += 1;
                        name = val(&raw, i).to_owned();
                        i += 1;
                    }
                    other => {
                        eprintln!("namespace-create: unexpected argument {other:?}");
                        std::process::exit(1);
                    }
                }
            }
            if name.is_empty() {
                usage();
            }
            reject_bad_scope("namespace-create", &name);
            let force = flags.force;
            let rule = presplit_rule("namespace-create", &name, flags);
            Command::NamespaceCreate { name, rule, force }
        }
        "namespace-delete" => {
            let mut name = String::new();
            let mut force = false;
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
                    _ => break,
                }
            }
            if name.is_empty() {
                usage();
            }
            Command::NamespaceDelete {
                name,
                force,
            }
        }
        "namespace-list" => Command::NamespaceList,
        "principal-list" => Command::PrincipalList,
        // read
        "info" => {
            let mut part: Option<u64> = None;
            let mut detail = false;
            let mut full = false;
            let mut file_sd = false;
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
                    "--file-sd" => {
                        file_sd = true;
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
            if file_sd && (part.is_some() || detail || full) {
                eprintln!("--file-sd takes no other info flag");
                usage();
            }
            Command::Info { part, detail, full, file_sd }
        }
        "policy-candidates" | "policy_candidates" | "policy" => Command::PolicyCandidates,
        "overview" => Command::Overview,
        // auto-policy <status|start <name>|deactivate|upsert|delete>
        "auto-policy" | "auto_policy" => {
            let action = if i < raw.len() {
                let a = raw[i].clone();
                i += 1;
                a
            } else {
                "status".to_string()
            };
            let mut name = String::new();
            let mut switches = String::new();
            let mut interval: Option<u64> = None;
            let mut cooldown: Option<u64> = None;
            let mut max_actions: Option<u32> = None;
            let mut desc: Option<String> = None;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--arm" => {
                        eprintln!(
                            "autumn-op: --arm was removed with the observe mode: \
                             `auto-policy start <name>` runs a policy"
                        );
                        std::process::exit(2);
                    }
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
                    other if !other.starts_with('-') && name.is_empty() => name = other.to_string(),
                    _ => {}
                }
                i += 1;
            }
            Command::AutoPolicy {
                action,
                name,
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
                        // silently did nothing (the
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
                             Create and cut each namespace AFTER bootstrap instead:\n\
                             \x20 autumn-op namespace-create --name fs    --parts 6\n\
                             \x20 autumn-op namespace-create --name kvc   --parts 16\n\
                             \x20 autumn-op namespace-create --name bench --parts 8"
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
        "repair" => {
            let mut extents: Vec<u64> = Vec::new();
            let mut node: Option<u64> = None;
            let mut cancel = false;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--cancel" => {
                        cancel = true;
                        i += 1;
                    }
                    "--node" => {
                        i += 1;
                        node = Some(val(&raw, i).parse().unwrap_or_else(|_| {
                            eprintln!("--node requires a node id");
                            usage()
                        }));
                        i += 1;
                    }
                    tok => {
                        extents.push(tok.parse().unwrap_or_else(|_| {
                            eprintln!("repair: {tok:?} is not an extent id");
                            usage()
                        }));
                        i += 1;
                    }
                }
            }
            if extents.is_empty() == node.is_none() {
                eprintln!("repair takes extent ids OR --node <NODE_ID>");
                usage();
            }
            Command::Repair {
                extents,
                node,
                cancel,
            }
        }
        "scrub" => {
            let mut extents: Vec<u64> = Vec::new();
            let mut part: Option<u64> = None;
            let mut all = false;
            while i < raw.len() {
                match raw[i].as_str() {
                    "--all" => {
                        all = true;
                        i += 1;
                    }
                    "--part" => {
                        i += 1;
                        part = Some(val(&raw, i).parse().unwrap_or_else(|_| {
                            eprintln!("--part requires a partition id");
                            usage()
                        }));
                        i += 1;
                    }
                    tok => {
                        extents.push(tok.parse().unwrap_or_else(|_| {
                            eprintln!("scrub: {tok:?} is not an extent id");
                            usage()
                        }));
                        i += 1;
                    }
                }
            }
            // Exactly one scope, and the whole cluster only when asked for by
            // name — an empty argument list must not mean "everything".
            let scopes = usize::from(!extents.is_empty()) + usize::from(part.is_some()) + usize::from(all);
            if scopes != 1 {
                eprintln!("scrub takes extent ids, OR --part <PART_ID>, OR --all");
                usage();
            }
            Command::Scrub { extents, part }
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
            // (D4). The CLI's user-facing surface speaks namespace + suffix,
            // never raw prefix bytes (§3.4 CLI refinement / refinement 3). Forms:
            //   split <PART>                                    → median (legacy)
            //   split <PART> --namespace NS [--at S | --at-hex HEX]
            //        → cut at "NS/" ++ suffix; empty/omitted suffix = the
            //          namespace boundary "NS/" itself
            //   split <PART> --at-raw-hex HEX                   → admin escape
            //        hatch: raw whole key, no namespace assembly
            let mut namespace: Option<String> = None;
            let mut suffix: Option<Vec<u8>> = None; // from --at / --at-hex
            let mut raw_key: Option<Vec<u8>> = None; // from --at-raw-hex
            while i < raw.len() {
                match raw[i].as_str() {
                    "--namespace" => {
                        i += 1;
                        namespace = Some(val(&raw, i).to_owned());
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
            let point = build_split_point(namespace, suffix, raw_key);
            Command::Split { part_id, point }
        }
        "presplit" => {
            // presplit --namespace <NS[/SUB]> --parts N  (the ns picks how)
            let mut namespace: Option<String> = None;
            let mut flags = RuleFlags::default();
            while i < raw.len() {
                if flags.take("presplit", &raw, &mut i) {
                    continue;
                }
                match raw[i].as_str() {
                    "--namespace" => { i += 1; namespace = Some(val(&raw, i).to_owned()); i += 1; }
                    other => { eprintln!("presplit: unknown flag {other}"); std::process::exit(1); }
                }
            }
            let scope = namespace.unwrap_or_else(|| {
                eprintln!("presplit requires --namespace <NS[/SUB]>"); std::process::exit(1);
            });
            reject_bad_scope("presplit", &scope);
            let force = flags.force;
            if flags.parts.is_none_or(|n| n < 2) {
                eprintln!("presplit --namespace {scope} needs --parts <N>, N >= 2");
                std::process::exit(1);
            }
            let rule = presplit_rule("presplit", &scope, flags).expect("--parts >= 2 gives a rule");
            Command::Presplit { scope, rule, force }
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
            Command::Merge {
                survivor_part_id,
                victim_part_id,
                force,
            }
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
            let mut dead_bytes: Option<u64> = None;
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
                    "--dead-bytes" => {
                        i += 1;
                        dead_bytes = Some(parse_byte_size(val(&raw, i)).unwrap_or_else(|e| {
                            eprintln!("--dead-bytes: {e}");
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
            Command::Gc {
                part_id,
                ratio,
                max_size,
                stream_debt,
                dead_bytes,
                empty_only,
            }
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
                    // Format is identity-only — these
                    // location flags moved to the EN binary itself (its own
                    // `--advertise`, self-registered at every startup). Hard
                    // error (not a silent ignore) so a not-yet-updated caller
                    // fails loudly instead of format silently keying off a
                    // register that never fires. Same-commit migration
                    // pattern as `--disk-id` / `--shards`.
                    "--listen" | "--advertise" | "--shard-ports" => {
                        eprintln!(
                            "error: format's `{}` was removed — \
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
        manager: g.manager.get(),
        json: g.json.get(),
        transport: g.transport.get().into(),
        cluster_secret_file: g.cluster_secret_file.get(),
        credential,
        wait: g.wait.get(),
        wait_timeout: g.timeout.get(),
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

/// A scope segment must be a SINGLE path segment so
/// `{scope}/ ++ suffix` is 1:1 with what the operator typed. The namespace
/// convention pins the charset to `[a-z0-9._-]+` (non-empty, no `/`). Pure so it is unit-testable; `reject_bad_scope` is
/// the fail-loud wrapper.
pub(crate) fn valid_segment(s: &str) -> bool {
    !s.is_empty()
        && s.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        })
}

/// A `--namespace` scope: one or more `/`-separated segments, each valid.
pub(crate) fn valid_scope(s: &str) -> bool {
    s.split('/').all(valid_segment)
}

fn reject_bad_scope(cmd: &str, s: &str) {
    if !valid_scope(s) {
        eprintln!(
            "{cmd}: --namespace {s:?} is not a valid scope — `/`-separated \
             segments, each non-empty and matching [a-z0-9._-]+."
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
///   `--namespace` / `--at` / `--at-hex`.
/// - `--at` / `--at-hex` (the suffix) require `--namespace` (a suffix is
///   relative to a namespace; a whole raw key uses `--at-raw-hex`).
/// - Nothing → `Median`.
pub(crate) fn build_split_point(
    namespace: Option<String>,
    suffix: Option<Vec<u8>>,
    raw_key: Option<Vec<u8>>,
) -> SplitPoint {
    if let Some(bytes) = raw_key {
        if namespace.is_some() || suffix.is_some() {
            eprintln!(
                "split: --at-raw-hex is an escape hatch and cannot be combined \
                 with --namespace/--at/--at-hex"
            );
            std::process::exit(1);
        }
        return SplitPoint::Raw(bytes);
    }
    match namespace {
        Some(scope) => {
            // Each scope segment must be `[a-z0-9._-]+`, else (scope, suffix)
            // is not 1:1 with the assembled raw key.
            reject_bad_scope("split", &scope);
            SplitPoint::Namespaced {
                scope,
                // Omitted suffix = empty = cut exactly at the namespace boundary.
                suffix: suffix.unwrap_or_default(),
            }
        }
        None => {
            if suffix.is_some() {
                eprintln!(
                    "split: --at/--at-hex require --namespace <ns> (a suffix is \
                     relative to a namespace). For a whole raw key use --at-raw-hex."
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

// ── Per-namespace presplit rules ─────────────────────────────────────────────
// Presplit a `{namespace}/` keyspace by the namespace's NATURAL
// high-entropy dimension (a raw-byte uniform split is namespace-blind — after
// SD-2/SD-3 all real keys sit in the `fs/`/`kvc/`/`mem/` byte sliver, so uniform
// splitting collapses everything into a couple of partitions). Each rule returns
// SUFFIXES relative to `{namespace}/`, ASCENDING; `cmd_presplit` prepends the
// `{namespace}/` prefix to get raw wire cut points and splits the owning
// partition at each.

/// The flags that pick a presplit rule, shared by `presplit` and
/// `namespace-create`: how many partitions (`--parts`), and `--force` to
/// narrow an fs's declared stripe width.
#[derive(Default)]
struct RuleFlags {
    parts: Option<usize>,
    force: bool,
}

impl RuleFlags {
    /// Consume `raw[*i]` (and its value) when it is a rule flag.
    fn take(&mut self, cmd: &str, raw: &[String], i: &mut usize) -> bool {
        match raw[*i].as_str() {
            "--force" => {
                self.force = true;
                *i += 1;
            }
            "--parts" => {
                let v = val(raw, *i + 1);
                self.parts = Some(v.trim().parse().unwrap_or_else(|_| {
                    eprintln!("{cmd}: --parts must be a number, got {v:?}");
                    std::process::exit(1);
                }));
                *i += 2;
            }
            _ => return false,
        }
        true
    }
}

/// The rule `--parts N` gives for `scope`; its first segment picks how the N
/// partitions are cut: fs over its 24 stripe lanes (N divides 24), kvc at its
/// hash buckets (N <= 16), any other namespace uniformly over hex. No
/// `--parts` means one partition: for fs and kvc still their rule, which cuts
/// nothing but records the lane / bucket grid, so a later split lands on it;
/// `None` for any other namespace. Exits on an N that does not fit.
fn presplit_rule(cmd: &str, scope: &str, f: RuleFlags) -> Option<PresplitRule> {
    let fail = |msg: String| -> ! {
        eprintln!("{cmd}: {msg}");
        std::process::exit(1);
    };
    let n = f.parts.unwrap_or(1);
    if n == 0 {
        fail("--parts N: N must be at least 1".to_string());
    }
    match scope.split('/').next().unwrap_or_default() {
        "fs" => {
            if scope != "fs" {
                fail(format!("fs is one tree — pass fs, not {scope:?}"));
            }
            let lanes = autumn_fs::schema::DEFAULT_STRIPE_LANES;
            if n > lanes as usize || lanes as usize % n != 0 {
                fail(format!("fs --parts N: N must divide its {lanes} stripe lanes (1, 2, 3, 4, 6, 8, 12, 24), got {n}"));
            }
            Some(PresplitRule::FsLanes { lanes, parts: n as u8 })
        }
        "kvc" => {
            if scope != "kvc" {
                fail(format!("kvc cuts at kvc/<hex> — pass kvc, not {scope:?}"));
            }
            if n > 16 {
                fail(format!("kvc --parts N: N must be <= 16 (one hex char), got {n}"));
            }
            Some(PresplitRule::Kvc { count: n })
        }
        _ if n == 1 => None,
        _ => Some(PresplitRule::Hex { count: n }),
    }
}

/// Per-namespace presplit rule (key structure ⇒ split dimension).
#[derive(Debug, Clone)]
pub(crate) enum PresplitRule {
    /// `fs --parts P`: split fs's `lanes` (24) stripe lanes into P partitions,
    /// cutting at lane boundaries `[0x03][k]` (static, ino-independent).
    /// A striped large file's extent at offset `off` lands on lane
    /// `(off/unit)%N` under key `[0x03][lane][ino][off]`, so its extents spread
    /// across these N partitions → parallel write/read. Lane 0 (`[start,[0x03][1])`)
    /// also holds the 0x01/0x02 metadata + any legacy non-striped `[0x03][ino…]`.
    FsLanes { lanes: u8, parts: u8 },
    /// `kvc` = split by HASH BUCKET. The kvcache adapters store
    /// `{h}/{model}/{pool}/{hash}…` with `h` the first char of the sha256
    /// hexdigest, so the cuts are model-agnostic and can be made before any
    /// model writes. `count` (<= 16) buckets split the 16 hex digits
    /// proportionally.
    Kvc { count: usize },
    /// ANY other namespace = split by UNIFORM HEX over the 8-hex-char space, the
    /// namespace-relative replacement for the retired `bootstrap --presplit
    /// N:hexstring`. That flag cut the RAW keyspace, so under Option 3 its points
    /// (`2aaaaaaa`, `aaaaaaa8`, …) sat nowhere near a namespaced key and the
    /// presplit silently did nothing — for the benches, `bench_user_starts` kept
    /// only partitions starting with `bench/perf/` and got none, so every
    /// `--partitions N` run measured ONE partition.
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
                    "fs presplit: {parts} parts cannot exceed its {lanes} lanes \
                     (there are only {} lane boundaries to cut at)",
                    lanes - 1
                );
            }
            if lanes % parts != 0 {
                bail!(
                    "fs presplit: {parts} parts must DIVIDE its {lanes} lanes, else \
                     partitions own uneven lane runs and every striped file lands lopsided. \
                     Divisors of {lanes}: {}",
                    (1..=*lanes).filter(|p| lanes % p == 0).map(|p| p.to_string())
                        .collect::<Vec<_>>().join(", ")
                );
            }
            let step = lanes / parts;
            Ok((1..*parts).map(|k| vec![0x03u8, k * step]).collect())
        }
        PresplitRule::Kvc { count } => {
            if *count > 16 {
                bail!("kvc presplit: at most 16 buckets (one hex char), got {count}");
            }
            // Proportional boundaries over the 16 hex digits (±1 spread).
            Ok((1..(*count).max(1)).map(|i| vec![HEX_DIGITS[i * 16 / count]]).collect())
        }
        PresplitRule::Hex { count } => {
            // RELATIVE suffixes evenly spaced over the 32-bit space rendered as 8 lowercase hex
            // chars, which is the shape the bench key builder produces.
            if *count < 2 {
                return Ok(vec![]);
            }
            let span = 0xFFFF_FFFFu64 / *count as u64;
            Ok((1..*count)
                .map(|i| format!("{:08x}", span * i as u64).into_bytes())
                .collect())
        }
    }
}

/// UX-fix (M4): the FULL declared boundary grid to RECORD as sacred,
/// which for `FsLanes` is EVERY lane boundary (`lanes-1`), not just the
/// `parts-1` we physically cut this run.
///
/// Why record more than we cut: an fs `--parts 6` run creates 6
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
        // All 15 bucket boundaries, so a later split of a multi-bucket
        // partition snaps to a bucket boundary.
        PresplitRule::Kvc { .. } => presplit_suffixes(&PresplitRule::Kvc { count: 16 }),
        // Every other rule cuts exactly its declared grid, so record == cut.
        other => presplit_suffixes(other),
    }
}

/// The absolute points a presplit cuts and records for `scope`. With `head`
/// (namespace-create) the namespace's head `{ns}/` comes first, so the
/// namespace starts its own partition; the next namespace's head ends it.
pub(crate) fn presplit_plan(
    scope: &str,
    head: bool,
    rule: Option<&PresplitRule>,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>)> {
    let namespace = scope.split('/').next().unwrap_or(scope);
    let prefix = format!("{scope}/").into_bytes();
    let in_scope = |suffixes: Vec<Vec<u8>>| suffixes.into_iter().map(|s| [prefix.as_slice(), &s].concat());
    let head: Vec<Vec<u8>> = if head { vec![format!("{namespace}/").into_bytes()] } else { vec![] };
    let (mut cut, mut record) = (head.clone(), head);
    if let Some(rule) = rule {
        cut.extend(in_scope(presplit_suffixes(rule)?));
        record.extend(in_scope(presplit_record_suffixes(rule)?));
    }
    Ok((cut, record))
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
    use super::{mint_token_credential, SplitPoint};

    /// The file's credential (both named forms are parsed by the SDK reader;
    /// `parse` hands it over) fills or checks `--principal`;
    /// `--credential` hex needs `--principal`.
    #[test]
    fn mint_token_takes_the_credential_files_principal() {
        let file = || Some(("loader".to_string(), vec![0x0a, 0xff]));
        let want = Ok(("loader".to_string(), vec![0x0a, 0xff]));
        assert_eq!(mint_token_credential(String::new(), None, file()), want);
        assert_eq!(mint_token_credential("loader".into(), None, file()), want);
        assert!(mint_token_credential("other".into(), None, file()).is_err());
        let app = Ok(("app".to_string(), vec![0x12]));
        assert_eq!(mint_token_credential("app".into(), Some("12"), file()), app);
        assert_eq!(mint_token_credential("loader".into(), Some("0aff"), None), want);
        assert!(mint_token_credential(String::new(), Some("0aff"), None).is_err());
        assert!(mint_token_credential(String::new(), None, None).is_err());
        assert!(mint_token_credential("app".into(), Some(""), None).is_err());
    }

    // ── CLI split-point assembly ──────────────────────

    #[test]
    fn split_point_namespaced_with_suffix_assembles_prefix() {
        let p = SplitPoint::Namespaced {
            scope: "kvc".into(),
            suffix: b"model-a/vllm/v1/80".to_vec(),
        };
        assert_eq!(
            p.resolve_at_key().unwrap(),
            b"kvc/model-a/vllm/v1/80".to_vec()
        );
    }

    #[test]
    fn split_point_sub_scope_assembles_every_segment() {
        let p = SplitPoint::Namespaced {
            scope: "bench/perf".into(),
            suffix: b"80".to_vec(),
        };
        assert_eq!(p.resolve_at_key().unwrap(), b"bench/perf/80".to_vec());
        assert!(super::valid_scope("bench/perf"));
        assert!(!super::valid_scope("bench/"));
        assert!(!super::valid_scope("/perf"));
        assert!(!super::valid_scope("bench//perf"));
    }

    #[test]
    fn split_point_empty_suffix_cuts_at_namespace_boundary() {
        let p = SplitPoint::Namespaced {
            scope: "fs".into(),
            suffix: vec![],
        };
        assert_eq!(p.resolve_at_key().unwrap(), b"fs/".to_vec());
    }

    #[test]
    fn split_point_hex_suffix_carries_binary_bytes() {
        // --at-hex path: a binary suffix (e.g. an fs inode prefix) rides on
        // the `{ns}/` assembly.
        let suffix = super::parse_hex_key("0103ff00");
        assert_eq!(suffix, vec![0x01, 0x03, 0xff, 0x00]);
        let p = SplitPoint::Namespaced {
            scope: "fs".into(),
            suffix,
        };
        let mut expected = b"fs/".to_vec();
        expected.extend_from_slice(&[0x01, 0x03, 0xff, 0x00]);
        assert_eq!(p.resolve_at_key().unwrap(), expected);
    }

    #[test]
    fn valid_segment_enforces_single_path_segment() {
        // The segment charset must be [a-z0-9._-]+ so
        // (ns, suffix) is 1:1 with the assembled raw key.
        assert!(super::valid_segment("kvc"));
        assert!(super::valid_segment("acme"));
        assert!(super::valid_segment("model-cfg_456cf7.0"));
        // Rejected: empty, embedded '/', uppercase, whitespace, other bytes.
        assert!(!super::valid_segment(""));
        assert!(!super::valid_segment("acme/prod")); // the collision case
        assert!(!super::valid_segment("Acme"));
        assert!(!super::valid_segment("a b"));
        assert!(!super::valid_segment("a:b"));
    }

    #[test]
    fn build_split_point_classification() {
        // Nothing → median.
        assert!(matches!(
            super::build_split_point(None, None, None),
            SplitPoint::Median
        ));
        // ns, no suffix → namespace boundary (empty suffix).
        match super::build_split_point(Some("a".into()), None, None) {
            SplitPoint::Namespaced { suffix, .. } => assert!(suffix.is_empty()),
            _ => panic!("expected namespaced"),
        }
        // ns + suffix → namespaced with suffix.
        match super::build_split_point(Some("a".into()), Some(b"s".to_vec()), None) {
            SplitPoint::Namespaced { suffix, .. } => assert_eq!(suffix, b"s".to_vec()),
            _ => panic!("expected namespaced"),
        }
        // raw escape hatch alone → raw.
        assert!(matches!(
            super::build_split_point(None, None, Some(vec![1, 2, 3])),
            SplitPoint::Raw(_)
        ));
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

    // ── Per-namespace presplit rules ──────────────────────────────────────────────

    /// The full wire cut point = `{ns}/ ++ suffix`.
    fn wire(ns: &str, suffix: &[u8]) -> Vec<u8> {
        let mut k = format!("{ns}/").into_bytes();
        k.extend_from_slice(suffix);
        k
    }

    #[test]
    fn namespace_create_cuts_its_head_first() {
        use super::{presplit_plan, PresplitRule};
        let (cut, rec) = presplit_plan("fs", true, None).unwrap();
        assert_eq!((cut, rec), (vec![b"fs/".to_vec()], vec![b"fs/".to_vec()]));
        let (cut, rec) = presplit_plan("kvc", true, Some(&PresplitRule::Kvc { count: 16 })).unwrap();
        assert_eq!(cut.len(), 16);
        assert_eq!(cut[0], b"kvc/".to_vec());
        assert_eq!(cut[1], b"kvc/1".to_vec());
        assert_eq!(rec, cut);
        // A sub-scope is cut at its namespace's head, then under the scope.
        let (cut, _) = presplit_plan("bench/perf", true, Some(&PresplitRule::Hex { count: 2 })).unwrap();
        assert_eq!(cut[0], b"bench/".to_vec());
        assert!(cut[1].starts_with(b"bench/perf/"));
        assert!(cut.windows(2).all(|w| w[0] < w[1]), "ascending: {cut:?}");
        // `presplit` cuts only by its rule.
        let (cut, rec) = presplit_plan("kvc", false, Some(&PresplitRule::Kvc { count: 2 })).unwrap();
        assert_eq!(cut, vec![b"kvc/8".to_vec()]);
        assert_eq!(rec.len(), 15);
    }

    #[test]
    fn namespace_create_and_presplit_read_the_same_rule_flags() {
        use super::{presplit_rule, PresplitRule, RuleFlags};
        let flags = |args: &[&str]| {
            let raw: Vec<String> = args.iter().map(|a| a.to_string()).collect();
            let mut f = RuleFlags::default();
            let mut i = 0;
            while i < raw.len() {
                assert!(f.take("t", &raw, &mut i), "{} is a rule flag", raw[i]);
            }
            f
        };
        // One partition: fs and kvc keep their rule (no cut, the grid is
        // recorded); any other namespace has none (namespace-create cuts only
        // the head).
        assert!(matches!(presplit_rule("t", "kvc", flags(&[])), Some(PresplitRule::Kvc { count: 1 })));
        assert!(matches!(presplit_rule("t", "fs", flags(&["--parts", "1"])), Some(PresplitRule::FsLanes { lanes: 24, parts: 1 })));
        assert!(presplit_rule("t", "gallery", flags(&[])).is_none());
        assert!(presplit_rule("t", "gallery", flags(&["--parts", "1"])).is_none());
        let (cut, rec) = super::presplit_plan("kvc", true, Some(&PresplitRule::Kvc { count: 1 })).unwrap();
        assert_eq!((cut.len(), rec.len()), (1, 16), "head only cut, head + 15 buckets recorded");
        let (cut, rec) = super::presplit_plan("fs", true, Some(&PresplitRule::FsLanes { lanes: 24, parts: 1 })).unwrap();
        assert_eq!((cut.len(), rec.len()), (1, 24), "head only cut, head + 23 lanes recorded");
        assert!(matches!(presplit_rule("t", "kvc", flags(&["--parts", "16"])), Some(PresplitRule::Kvc { count: 16 })));
        assert!(matches!(presplit_rule("t", "bench", flags(&["--parts", "8"])), Some(PresplitRule::Hex { count: 8 })));
        assert!(matches!(
            presplit_rule("t", "fs", flags(&["--parts", "6"])),
            Some(PresplitRule::FsLanes { lanes: 24, parts: 6 })
        ));
        assert!(flags(&["--force"]).force);
        let raw = vec!["--count".to_string(), "4".to_string()];
        assert!(!RuleFlags::default().take("t", &raw, &mut 0), "--count is gone");
    }

    #[test]
    fn presplit_fs_lanes_cuts_at_lane_boundaries() {
        use super::PresplitRule;
        // 4 lanes → cut at [0x03][1], [0x03][2], [0x03][3] → 4 partitions.
        let s = super::presplit_suffixes(&PresplitRule::FsLanes { lanes: 4, parts: 4 }).unwrap();
        assert_eq!(s, vec![vec![0x03, 1], vec![0x03, 2], vec![0x03, 3]]);
        // A striped extent on lane 2 (wire [0x03][2][ino][off]) sorts into the
        // partition [[0x03][2], [0x03][3]).
        let ext_lane2 = wire("fs",
            &[
                0x03, 2, /*ino*/ 0, 0, 0, 0, 0, 0, 0, 9, /*off*/ 0, 0, 0, 0, 0, 0, 0, 0,
            ],
        );
        let cut2 = wire("fs", &s[1]); // [0x03][2]
        let cut3 = wire("fs", &s[2]); // [0x03][3]
        assert!(ext_lane2 >= cut2 && ext_lane2 < cut3);
        // < 2 lanes → no cut.
        assert!(super::presplit_suffixes(&PresplitRule::FsLanes { lanes: 1, parts: 1 }).unwrap().is_empty());
    }

    #[test]
    fn presplit_kvc_cuts_at_the_hash_bucket() {
        use super::{presplit_record_suffixes, PresplitRule};
        let s = super::presplit_suffixes(&PresplitRule::Kvc { count: 4 }).unwrap();
        assert_eq!(s, vec![b"4".to_vec(), b"8".to_vec(), b"c".to_vec()]);
        // A hash starting '9' lands in [8, c) for any model.
        let k9 = wire("kvc", b"9/qwen3-8b_0_1/vllm/v2/9abc/l0");
        assert!(k9 >= wire("kvc", b"8") && k9 < wire("kvc", b"c"));
        let s16 = super::presplit_suffixes(&PresplitRule::Kvc { count: 16 }).unwrap();
        assert_eq!(s16.len(), 15);
        // The full grid is recorded whatever was cut.
        assert_eq!(presplit_record_suffixes(&PresplitRule::Kvc { count: 4 }).unwrap(), s16);
        assert!(super::presplit_suffixes(&PresplitRule::Kvc { count: 1 }).unwrap().is_empty());
        assert!(super::presplit_suffixes(&PresplitRule::Kvc { count: 17 }).is_err());
    }
}

#[cfg(test)]
mod lane_parts_tests {
    use super::{presplit_suffixes, PresplitRule};

    #[test]
    fn record_grid_is_the_full_lane_grid_not_just_the_cut_grid() {
        use super::{presplit_record_suffixes, presplit_suffixes, PresplitRule};
        // 24 lanes over 6 parts: we CUT 5 points (the parts grid) …
        let cut = presplit_suffixes(&PresplitRule::FsLanes {
            lanes: 24,
            parts: 6,
        })
        .unwrap();
        assert_eq!(cut.len(), 5);
        // … but RECORD all 23 lane boundaries (M4): a partition owning lanes
        // [0,4) then holds a declared point so auto-split snaps to a lane
        // boundary instead of a median inside a lane.
        let rec = presplit_record_suffixes(&PresplitRule::FsLanes {
            lanes: 24,
            parts: 6,
        })
        .unwrap();
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
        // RELATIVE suffixes — the caller prepends `{ns}/`, which is the
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
        let s = presplit_suffixes(&PresplitRule::FsLanes {
            lanes: 24,
            parts: 4,
        })
        .unwrap();
        assert_eq!(s, vec![vec![0x03, 6], vec![0x03, 12], vec![0x03, 18]]);
        // Every divisor of 24 works and yields parts-1 cuts, evenly spaced.
        for p in [2u8, 3, 4, 6, 8, 12, 24] {
            let cuts = presplit_suffixes(&PresplitRule::FsLanes {
                lanes: 24,
                parts: p,
            })
            .unwrap();
            assert_eq!(cuts.len() as u8, p - 1, "parts={p}");
            let step = 24 / p;
            for (k, c) in cuts.iter().enumerate() {
                assert_eq!(c[1], (k as u8 + 1) * step, "parts={p} cut {k}");
            }
        }
        // A non-divisor is REJECTED, not rounded: uneven lane runs make every
        // striped file land lopsided.
        let err = presplit_suffixes(&PresplitRule::FsLanes {
            lanes: 24,
            parts: 5,
        })
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

    /// Global flags work before and after the command, and the command's
    /// own flags reach it untouched, in order.
    #[test]
    fn global_flags_stand_anywhere() {
        use autumn_params::{op::OpParams, Positional};
        let parse = |line: &str| {
            let argv: Vec<String> = line.split_whitespace().map(String::from).collect();
            let g = OpParams::default();
            match autumn_params::parse(&argv, &[&g], Positional::Anywhere) {
                Ok(autumn_params::Parsed::Rest(rest)) => (g, rest.join(" ")),
                other => panic!("{line}: {other:?}"),
            }
        };
        let f = Some(std::path::PathBuf::from("F"));
        for line in [
            "principal-create --cluster-secret-file F --principal p",
            "--cluster-secret-file F principal-create --principal p",
            "principal-create --principal p --cluster-secret-file F",
        ] {
            let (g, rest) = parse(line);
            assert_eq!((g.cluster_secret_file.get(), rest.as_str()), (f.clone(), "principal-create --principal p"));
        }
        let (g, rest) = parse("--json presplit --force --namespace fs --cluster-secret-file F");
        assert_eq!((g.json.get(), g.cluster_secret_file.get(), rest.as_str()), (true, f, "presplit --force --namespace fs"));
        let (g, rest) = parse("compact 7 --wait --timeout 30 --json");
        assert_eq!((g.wait.get(), g.timeout.get(), g.json.get(), rest.as_str()), (true, 30, true, "compact 7"));
        // The last --credential-file wins, wherever each stands.
        let (g, _) = parse("--credential-file a mint-token --credential-file b");
        assert_eq!(g.credential_file.get().as_deref(), Some("b"));
    }

    /// A global flag is taken anywhere on the line, so a command flag of the
    /// same name would never reach its command.
    #[test]
    fn no_command_flag_shadows_a_global_flag() {
        use autumn_params::Params;
        let globals: Vec<String> = autumn_params::op::OpParams::specs()
            .iter()
            .chain(autumn_params::client::ClientParams::specs())
            .map(|s| format!("\"--{}\"", s.flag()))
            .collect();
        let src = include_str!("args.rs");
        for g in &globals {
            assert!(!src.contains(g.as_str()), "a command parses {g}, which is a global flag");
        }
    }
}
