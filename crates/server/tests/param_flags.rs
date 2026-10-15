//! Every binary takes its tunables from `autumn-params`: `--help` lists them
//! all, and a bad value or an unknown argument exits 2 before anything starts.

use std::process::Command;

use autumn_params::Params;

const MANAGER_BIN: &str = env!("CARGO_BIN_EXE_autumn-manager-server");
const PS_BIN: &str = env!("CARGO_BIN_EXE_autumn-ps");
const EN_BIN: &str = env!("CARGO_BIN_EXE_autumn-extent-node");
const AUTUMNFS_BIN: &str = env!("CARGO_BIN_EXE_autumnfs");
const S3_BIN: &str = env!("CARGO_BIN_EXE_autumn-s3");
const DASHBOARD_BIN: &str = env!("CARGO_BIN_EXE_autumn-dashboard");
const CLIENT_BIN: &str = env!("CARGO_BIN_EXE_autumn-client");
const OP_BIN: &str = env!("CARGO_BIN_EXE_autumn-op");

fn run(bin: &str, args: &[&str]) -> (i32, String) {
    let out = Command::new(bin).args(args).output().expect("spawn");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code().unwrap_or(-1), text)
}

fn assert_help_lists_every_parameter<P: Params>(bin: &str) {
    let (code, text) = run(bin, &["--help"]);
    assert_eq!(code, 0, "{text}");
    for spec in P::specs() {
        assert!(
            text.contains(&format!("--{} ", spec.flag())),
            "--help is missing --{}:\n{text}",
            spec.flag()
        );
    }
}

#[test]
fn manager_help_lists_every_parameter() {
    assert_help_lists_every_parameter::<autumn_params::manager::ManagerParams>(MANAGER_BIN);
}

#[test]
fn manager_refuses_a_bad_value_and_an_unknown_argument() {
    let (code, text) = run(MANAGER_BIN, &["--recovery-max-global", "0"]);
    assert_eq!(code, 2, "{text}");
    assert!(text.contains("--recovery-max-global 0"), "{text}");

    let (code, text) = run(MANAGER_BIN, &["--recovery-max-global", "lots"]);
    assert_eq!(code, 2, "{text}");

    let (code, text) = run(MANAGER_BIN, &["--no-such-flag", "1"]);
    assert_eq!(code, 2, "{text}");
    assert!(text.contains("unknown argument"), "{text}");

    // Cross-field check.
    let (code, text) = run(
        MANAGER_BIN,
        &["--policy-required-buckets", "20", "--policy-window-buckets", "10"],
    );
    assert_eq!(code, 2, "{text}");
}

#[test]
fn ps_help_lists_every_parameter() {
    assert_help_lists_every_parameter::<autumn_params::ps::PsParams>(PS_BIN);
}

#[test]
fn ps_refuses_a_bad_value_an_unknown_argument_and_the_removed_flags() {
    // Used to be clamped or silently ignored.
    for args in [
        &["--psid", "1", "--conn-inflight-cap", "0"][..],
        &["--psid", "1", "--gc-batch-records", "5000"],
        &["--psid", "1", "--fg-saturated-threshold", "2"],
        &["--psid", "1", "--compact-min-files", "5", "--compact-max-files", "4"],
        &["--psid", "1", "--data", "/tmp/x"],
        &["--psid", "1", "--bind-host", "127.0.0.1"],
        &["--psid", "1", "--gc-debt-high-bytes", "1"],
        &["--psid", "1", "--min-pipeline-batch", "8"],
    ] {
        let (code, text) = run(PS_BIN, args);
        assert_eq!(code, 2, "{args:?}: {text}");
    }
}

#[test]
fn en_help_lists_every_parameter() {
    assert_help_lists_every_parameter::<autumn_params::en::EnParams>(EN_BIN);
}

#[test]
fn en_refuses_a_bad_value_and_an_unknown_argument() {
    for args in [
        &["--recovery-parallelism", "0"][..],
        &["--inflight-cap", "0"],
        &["--ec-stripe-bytes", "1k"],
        &["--no-such-flag", "1"],
        &["--disk-id", "1"],
    ] {
        let (code, text) = run(EN_BIN, args);
        assert_eq!(code, 2, "{args:?}: {text}");
    }
}

/// `--cpu-start` was removed from both servers: a `--cpuset` range says it.
#[test]
fn ps_and_en_refuse_cpu_start() {
    for (bin, args) in [
        (PS_BIN, &["--psid", "1", "--cpu-start", "4"][..]),
        (EN_BIN, &["--cpu-start", "4"]),
    ] {
        let (code, text) = run(bin, args);
        assert_eq!(code, 2, "{bin} {args:?}: {text}");
        assert!(text.contains("--cpu-start was removed"), "{bin}: {text}");
    }
}

/// The client-side binaries take their own parameters plus the shared file
/// system and client SDK ones.
#[test]
fn client_side_help_lists_every_parameter() {
    use autumn_params::{autumnfs::AutumnfsParams, client::ClientParams, dashboard::DashboardParams};
    use autumn_params::{fs::FsParams, s3::S3Params};
    assert_help_lists_every_parameter::<AutumnfsParams>(AUTUMNFS_BIN);
    assert_help_lists_every_parameter::<FsParams>(AUTUMNFS_BIN);
    assert_help_lists_every_parameter::<ClientParams>(AUTUMNFS_BIN);
    assert_help_lists_every_parameter::<S3Params>(S3_BIN);
    assert_help_lists_every_parameter::<FsParams>(S3_BIN);
    assert_help_lists_every_parameter::<ClientParams>(S3_BIN);
    assert_help_lists_every_parameter::<DashboardParams>(DASHBOARD_BIN);
}

#[test]
fn client_side_binaries_refuse_a_bad_value() {
    for (bin, args) in [
        (AUTUMNFS_BIN, &["--put-depth", "0", "ls"][..]),
        (AUTUMNFS_BIN, &["--put-depth-striped-min", "50", "--put-depth-striped-max", "40", "ls"]),
        (AUTUMNFS_BIN, &["--write-buf-extents", "0", "ls"]),
        (S3_BIN, &["--manager", "x", "--pin-linger-ms", "lots"]),
        (DASHBOARD_BIN, &["--metrics-interval-secs", "0"]),
        (CLIENT_BIN, &["--rpc-timeout-ms", "1", "get", "k"]),
    ] {
        let (code, text) = run(bin, args);
        assert_eq!(code, 2, "{bin} {args:?}: {text}");
    }
}

/// `autumn-op config keys` lists exactly the runtime parameters, offline.
#[test]
fn config_keys_lists_every_runtime_parameter() {
    fn keys<P: Params>() -> Vec<String> {
        P::specs()
            .iter()
            .filter(|s| s.runtime)
            .map(|s| format!("{}.{}", P::ROLE, s.flag()))
            .collect()
    }
    let (code, text) = run(OP_BIN, &["--manager", "127.0.0.1:1", "config", "keys"]);
    assert_eq!(code, 0, "{text}");
    let mut want = keys::<autumn_params::manager::ManagerParams>();
    want.extend(keys::<autumn_params::ps::PsParams>());
    want.extend(keys::<autumn_params::en::EnParams>());
    let listed: Vec<&str> = text
        .lines()
        .filter(|l| !l.starts_with(' '))
        .filter_map(|l| l.split_whitespace().next())
        .collect();
    assert_eq!(listed, want, "{text}");
}

const MIGRATE_BIN: &str = env!("CARGO_BIN_EXE_migratev1_v2");

/// The CLIs' own parameter structs (the servers' are checked above): their
/// `--help` lists every flag the parser takes.
#[test]
fn cli_help_lists_every_parameter() {
    assert_help_lists_every_parameter::<autumn_params::op::OpParams>(OP_BIN);
    assert_help_lists_every_parameter::<autumn_params::client_cli::ClientCliParams>(CLIENT_BIN);
    assert_help_lists_every_parameter::<autumn_params::migrate::MigrateParams>(MIGRATE_BIN);
}

/// A bad startup argument exits 2 before anything starts, on every binary
/// (a non-numeric `--port` used to panic).
#[test]
fn bad_startup_arguments_exit_2() {
    for (bin, args, want) in [
        (MANAGER_BIN, &["--port", "x"][..], "--port x"),
        (MANAGER_BIN, &["--transport", "rdma"], "not tcp or ucx"),
        (MANAGER_BIN, &["--etcd", "http://e:2379"], "--manager-id"),
        (MANAGER_BIN, &["--metrics-port", "1"], "the manager serves no /metrics"),
        (PS_BIN, &["--port", "9201"], "--psid is required"),
        (PS_BIN, &["--psid", "0"], "--psid 0"),
        (PS_BIN, &["--psid", "1", "--cpuset", "3-1"], "hi<lo"),
        (PS_BIN, &["--psid", "1", "--metrics-port", "70000"], "--metrics-port 70000"),
        (EN_BIN, &["--port", "9101"], "--data is required"),
        (EN_BIN, &["--data", "/x", "--shard-stride", "0"], "--shard-stride 0"),
        (EN_BIN, &["--data", "/x", "--shards", "2"], "--shards was removed"),
        (S3_BIN, &["--port", "9000"], "--manager is required"),
        (S3_BIN, &["--manager", ""], "--manager needs a non-empty value"),
        (DASHBOARD_BIN, &["--port", "1"], "--cluster-secret-file is required"),
        (AUTUMNFS_BIN, &["frob"], "unknown command"),
        (AUTUMNFS_BIN, &["--direct-read", "maybe", "ls"], "--direct-read maybe"),
        (CLIENT_BIN, &["--transport", "x", "get", "k"], "not tcp or ucx"),
        (OP_BIN, &["status", "--timeout", "soon"], "--timeout soon"),
        (OP_BIN, &["--admin-token", "t", "status"], "--admin-token was removed"),
        (MIGRATE_BIN, &["--dry-run"], "--etcd is required"),
        (MIGRATE_BIN, &["--etcd", "e", "--bogus"], "unknown argument"),
    ] {
        let (code, text) = run(bin, args);
        assert_eq!(code, 2, "{bin} {args:?}: {text}");
        assert!(text.contains(want), "{bin} {args:?}: {text}");
    }
}

/// `--scope` stays an accepted spelling of autumn-client's `--namespace`.
#[test]
fn client_accepts_scope_for_namespace() {
    let (code, text) = run(CLIENT_BIN, &["--scope", "x", "--transport", "rdma", "get", "k"]);
    assert_eq!(code, 2, "{text}");
    assert!(text.contains("not tcp or ucx"), "--scope was not taken as a flag: {text}");
}
