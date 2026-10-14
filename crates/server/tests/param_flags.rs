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
