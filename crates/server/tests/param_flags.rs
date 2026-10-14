//! Every binary takes its tunables from `autumn-params`: `--help` lists them
//! all, and a bad value or an unknown argument exits 2 before anything starts.

use std::process::Command;

use autumn_params::Params;

const MANAGER_BIN: &str = env!("CARGO_BIN_EXE_autumn-manager-server");

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
