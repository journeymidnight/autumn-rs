//! autumn-fuse takes its tunables from `autumn-params`: `--help` lists them and
//! a bad value exits 2 before anything is mounted.

use std::process::Command;

use autumn_params::Params;

const BIN: &str = env!("CARGO_BIN_EXE_autumn-fuse");

#[test]
fn help_lists_every_parameter_and_a_bad_value_exits_2() {
    let out = Command::new(BIN).arg("--help").output().expect("spawn");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{text}");
    let flags = autumn_params::fuse::FuseParams::specs()
        .iter()
        .chain(autumn_params::fs::FsParams::specs())
        .chain(autumn_params::client::ClientParams::specs());
    for spec in flags {
        assert!(text.contains(&format!("--{} ", spec.flag())), "missing --{}", spec.flag());
    }
    for args in [
        &["--mountpoint", "/nonexistent", "--read-io-threads", "999"][..],
        // A window below one block would panic the prefetch `clamp` mid-read.
        &["--mountpoint", "/nonexistent", "--prefetch-block-bytes", "8M", "--prefetch-window-bytes", "4M"],
        // 3/4 of it is the u16 congestion threshold.
        &["--mountpoint", "/nonexistent", "--max-background", "40000"],
    ] {
        let out = Command::new(BIN).args(args).output().expect("spawn");
        assert_eq!(out.status.code(), Some(2), "{args:?}: {}", String::from_utf8_lossy(&out.stderr));
    }
}
