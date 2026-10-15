//! gallery reads no environment variable: its scope, credential and listen
//! address are flags, its tunables come from `autumn-params`.

use std::process::Command;

use autumn_params::Params;

const BIN: &str = env!("CARGO_BIN_EXE_gallery");

#[test]
fn help_lists_every_parameter_and_bad_input_exits_2() {
    let out = Command::new(BIN).arg("--help").output().expect("spawn");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{text}");
    for spec in autumn_params::gallery::GalleryParams::specs()
        .iter()
        .chain(autumn_params::client::ClientParams::specs())
    {
        assert!(text.contains(&format!("--{} ", spec.flag())), "missing --{}", spec.flag());
    }
    for f in ["--manager <v>", "--scope <v>", "--credential-file <path>", "--listen <v>", "--port <n>"] {
        assert!(text.contains(f), "missing {f}:\n{text}");
    }
    for args in [
        &["--thumb-quality", "0"][..],
        &["--hls-time-min-secs", "30", "--hls-time-max-secs", "20"],
        &["127.0.0.1:9001"],
        &["--no-such-flag", "1"],
        &["--port", "x"],
    ] {
        let out = Command::new(BIN).args(args).output().expect("spawn");
        assert_eq!(out.status.code(), Some(2), "{args:?}: {}", String::from_utf8_lossy(&out.stderr));
    }
}
