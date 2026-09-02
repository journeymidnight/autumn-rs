//! WIRE-1: compile a fingerprint of the wire-schema SOURCE files into the
//! binary. autumn-rs deploys are same-commit (rkyv has no cross-version
//! compatibility); a mixed-version cluster fails SILENTLY with garbage
//! decodes (once, a stale python wheel decoded PutReq with part_id=0 and
//! every write failed with nothing pointing at the cause). Every long-lived
//! process cross-checks this fingerprint against the manager at startup and
//! refuses to join on mismatch — loud, immediate, at the right layer.
//!
//! Hashing the SCHEMA SOURCE (not the git commit) keeps dev flows sane:
//! unrelated code changes don't perturb the fingerprint; any edit to the
//! wire structs does.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn main() {
    // All three wire schemas now live in this crate (extent_rpc relocated
    // from autumn-stream when the wire schemas were unified).
    let files = [
        "src/manager_rpc.rs",
        "src/partition_rpc.rs",
        "src/frame.rs",
        "src/extent_rpc.rs",
        // the capability-token layout is wire schema (exchanged over
        // MINT_TOKEN + AUTH_HELLO). Hash it so any CapClaims change bumps the
        // fingerprint, exactly like the other wire-schema files.
        "src/cap_token.rs",
    ];
    // These files are hashed BYTE FOR BYTE, comments included. A pure-cosmetic
    // edit to one of them — reformatting, a typo fix, translating a comment —
    // changes WIRE_FINGERPRINT and makes the build reject every already-deployed
    // peer with "wire-version fraud", even though the schema is identical. That
    // is not hypothetical: translating one Chinese comment in manager_rpc.rs
    // split a live cluster mid-rollout. Touch these files only for real schema
    // changes; leave their prose alone.
    let mut h = DefaultHasher::new();
    for f in &files {
        println!("cargo:rerun-if-changed={f}");
        let content = std::fs::read(f).unwrap_or_else(|e| panic!("read {f}: {e}"));
        content.hash(&mut h);
    }
    println!("cargo:rustc-env=AUTUMN_WIRE_FINGERPRINT={:016x}", h.finish());
}
