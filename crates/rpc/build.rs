//! WIRE-1: compile a fingerprint of the wire schema into the binary.
//!
//! autumn-rs deploys are same-commit (rkyv has no cross-version
//! compatibility); a mixed-version cluster fails SILENTLY with garbage
//! decodes (once, a stale python wheel decoded PutReq with part_id=0 and
//! every write failed with nothing pointing at the cause). Every long-lived
//! process cross-checks this fingerprint against the manager at startup and
//! refuses to join on mismatch — loud, immediate, at the right layer.
//!
//! ── What is hashed, and why it is no longer the file bytes ────────────────
//! This used to hash the schema files BYTE FOR BYTE, comments included. That
//! made the fingerprint fire on changes that cannot affect decoding, and the
//! cost was not merely annoyance:
//!
//!   * translating one Chinese comment in `manager_rpc.rs` split a live
//!     cluster mid-rollout;
//!   * a doc comment added to `ExtentHealth.unhealthy` moved the fingerprint
//!     out from under an image that had already been built;
//!   * two prose edits during one afternoon's work each forced a registry
//!     refresh with no schema change behind it.
//!
//! The mitigation was a comment telling developers to leave the prose alone —
//! a rule people must remember, guarding a mechanical property. It failed
//! three times. Worse, an alarm that fires on comment edits teaches the reflex
//! "refresh the recorded hash and move on", which is precisely how a REAL
//! schema change would get waved through. An over-sensitive check does not
//! merely cost time; it spends the credibility the check runs on.
//!
//! So: parse each file as Rust and hash its TOKENS, with doc attributes
//! removed. Ordinary comments are not tokens and disappear on their own;
//! whitespace and formatting normalise away; everything else — every type,
//! field, field order, constant value, attribute — still lands in the hash.
//!
//! The rule chosen deliberately is "strip only what provably cannot affect the
//! wire, keep everything else", NOT "hash the items that look wire-relevant".
//! Selecting relevant items is a judgement call, and a wrong judgement here
//! fails toward a fingerprint that does NOT move when the layout did — silent
//! garbage decode, the exact failure this file exists to prevent. Being too
//! sensitive costs a refresh; being not sensitive enough costs a corrupted
//! cluster, so the asymmetry decides the design.
//!
//! LIMIT, stated plainly: this hashes the SOURCE FORM of the schema, not the
//! archived layout rkyv actually produces. An rkyv version bump, or a type
//! alias that starts resolving elsewhere, can change the layout without
//! changing these tokens. Closing that needs a layout descriptor derived from
//! the `Archived` types themselves, which is a larger piece of work.

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Drop `#[doc = "..."]` / `#![doc = "..."]` (what a `///` or `//!` comment
/// becomes) anywhere they appear, at any nesting depth.
fn strip_doc_attrs(ts: TokenStream) -> TokenStream {
    let mut out: Vec<TokenTree> = Vec::new();
    let mut it = ts.into_iter().peekable();
    while let Some(tt) = it.next() {
        // A doc attribute is `#` (optionally `!`) followed by a bracket group
        // whose first token is the identifier `doc`.
        if let TokenTree::Punct(ref p) = tt {
            if p.as_char() == '#' {
                let mut lookahead = it.clone();
                // optional inner-attribute bang
                if let Some(TokenTree::Punct(bang)) = lookahead.peek() {
                    if bang.as_char() == '!' {
                        lookahead.next();
                    }
                }
                if let Some(TokenTree::Group(g)) = lookahead.peek() {
                    if g.delimiter() == Delimiter::Bracket {
                        let first = g.stream().into_iter().next();
                        if matches!(first, Some(TokenTree::Ident(ref i)) if i == "doc") {
                            // Consume what we looked ahead over and emit
                            // nothing for the whole attribute.
                            it = lookahead;
                            it.next();
                            continue;
                        }
                    }
                }
            }
        }
        // Recurse into groups so nested items are covered too.
        if let TokenTree::Group(g) = tt {
            let inner = strip_doc_attrs(g.stream());
            let mut ng = proc_macro2::Group::new(g.delimiter(), inner);
            ng.set_span(proc_macro2::Span::call_site());
            out.push(TokenTree::Group(ng));
            continue;
        }
        out.push(tt);
    }
    out.into_iter().collect()
}

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
    let mut h = DefaultHasher::new();
    for f in &files {
        println!("cargo:rerun-if-changed={f}");
        let src = std::fs::read_to_string(f).unwrap_or_else(|e| panic!("read {f}: {e}"));
        // Parse rather than lex: a file that stops being valid Rust must fail
        // the build here with a clear message, not silently hash a token soup.
        let parsed: syn::File =
            syn::parse_file(&src).unwrap_or_else(|e| panic!("parse {f} as Rust: {e}"));
        let tokens = strip_doc_attrs(quote::ToTokens::to_token_stream(&parsed));
        tokens.to_string().hash(&mut h);
    }
    println!("cargo:rustc-env=AUTUMN_WIRE_FINGERPRINT={:016x}", h.finish());
}
