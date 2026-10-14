//! Every tunable of every autumn process, one file per role.
//!
//! A parameter is declared once, in its role's file, with its type, default,
//! valid range and help text. The binary's flag parser, its `--help`, the
//! range check and the runtime override all come from that declaration, so
//! there is no second list to drift.
//!
//! Three kinds of parameter:
//! - hard-coded: a format or protocol invariant, stays a `const` in its crate;
//! - startup: declared here, set by a CLI flag, fixed for the process lifetime;
//! - runtime: declared here with `[runtime]`, also overridable while running
//!   through `autumn-op config set` (the override wins over the flag; unset
//!   goes back to the flag's value).
//!
//! Values live in atomics (`Live`), so a reader anywhere sees an override as
//! soon as it is applied, and a struct can be shared without a lock.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

pub mod autumnfs;
pub mod client;
pub mod dashboard;
pub mod en;
pub mod fs;
pub mod fuse;
pub mod gallery;
pub mod manager;
pub mod ps;
pub mod s3;

pub const KIB: u64 = 1024;
pub const MIB: u64 = 1024 * KIB;
pub const GIB: u64 = 1024 * MIB;

/// A parameter type: parsed from a flag, stored as 64 bits.
pub trait Value: Copy + PartialOrd + std::fmt::Display + Send + Sync + 'static {
    fn parse(s: &str) -> Result<Self, String>;
    fn to_bits(self) -> u64;
    fn from_bits(bits: u64) -> Self;
}

/// Unsigned integer with an optional binary suffix: `64M`, `64MiB`, `1g`, `512k`.
fn parse_unsigned(s: &str) -> Result<u64, String> {
    let t = s.trim().replace('_', "");
    let lower = t.to_ascii_lowercase();
    let body = lower
        .strip_suffix("ib")
        .or_else(|| lower.strip_suffix('b'))
        .unwrap_or(&lower);
    let (digits, shift) = match body.chars().last() {
        Some('k') => (&body[..body.len() - 1], 10),
        Some('m') => (&body[..body.len() - 1], 20),
        Some('g') => (&body[..body.len() - 1], 30),
        Some('t') => (&body[..body.len() - 1], 40),
        _ if body.len() != lower.len() => return Err(format!("{s:?} is not a number")),
        _ => (body, 0),
    };
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("{s:?} is not a number"))?;
    n.checked_mul(1u64 << shift)
        .ok_or_else(|| format!("{s:?} overflows"))
}

macro_rules! unsigned_value {
    ($($t:ty),*) => {$(
        impl Value for $t {
            fn parse(s: &str) -> Result<Self, String> {
                let n = parse_unsigned(s)?;
                <$t>::try_from(n).map_err(|_| format!("{s:?} is too large"))
            }
            fn to_bits(self) -> u64 {
                self as u64
            }
            fn from_bits(bits: u64) -> Self {
                bits as $t
            }
        }
    )*};
}
unsigned_value!(u8, u16, u32, u64, usize);

impl Value for i64 {
    fn parse(s: &str) -> Result<Self, String> {
        s.trim().parse().map_err(|_| format!("{s:?} is not a number"))
    }
    fn to_bits(self) -> u64 {
        self as u64
    }
    fn from_bits(bits: u64) -> Self {
        bits as i64
    }
}

impl Value for f64 {
    fn parse(s: &str) -> Result<Self, String> {
        let v: f64 = s.trim().parse().map_err(|_| format!("{s:?} is not a number"))?;
        if v.is_finite() {
            Ok(v)
        } else {
            Err(format!("{s:?} is not finite"))
        }
    }
    fn to_bits(self) -> u64 {
        f64::to_bits(self)
    }
    fn from_bits(bits: u64) -> Self {
        f64::from_bits(bits)
    }
}

impl Value for bool {
    fn parse(s: &str) -> Result<Self, String> {
        match s.trim() {
            "true" | "1" | "on" => Ok(true),
            "false" | "0" | "off" => Ok(false),
            _ => Err(format!("{s:?} is not true/false")),
        }
    }
    fn to_bits(self) -> u64 {
        self as u64
    }
    fn from_bits(bits: u64) -> Self {
        bits != 0
    }
}

/// Parse `s` and check it against `[lo, hi]`.
pub fn check<T: Value>(s: &str, lo: T, hi: T) -> Result<T, String> {
    let v = T::parse(s)?;
    if v < lo || v > hi {
        return Err(format!("{v} is outside [{lo}, {hi}]"));
    }
    Ok(v)
}

/// One parameter's value. `base` is what the flag (or the default) set;
/// `cur` is what readers see: `base`, or a runtime override.
pub struct Live<T: Value> {
    cur: AtomicU64,
    base: AtomicU64,
    _t: PhantomData<fn() -> T>,
}

impl<T: Value> Live<T> {
    pub fn new(v: T) -> Self {
        Self {
            cur: AtomicU64::new(v.to_bits()),
            base: AtomicU64::new(v.to_bits()),
            _t: PhantomData,
        }
    }

    #[inline]
    pub fn get(&self) -> T {
        T::from_bits(self.cur.load(Ordering::Relaxed))
    }

    /// Set the startup value (a flag, or a test); drops any override.
    pub fn set(&self, v: T) {
        self.base.store(v.to_bits(), Ordering::Relaxed);
        self.cur.store(v.to_bits(), Ordering::Relaxed);
    }

    /// Runtime override; `None` goes back to the startup value.
    pub fn set_override(&self, v: Option<T>) {
        let bits = v.map_or_else(|| self.base.load(Ordering::Relaxed), T::to_bits);
        self.cur.store(bits, Ordering::Relaxed);
    }

    pub fn base(&self) -> T {
        T::from_bits(self.base.load(Ordering::Relaxed))
    }
}

impl<T: Value + std::fmt::Debug> std::fmt::Debug for Live<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.get(), f)
    }
}

/// The declaration of one parameter, as `--help` and `autumn-op` see it.
pub struct Spec {
    /// Field name; the flag is this with `_` → `-`.
    pub field: &'static str,
    pub help: &'static str,
    pub runtime: bool,
    pub default: fn() -> String,
    pub range: fn() -> String,
    /// Parse and range-check a value without an instance (the manager
    /// validating `config set` for a PS or EN key).
    pub check: fn(&str) -> Result<(), String>,
}

impl Spec {
    pub fn flag(&self) -> String {
        self.field.replace('_', "-")
    }
}

/// Checks that span several parameters of one struct. Every parameter struct
/// implements it (most with the empty default).
pub trait CrossCheck {
    fn cross_check(&self) -> Result<(), String> {
        Ok(())
    }
}

/// What the `params!` macro implements for every parameter struct.
pub trait Params: Sync + CrossCheck {
    /// Role name, the prefix of a runtime key (`ps.fg-rate-bytes-per-sec`).
    const ROLE: &'static str;
    fn specs() -> &'static [Spec];
    /// Set the startup value of `field`. `None` = not a field of this struct.
    fn set_str(&self, field: &str, value: &str) -> Option<Result<(), String>>;
    /// Apply (`Some`) or drop (`None`) a runtime override of a `[runtime]` field.
    fn override_str(&self, field: &str, value: Option<&str>) -> Option<Result<(), String>>;
    fn get_str(&self, field: &str) -> Option<String>;
}

/// Outcome of offering one argv entry to a parameter struct.
pub enum Flag {
    /// The flag and its value were consumed.
    Taken,
    /// Not a parameter of this struct.
    Unknown,
}

/// Offer `--flag value` to `p`. A known flag with a missing or bad value is an
/// error; the caller exits 2 on it.
pub fn take_flag<P: Params>(p: &P, arg: &str, value: Option<&str>) -> Result<Flag, String> {
    let Some(name) = arg.strip_prefix("--") else {
        return Ok(Flag::Unknown);
    };
    let field = name.replace('-', "_");
    if !P::specs().iter().any(|s| s.field == field) {
        return Ok(Flag::Unknown);
    }
    let value = value.ok_or_else(|| format!("{arg} requires a value"))?;
    match p.set_str(&field, value) {
        Some(Ok(())) => Ok(Flag::Taken),
        Some(Err(e)) => Err(format!("{arg} {value}: {e}")),
        None => Ok(Flag::Unknown),
    }
}

/// Pull every parameter flag out of `args` (for a binary whose own parser,
/// e.g. clap, runs on what is left). `take` offers one flag and its value to
/// the binary's parameter structs, usually a chain of `take_flag` calls.
pub fn split_params(
    args: Vec<String>,
    take: impl Fn(&str, Option<&str>) -> Result<Flag, String>,
) -> Result<Vec<String>, String> {
    let mut rest = Vec::with_capacity(args.len());
    let mut i = 0;
    while i < args.len() {
        match take(&args[i], args.get(i + 1).map(|s| s.as_str()))? {
            Flag::Taken => i += 2,
            Flag::Unknown => {
                rest.push(args[i].clone());
                i += 1;
            }
        }
    }
    Ok(rest)
}

/// Offer `--flag value` to several parameter structs in turn.
#[macro_export]
macro_rules! take_any {
    ($arg:expr, $value:expr, $($p:expr),+ $(,)?) => {{
        let mut out: Result<$crate::Flag, String> = Ok($crate::Flag::Unknown);
        $(
            if matches!(out, Ok($crate::Flag::Unknown)) {
                out = $crate::take_flag($p, $arg, $value);
            }
        )+
        out
    }};
}

/// The `--help` section listing every parameter of `P`.
pub fn help<P: Params>() -> String {
    let mut out = String::new();
    for s in P::specs() {
        out.push_str(&format!(
            "  --{} <v>  (default {}, range {}{})\n",
            s.flag(),
            (s.default)(),
            (s.range)(),
            if s.runtime { ", runtime" } else { "" }
        ));
        for line in s.help.lines() {
            out.push_str(&format!("        {}\n", line.trim()));
        }
    }
    out
}

/// Look up a runtime key (`<role>.<flag>`) among `P`'s specs.
pub fn runtime_spec<P: Params>(key: &str) -> Option<&'static Spec> {
    let flag = key.strip_prefix(P::ROLE)?.strip_prefix('.')?;
    // The flag spelling only: a second spelling would be a second stored row.
    P::specs().iter().find(|s| s.runtime && s.flag() == flag)
}

/// Install the full override set for `P`: keys of this role get their value,
/// every other runtime parameter of `P` goes back to its startup value.
/// Returns one message per entry that could not be applied: a key this binary
/// does not know, a value it rejects (the field keeps what it had), or a change
/// that leaves the struct failing its `CrossCheck` against the values around it.
/// Changes apply one at a time and a refused one is retried after the others,
/// since it may only fit once they are in; whatever still fails keeps its old
/// value, and the rest of the set applies.
pub fn apply_overrides<P: Params>(p: &P, entries: &[(String, String)]) -> Vec<String> {
    let mut errs = Vec::new();
    let mut seen: Vec<&'static str> = Vec::new();
    // (field, value; None = back to base, label)
    let mut steps: Vec<(&'static str, Option<&str>, String)> = Vec::new();
    for (key, value) in entries {
        let Some(flag) = key.strip_prefix(P::ROLE).and_then(|k| k.strip_prefix('.')) else {
            continue;
        };
        let Some(spec) = runtime_spec::<P>(key) else {
            errs.push(format!("{}.{flag}: not a runtime parameter", P::ROLE));
            continue;
        };
        seen.push(spec.field);
        match (spec.check)(value) {
            Ok(()) => steps.push((spec.field, Some(value), format!("{key} = {value}"))),
            Err(e) => errs.push(format!("{key} = {value}: {e}")),
        }
    }
    for s in P::specs().iter().filter(|s| s.runtime && !seen.contains(&s.field)) {
        steps.push((s.field, None, format!("{}.{} (unset)", P::ROLE, s.flag())));
    }
    loop {
        let mut refused = Vec::new();
        let before = steps.len();
        for (field, value, label) in steps {
            let Some(prev) = p.get_str(field) else { continue };
            if let Some(Err(e)) = p.override_str(field, value) {
                errs.push(format!("{label}: {e}"));
                continue;
            }
            if let Err(e) = p.cross_check() {
                // `prev` round-trips exactly; it fails only if it was never in range.
                if let Some(Err(re)) = p.override_str(field, Some(&prev)) {
                    errs.push(format!("{label}: restoring {prev}: {re}"));
                }
                refused.push((field, value, label, e));
            }
        }
        if refused.is_empty() || refused.len() == before {
            for (_, _, label, e) in refused {
                errs.push(format!("{label}: refused: {e}"));
            }
            return errs;
        }
        steps = refused.into_iter().map(|(f, v, l, _)| (f, v, l)).collect();
    }
}

/// The override set a process installed last, so a poll that brings back
/// the same entries changes nothing and logs nothing.
pub struct Installed(std::sync::Mutex<Option<Vec<(String, String)>>>);

impl Installed {
    pub const fn new() -> Self {
        Self(std::sync::Mutex::new(None))
    }

    /// Apply `entries` (every role's) to `p` if the entries of `P`'s role
    /// differ from the last install. `None` = unchanged; else this role's
    /// entries and what `apply_overrides` refused.
    pub fn install<P: Params>(
        &self,
        p: &P,
        entries: &[(String, String)],
    ) -> Option<(Vec<(String, String)>, Vec<String>)> {
        let mine: Vec<(String, String)> = entries
            .iter()
            .filter(|(k, _)| k.strip_prefix(P::ROLE).is_some_and(|r| r.starts_with('.')))
            .cloned()
            .collect();
        let mut last = self.0.lock().unwrap_or_else(|e| e.into_inner());
        // Before the first install every field already holds its base.
        if last.as_ref().map_or(mine.is_empty(), |l| *l == mine) {
            *last = Some(mine);
            return None;
        }
        let errs = apply_overrides(p, &mine);
        *last = Some(mine.clone());
        Some((mine, errs))
    }
}

impl Default for Installed {
    fn default() -> Self {
        Self::new()
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __runtime {
    () => {
        false
    };
    (runtime) => {
        true
    };
}

/// Declare a parameter struct:
///
/// ```ignore
/// params! {
///     pub struct PsParams("ps") {
///         /// Help text.
///         group_commit_cap: usize = 3072, [1, 1_000_000];
///         [runtime] fg_rate_bytes_per_sec: u64 = GIB, [0, u64::MAX];
///     }
/// }
/// ```
#[macro_export]
macro_rules! params {
    (
        $(#[$smeta:meta])*
        pub struct $S:ident ($role:literal) {
            $(
                $(#[doc = $doc:literal])*
                $([$rt:ident])? $name:ident : $ty:ty = $def:expr, [$lo:expr, $hi:expr];
            )*
        }
    ) => {
        $(#[$smeta])*
        pub struct $S {
            $(
                $(#[doc = $doc])*
                pub $name: $crate::Live<$ty>,
            )*
        }

        impl Default for $S {
            fn default() -> Self {
                Self { $($name: $crate::Live::new($def),)* }
            }
        }

        impl $crate::Params for $S {
            const ROLE: &'static str = $role;

            fn specs() -> &'static [$crate::Spec] {
                const SPECS: &[$crate::Spec] = &[$(
                    $crate::Spec {
                        field: stringify!($name),
                        help: concat!($($doc, "\n"),*),
                        runtime: $crate::__runtime!($($rt)?),
                        default: || <$ty as ::std::string::ToString>::to_string(&$def),
                        range: || format!("[{}, {}]", $lo as $ty, $hi as $ty),
                        check: |s| $crate::check::<$ty>(s, $lo, $hi).map(|_| ()),
                    },
                )*];
                SPECS
            }

            fn set_str(&self, field: &str, value: &str) -> Option<Result<(), String>> {
                match field {
                    $(stringify!($name) => Some(
                        $crate::check::<$ty>(value, $lo, $hi).map(|v| self.$name.set(v)),
                    ),)*
                    _ => None,
                }
            }

            fn override_str(&self, field: &str, value: Option<&str>) -> Option<Result<(), String>> {
                match field {
                    $(stringify!($name) if $crate::__runtime!($($rt)?) => Some(match value {
                        Some(v) => $crate::check::<$ty>(v, $lo, $hi)
                            .map(|v| self.$name.set_override(Some(v))),
                        None => {
                            self.$name.set_override(None);
                            Ok(())
                        }
                    }),)*
                    _ => None,
                }
            }

            fn get_str(&self, field: &str) -> Option<String> {
                match field {
                    $(stringify!($name) => Some(self.$name.get().to_string()),)*
                    _ => None,
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    params! {
        pub struct T("t") {
            /// A count.
            count: usize = 4, [1, 64];
            /// A rate.
            [runtime] rate_bytes_per_sec: u64 = 64 * MIB, [0, u64::MAX];
            [runtime] ratio: f64 = 0.8, [0.0, 1.0];
            [runtime] floor: u64 = 1, [0, 100];
        }
    }

    impl CrossCheck for T {
        fn cross_check(&self) -> Result<(), String> {
            if self.floor.get() > self.rate_bytes_per_sec.get() {
                return Err("floor above rate".into());
            }
            Ok(())
        }
    }

    #[test]
    fn units_parse() {
        assert_eq!(u64::parse("64M").unwrap(), 64 * MIB);
        assert_eq!(u64::parse("64MiB").unwrap(), 64 * MIB);
        assert_eq!(u64::parse("1g").unwrap(), GIB);
        assert_eq!(u64::parse("1_000").unwrap(), 1000);
        assert!(u64::parse("12x").is_err());
        assert!(u64::parse("-1").is_err());
        assert!(u32::parse("8G").is_err());
    }

    #[test]
    fn flags_set_and_range_check() {
        let t = T::default();
        assert!(matches!(take_flag(&t, "--count", Some("8")), Ok(Flag::Taken)));
        assert_eq!(t.count.get(), 8);
        assert!(take_flag(&t, "--count", Some("0")).is_err());
        assert!(take_flag(&t, "--count", None).is_err());
        assert!(matches!(take_flag(&t, "--nope", Some("1")), Ok(Flag::Unknown)));
        assert!(help::<T>().contains("--rate-bytes-per-sec"));
        let argv: Vec<String> = ["x", "--count", "9", "--mount", "/m", "--ratio", "0.5"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let rest = split_params(argv, |a, v| take_any!(a, v, &t)).unwrap();
        assert_eq!(rest, ["x", "--mount", "/m"]);
        assert_eq!((t.count.get(), t.ratio.get()), (9, 0.5));
    }

    #[test]
    fn overrides_win_and_unset_restores_the_flag() {
        let t = T::default();
        t.rate_bytes_per_sec.set(32 * MIB); // the flag
        let errs = apply_overrides(
            &t,
            &[
                ("t.rate-bytes-per-sec".into(), "8M".into()),
                ("other.x".into(), "1".into()),
            ],
        );
        assert!(errs.is_empty(), "{errs:?}");
        assert_eq!(t.rate_bytes_per_sec.get(), 8 * MIB);
        apply_overrides(&t, &[]);
        assert_eq!(t.rate_bytes_per_sec.get(), 32 * MIB);
        // Not runtime, out of range, unknown: reported, nothing changes.
        let errs = apply_overrides(
            &t,
            &[
                ("t.count".into(), "2".into()),
                ("t.ratio".into(), "2".into()),
                ("t.bogus".into(), "1".into()),
            ],
        );
        assert_eq!(errs.len(), 3, "{errs:?}");
        assert_eq!(t.count.get(), 4);
        assert_eq!(t.ratio.get(), 0.8);
        // A rejected value keeps the override already in place.
        apply_overrides(&t, &[("t.ratio".into(), "0.5".into())]);
        let errs = apply_overrides(&t, &[("t.ratio".into(), "7".into())]);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(t.ratio.get(), 0.5);
        // A change failing the cross-field check is refused; the rest applies.
        let errs = apply_overrides(
            &t,
            &[
                ("t.floor".into(), "50".into()),
                ("t.rate-bytes-per-sec".into(), "2".into()),
                ("t.ratio".into(), "0.25".into()),
            ],
        );
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("t.rate-bytes-per-sec = 2"), "{errs:?}");
        assert_eq!((t.rate_bytes_per_sec.get(), t.floor.get(), t.ratio.get()), (32 * MIB, 50, 0.25));
        // A change that fits only after a later one is retried after it.
        let errs = apply_overrides(
            &t,
            &[("t.rate-bytes-per-sec".into(), "10".into()), ("t.floor".into(), "5".into())],
        );
        assert!(errs.is_empty(), "{errs:?}");
        assert_eq!((t.rate_bytes_per_sec.get(), t.floor.get()), (10, 5));
        assert!(runtime_spec::<T>("t.count").is_none());
        assert!(runtime_spec::<T>("t.rate_bytes_per_sec").is_none(), "one spelling per key");
        assert!((runtime_spec::<T>("t.ratio").unwrap().check)("0.5").is_ok());
    }

    #[test]
    fn an_unchanged_set_is_not_installed_again() {
        let t = T::default();
        let inst = Installed::new();
        assert!(inst.install(&t, &[]).is_none(), "nothing to do before any override");
        let set = vec![
            ("t.rate-bytes-per-sec".to_string(), "100".to_string()),
            ("other.x".to_string(), "1".to_string()),
        ];
        let (mine, errs) = inst.install(&t, &set).expect("first install applies");
        assert_eq!(mine.len(), 1, "only this role's entries");
        assert!(errs.is_empty());
        assert_eq!(t.rate_bytes_per_sec.get(), 100);
        // Another role's change is not this role's change.
        let set2 = vec![set[0].clone(), ("other.x".to_string(), "2".to_string())];
        assert!(inst.install(&t, &set2).is_none());
        // Dropping the entry goes back to the base.
        assert!(inst.install(&t, &[]).is_some());
        assert_eq!(t.rate_bytes_per_sec.get(), 64 * MIB);
        assert!(inst.install(&t, &[]).is_none());
    }
}
