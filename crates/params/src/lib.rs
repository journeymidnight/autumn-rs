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

pub mod en;
pub mod manager;
pub mod ps;

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
unsigned_value!(u16, u32, u64, usize);

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

/// What the `params!` macro implements for every parameter struct.
pub trait Params: Sync {
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
    let field = flag.replace('-', "_");
    P::specs().iter().find(|s| s.runtime && s.field == field)
}

/// Install the full override set for `P`: keys of this role get their value,
/// every other runtime parameter of `P` goes back to its startup value.
/// Returns one message per entry that could not be applied (a key this binary
/// does not know, or a value it rejects); the rest still apply, and a field
/// whose value is rejected keeps what it had.
pub fn apply_overrides<P: Params>(p: &P, entries: &[(String, String)]) -> Vec<String> {
    let mut errs = Vec::new();
    let mut seen: Vec<&'static str> = Vec::new();
    for (key, value) in entries {
        let Some(flag) = key.strip_prefix(P::ROLE).and_then(|k| k.strip_prefix('.')) else {
            continue;
        };
        match runtime_spec::<P>(key) {
            Some(spec) => match p.override_str(spec.field, Some(value)) {
                Some(Ok(())) => seen.push(spec.field),
                Some(Err(e)) => {
                    seen.push(spec.field);
                    errs.push(format!("{key} = {value}: {e}"));
                }
                None => errs.push(format!("{key}: not a runtime parameter")),
            },
            None => errs.push(format!("{}.{flag}: not a runtime parameter", P::ROLE)),
        }
    }
    for s in P::specs().iter().filter(|s| s.runtime) {
        if !seen.contains(&s.field) {
            p.override_str(s.field, None);
        }
    }
    errs
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
        assert!(runtime_spec::<T>("t.count").is_none());
        assert!((runtime_spec::<T>("t.ratio").unwrap().check)("0.5").is_ok());
    }
}
