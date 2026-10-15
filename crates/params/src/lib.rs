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
//!
//! Not only tunables: a binary's ports, addresses, paths, ids and switches
//! are parameters too (startup-only, kept in a `Fixed` when they do not fit
//! in 64 bits). `parse_or_exit` reads argv into them (`args.rs`).

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

mod args;
pub mod autumnfs;
pub mod client;
pub mod client_cli;
pub mod dashboard;
pub mod en;
pub mod fs;
pub mod fuse;
pub mod gallery;
pub mod manager;
pub mod migrate;
pub mod op;
pub mod ps;
pub mod s3;

pub use args::{
    help_text, parse, parse_cpuset, parse_or_exit, ArgValue, CpuSet, Flag, Parsed, Positional,
    Section, Transport,
};

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

/// `v` within `[lo, hi]`.
pub fn in_range<T: PartialOrd + std::fmt::Display>(v: &T, lo: T, hi: T) -> Result<(), String> {
    if *v < lo || *v > hi {
        return Err(format!("{v} is outside [{lo}, {hi}]"));
    }
    Ok(())
}

/// A numeric parameter. `base` is what the flag (or the default) set; `cur`
/// is what readers see: `base`, or a runtime override.
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

/// A parameter that does not fit in 64 bits (a string, a path, a list): set
/// while the flags are parsed, never overridden at runtime.
pub struct Fixed<T>(RwLock<T>);

impl<T: Clone> Fixed<T> {
    pub fn new(v: T) -> Self {
        Self(RwLock::new(v))
    }

    pub fn get(&self) -> T {
        self.0.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn set(&self, v: T) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = v;
    }
}

impl<T: Clone + std::fmt::Debug> std::fmt::Debug for Fixed<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.get(), f)
    }
}

/// How the `params!` macro stores and reads a field, whatever its cell.
pub trait Slot<T>: Send + Sync {
    fn new_slot(v: T) -> Self;
    fn load(&self) -> T;
    fn store(&self, v: T);
}

impl<T: Value> Slot<T> for Live<T> {
    fn new_slot(v: T) -> Self {
        Live::new(v)
    }
    fn load(&self) -> T {
        self.get()
    }
    fn store(&self, v: T) {
        self.set(v)
    }
}

impl<T: Clone + Send + Sync> Slot<T> for Fixed<T> {
    fn new_slot(v: T) -> Self {
        Fixed::new(v)
    }
    fn load(&self) -> T {
        self.get()
    }
    fn store(&self, v: T) {
        self.set(v)
    }
}

/// A parameter type and the cell it lives in: numbers and `bool` in a `Live`
/// (the only kind a `[runtime]` field can be), the rest in a `Fixed`.
pub trait Param: ArgValue + Default + Clone + Send + Sync + 'static {
    type Cell: Slot<Self>;
}

macro_rules! live_param {
    ($($t:ty),*) => {$(impl Param for $t { type Cell = Live<$t>; })*};
}
live_param!(u8, u16, u32, u64, usize, i64, f64, bool);

macro_rules! fixed_param {
    ($($t:ty),*) => {$(impl Param for $t { type Cell = Fixed<$t>; })*};
}
fixed_param!(String, std::path::PathBuf, Transport, CpuSet);

impl<T: ArgValue + Default + Clone + Send + Sync + 'static> Param for Option<T> {
    type Cell = Fixed<Option<T>>;
}

impl<T: ArgValue + Default + Clone + Send + Sync + 'static> Param for Vec<T> {
    type Cell = Fixed<Vec<T>>;
}

/// The declaration of one parameter, as `--help` and `autumn-op` see it.
pub struct Spec {
    /// Field name; the flag is this with `_` → `-`.
    pub field: &'static str,
    pub help: &'static str,
    pub runtime: bool,
    /// Must be given on the command line, with a non-empty value.
    pub required: bool,
    /// Takes no value: the flag alone sets it.
    pub switch: bool,
    /// Placeholder for the value in `--help`.
    pub meta: fn() -> &'static str,
    /// "" when there is nothing to show.
    pub default: fn() -> String,
    /// "" for a field without a range.
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
    /// Retired flags (dash spelling, no `--`) and the hint refusing each.
    fn removed() -> &'static [(&'static str, &'static str)];
    /// Older spellings: (old, current), dash spelling, no `--`.
    fn aliases() -> &'static [(&'static str, &'static str)];
    /// Set the startup value of `field`. `None` = not a field of this struct.
    fn set_str(&self, field: &str, value: &str) -> Option<Result<(), String>>;
    /// Apply (`Some`) or drop (`None`) a runtime override of a `[runtime]` field.
    fn override_str(&self, field: &str, value: Option<&str>) -> Option<Result<(), String>>;
    fn get_str(&self, field: &str) -> Option<String>;
}

/// The current spelling of `name` (an alias resolved).
fn canonical<P: Params>(name: &str) -> &str {
    P::aliases().iter().find(|(old, _)| *old == name).map_or(name, |(_, new)| new)
}

/// Offer `--flag` and the token after it to `p`. A retired flag, or a known
/// flag with a missing or bad value, is an error; the caller exits 2 on it.
pub fn take_flag<P: Params>(p: &P, arg: &str, value: Option<&str>) -> Result<Flag, String> {
    let Some(name) = arg.strip_prefix("--") else {
        return Ok(Flag::Unknown);
    };
    if let Some((_, why)) = P::removed().iter().find(|(old, _)| *old == name) {
        return Err(why.to_string());
    }
    let field = canonical::<P>(name).replace('-', "_");
    let Some(spec) = P::specs().iter().find(|s| s.field == field) else {
        return Ok(Flag::Unknown);
    };
    if spec.switch {
        return match p.set_str(&field, "true") {
            Some(Err(e)) => Err(format!("{arg}: {e}")),
            _ => Ok(Flag::Switch),
        };
    }
    let value = value.ok_or_else(|| format!("{arg} requires a value"))?;
    match p.set_str(&field, value) {
        Some(Ok(())) => Ok(Flag::Value),
        Some(Err(e)) => Err(format!("{arg} {value}: {e}")),
        None => Ok(Flag::Unknown),
    }
}

/// After the last flag: every `[required]` field was given a non-empty
/// value, then the cross-field checks. `given` holds the flags taken, as typed.
pub fn finish<P: Params>(p: &P, given: &[String]) -> Result<(), String> {
    for s in P::specs().iter().filter(|s| s.required) {
        let flag = s.flag();
        if !given.iter().any(|g| canonical::<P>(g) == flag) {
            return Err(format!("--{flag} is required"));
        }
        if p.get_str(s.field).is_none_or(|v| v.is_empty()) {
            return Err(format!("--{flag} needs a non-empty value"));
        }
    }
    p.cross_check()
}

/// The `--help` section listing every parameter of `P`.
pub fn help<P: Params>() -> String {
    let mut out = String::new();
    for s in P::specs() {
        let mut notes: Vec<String> = Vec::new();
        if s.required {
            notes.push("required".into());
        } else if s.switch {
            notes.push("switch".into());
        } else if !(s.default)().is_empty() {
            notes.push(format!("default {}", (s.default)()));
        }
        if !(s.range)().is_empty() {
            notes.push(format!("range {}", (s.range)()));
        }
        if s.runtime {
            notes.push("runtime".into());
        }
        out.push_str(&format!("  --{}", s.flag()));
        if !s.switch {
            out.push_str(&format!(" {}", (s.meta)()));
        }
        if !notes.is_empty() {
            out.push_str(&format!("  ({})", notes.join(", ")));
        }
        out.push('\n');
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
macro_rules! __is {
    ($want:ident;) => {
        false
    };
    (runtime; runtime) => {
        true
    };
    (required; required) => {
        true
    };
    (switch; switch) => {
        true
    };
    ($want:ident; $kind:ident) => {
        false
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __default {
    ($ty:ty) => {
        <$ty as ::core::default::Default>::default()
    };
    ($ty:ty, $def:expr) => {
        $def
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __range {
    ($ty:ty) => {
        String::new()
    };
    ($ty:ty, $lo:expr, $hi:expr) => {
        format!("[{}, {}]", $lo as $ty, $hi as $ty)
    };
}

/// A runtime override of field `$name`; `None` (not overridable) unless the
/// field is `[runtime]`, which only a `Live` (numeric) field can be.
#[doc(hidden)]
#[macro_export]
macro_rules! __override {
    ([runtime], $self:ident, $name:ident, $ty:ty, $value:ident, [$($lo:expr, $hi:expr)?]) => {
        Some(match $value {
            Some(v) => (|| {
                let x = <$ty as $crate::Value>::parse(v)?;
                $($crate::in_range::<$ty>(&x, $lo, $hi)?;)?
                $self.$name.set_override(Some(x));
                Ok(())
            })(),
            None => {
                $self.$name.set_override(None);
                Ok(())
            }
        })
    };
    ([$($kind:ident)?], $self:ident, $name:ident, $ty:ty, $value:ident, [$($lo:expr, $hi:expr)?]) => {
        None
    };
}

/// Declare a parameter struct:
///
/// ```ignore
/// params! {
///     pub struct PsParams("ps") {
///         /// Partition server id.
///         [required] psid: u64, [1, u64::MAX];
///         /// Manager endpoints.
///         manager: String = "127.0.0.1:9001".into();
///         /// Help text.
///         group_commit_cap: usize = 3072, [1, 1_000_000];
///         [runtime] fg_rate_bytes_per_sec: u64 = GIB, [0, u64::MAX];
///         [switch] policy_fast_mode: bool;
///     }
///     removed { "cpu-start" => "--cpu-start was removed; pass --cpuset" }
///     aliases { "scope" => "namespace" }
/// }
/// ```
///
/// Flag = field name with `_` → `-`. Every field is a startup flag;
/// `[runtime]` (numbers and `bool` only) may also be overridden while the
/// process runs. `[required]` must be given, `[switch]` takes no value. A
/// field without a default starts at `Default::default()`.
#[macro_export]
macro_rules! params {
    (
        $(#[$smeta:meta])*
        pub struct $S:ident ($role:literal) {
            $(
                $(#[doc = $doc:literal])*
                $([$kind:ident])? $name:ident : $ty:ty $(= $def:expr)? $(, [$lo:expr, $hi:expr])? ;
            )*
        }
        $(removed { $($old:literal => $why:literal),* $(,)? })?
        $(aliases { $($alias:literal => $canon:literal),* $(,)? })?
    ) => {
        $(#[$smeta])*
        pub struct $S {
            $(
                $(#[doc = $doc])*
                pub $name: <$ty as $crate::Param>::Cell,
            )*
        }

        impl Default for $S {
            fn default() -> Self {
                Self {
                    $($name: {
                        let d: $ty = $crate::__default!($ty $(, $def)?);
                        <<$ty as $crate::Param>::Cell as $crate::Slot<$ty>>::new_slot(d)
                    },)*
                }
            }
        }

        impl $crate::Params for $S {
            const ROLE: &'static str = $role;

            fn specs() -> &'static [$crate::Spec] {
                const SPECS: &[$crate::Spec] = &[$(
                    $crate::Spec {
                        field: stringify!($name),
                        help: concat!($($doc, "\n"),*),
                        runtime: $crate::__is!(runtime; $($kind)?),
                        required: $crate::__is!(required; $($kind)?),
                        switch: $crate::__is!(switch; $($kind)?),
                        meta: <$ty as $crate::ArgValue>::meta,
                        default: || {
                            let d: $ty = $crate::__default!($ty $(, $def)?);
                            $crate::ArgValue::show(&d)
                        },
                        range: || $crate::__range!($ty $(, $lo, $hi)?),
                        check: |s| {
                            let mut v: $ty = ::core::default::Default::default();
                            $crate::ArgValue::set(&mut v, s)?;
                            $($crate::in_range::<$ty>(&v, $lo, $hi)?;)?
                            Ok(())
                        },
                    },
                )*];
                SPECS
            }

            fn removed() -> &'static [(&'static str, &'static str)] {
                &[$($(($old, $why)),*)?]
            }

            fn aliases() -> &'static [(&'static str, &'static str)] {
                &[$($(($alias, $canon)),*)?]
            }

            fn set_str(&self, field: &str, value: &str) -> Option<Result<(), String>> {
                match field {
                    $(stringify!($name) => Some((|| {
                        let mut v: $ty = $crate::Slot::load(&self.$name);
                        $crate::ArgValue::set(&mut v, value)?;
                        $($crate::in_range::<$ty>(&v, $lo, $hi)?;)?
                        $crate::Slot::store(&self.$name, v);
                        Ok(())
                    })()),)*
                    _ => None,
                }
            }

            // `value` is unused in a struct without a `[runtime]` field.
            #[allow(unused_variables)]
            fn override_str(&self, field: &str, value: Option<&str>) -> Option<Result<(), String>> {
                match field {
                    $(stringify!($name) => $crate::__override!(
                        [$($kind)?], self, $name, $ty, value, [$($lo, $hi)?]
                    ),)*
                    _ => None,
                }
            }

            fn get_str(&self, field: &str) -> Option<String> {
                match field {
                    $(stringify!($name) => Some($crate::ArgValue::show(
                        &$crate::Slot::<$ty>::load(&self.$name),
                    )),)*
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
        assert_eq!(take_flag(&t, "--count", Some("8")), Ok(Flag::Value));
        assert_eq!(t.count.get(), 8);
        assert!(take_flag(&t, "--count", Some("0")).is_err());
        assert!(take_flag(&t, "--count", None).is_err());
        assert_eq!(take_flag(&t, "--nope", Some("1")), Ok(Flag::Unknown));
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
