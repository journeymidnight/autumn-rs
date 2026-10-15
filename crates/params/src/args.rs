//! Flag value types and the one argv parser every binary uses.
//!
//! A binary hands `parse_or_exit` its `params!` structs; each is a `Section`.
//! One pass over argv offers every flag to the sections in turn. `--help`
//! prints every section; an unknown flag, a bad value or a missing required
//! flag exits 2.

use std::path::PathBuf;

use crate::{Params, Value};

/// A parameter's type: what one flag value does to the field.
pub trait ArgValue {
    /// Take one flag value. A list appends; everything else replaces.
    fn set(&mut self, s: &str) -> Result<(), String>;
    /// The value as `--help` shows a default ("" = shown as no default).
    fn show(&self) -> String;
    /// Placeholder for the value in `--help`.
    fn meta() -> &'static str {
        "<v>"
    }
}

macro_rules! value_arg {
    ($meta:literal: $($t:ty),*) => {$(
        impl ArgValue for $t {
            fn set(&mut self, s: &str) -> Result<(), String> {
                *self = <$t as Value>::parse(s)?;
                Ok(())
            }
            fn show(&self) -> String {
                self.to_string()
            }
            fn meta() -> &'static str {
                $meta
            }
        }
    )*};
}
value_arg!("<n>": u8, u16, u32, u64, usize, i64, f64);
value_arg!("true|false": bool);

impl ArgValue for String {
    fn set(&mut self, s: &str) -> Result<(), String> {
        *self = s.to_string();
        Ok(())
    }
    fn show(&self) -> String {
        self.clone()
    }
}

impl ArgValue for PathBuf {
    fn set(&mut self, s: &str) -> Result<(), String> {
        *self = PathBuf::from(s);
        Ok(())
    }
    fn show(&self) -> String {
        self.display().to_string()
    }
    fn meta() -> &'static str {
        "<path>"
    }
}

impl<T: ArgValue + Default> ArgValue for Option<T> {
    fn set(&mut self, s: &str) -> Result<(), String> {
        let mut v = T::default();
        v.set(s)?;
        *self = Some(v);
        Ok(())
    }
    fn show(&self) -> String {
        self.as_ref().map(T::show).unwrap_or_default()
    }
    fn meta() -> &'static str {
        T::meta()
    }
}

/// Comma-separated; a repeated flag appends.
impl<T: ArgValue + Default> ArgValue for Vec<T> {
    fn set(&mut self, s: &str) -> Result<(), String> {
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let mut v = T::default();
            v.set(part)?;
            self.push(v);
        }
        Ok(())
    }
    fn show(&self) -> String {
        self.iter().map(T::show).collect::<Vec<_>>().join(",")
    }
    fn meta() -> &'static str {
        "<a,b,...>"
    }
}

/// `--transport`: must match the cluster's transport.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Transport {
    #[default]
    Tcp,
    Ucx,
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Transport::Tcp => "tcp",
            Transport::Ucx => "ucx",
        })
    }
}

impl ArgValue for Transport {
    fn set(&mut self, s: &str) -> Result<(), String> {
        *self = match s {
            "tcp" => Transport::Tcp,
            "ucx" => Transport::Ucx,
            _ => return Err(format!("{s:?} is not tcp or ucx")),
        };
        Ok(())
    }
    fn show(&self) -> String {
        self.to_string()
    }
    fn meta() -> &'static str {
        "tcp|ucx"
    }
}

/// A core list in taskset syntax: `4-11`, `0,2,4`, `0-3,8-11`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CpuSet(pub Vec<usize>);

impl ArgValue for CpuSet {
    fn set(&mut self, s: &str) -> Result<(), String> {
        *self = CpuSet(parse_cpuset(s)?);
        Ok(())
    }
    fn show(&self) -> String {
        self.0.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(",")
    }
    fn meta() -> &'static str {
        "<cores>"
    }
}

/// Parse a taskset-style core list into sorted, deduplicated core ids:
/// `<int>` or `<int>-<int>` segments separated by commas (the
/// `:groupsize/chunksize` form is not supported).
pub fn parse_cpuset(spec: &str) -> Result<Vec<usize>, String> {
    let mut out: Vec<usize> = Vec::new();
    for seg in spec.split(',') {
        let seg = seg.trim();
        if seg.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = seg.split_once('-') {
            let lo: usize = lo
                .trim()
                .parse()
                .map_err(|_| format!("cpuset range lo not a non-negative integer: {seg:?}"))?;
            let hi: usize = hi
                .trim()
                .parse()
                .map_err(|_| format!("cpuset range hi not a non-negative integer: {seg:?}"))?;
            if hi < lo {
                return Err(format!("cpuset range hi<lo: {seg:?}"));
            }
            out.extend(lo..=hi);
        } else {
            let c: usize = seg
                .parse()
                .map_err(|_| format!("cpuset segment not a non-negative integer: {seg:?}"))?;
            out.push(c);
        }
    }
    if out.is_empty() {
        return Err("cpuset is empty".to_string());
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// Outcome of offering one flag to a section.
#[derive(Debug, PartialEq, Eq)]
pub enum Flag {
    /// Not this section's flag.
    Unknown,
    /// Taken together with the value that followed it.
    Value,
    /// A switch: taken, the value was not used.
    Switch,
}

/// A group of flags one binary accepts: a `params!` struct.
pub trait Section {
    /// Offer `--flag` and the token after it (or the part after `=`).
    fn offer(&self, arg: &str, value: Option<&str>) -> Result<Flag, String>;
    fn title(&self) -> String;
    fn help(&self) -> String;
    /// After the last flag. `given` holds the flags this section took
    /// (dash spelling, no `--`).
    fn finish(&self, given: &[String]) -> Result<(), String>;
}

impl<P: Params> Section for P {
    fn offer(&self, arg: &str, value: Option<&str>) -> Result<Flag, String> {
        crate::take_flag(self, arg, value)
    }
    fn title(&self) -> String {
        format!("{} parameters", P::ROLE)
    }
    fn help(&self) -> String {
        crate::help::<P>()
    }
    fn finish(&self, given: &[String]) -> Result<(), String> {
        crate::finish(self, given)
    }
}

/// Where flags may stand relative to the first non-flag word.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Positional {
    /// No non-flag word is accepted.
    Refuse,
    /// The first non-flag word (a subcommand) and everything after it are
    /// returned unparsed.
    Stop,
    /// Flags are taken wherever they stand; every other word, flag-like or
    /// not, is returned in order (for a subcommand parser to read). A
    /// command's flag value spelled exactly like a global flag is taken as
    /// that flag.
    Anywhere,
}

/// What `parse` found.
#[derive(Debug, PartialEq, Eq)]
pub enum Parsed {
    Help,
    /// The words left for the caller (see `Positional`).
    Rest(Vec<String>),
}

/// Parse `argv` (without the program name) into `sections`.
pub fn parse(
    argv: &[String],
    sections: &[&dyn Section],
    positional: Positional,
) -> Result<Parsed, String> {
    let mut given: Vec<Vec<String>> = vec![Vec::new(); sections.len()];
    let mut rest = Vec::new();
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].as_str();
        let flag_like = arg.starts_with('-') && arg != "-";
        if !flag_like {
            match positional {
                Positional::Refuse => return Err(format!("unexpected argument {arg:?}")),
                Positional::Stop => {
                    rest.extend_from_slice(&argv[i..]);
                    break;
                }
                Positional::Anywhere => {
                    rest.push(argv[i].clone());
                    i += 1;
                    continue;
                }
            }
        }
        if positional != Positional::Anywhere && (arg == "-h" || arg == "--help") {
            return Ok(Parsed::Help);
        }
        let (name, inline) = match arg.split_once('=') {
            Some((n, v)) if n.starts_with("--") => (n, Some(v)),
            _ => (arg, None),
        };
        let value = inline.or_else(|| argv.get(i + 1).map(String::as_str));
        let mut taken = None;
        for (k, s) in sections.iter().enumerate() {
            match s.offer(name, value)? {
                Flag::Unknown => continue,
                f => {
                    taken = Some((k, f));
                    break;
                }
            }
        }
        match taken {
            None if positional == Positional::Anywhere => {
                rest.push(argv[i].clone());
                i += 1;
            }
            None => return Err(format!("unknown argument {arg:?}")),
            Some((_, Flag::Switch)) if inline.is_some() => {
                return Err(format!("{name} takes no value"));
            }
            Some((k, f)) => {
                given[k].push(name.trim_start_matches('-').replace('_', "-"));
                i += if f == Flag::Value && inline.is_none() { 2 } else { 1 };
            }
        }
    }
    for (s, g) in sections.iter().zip(&given) {
        s.finish(g)?;
    }
    Ok(Parsed::Rest(rest))
}

/// `usage`, then every section's flags.
pub fn help_text(usage: &str, sections: &[&dyn Section]) -> String {
    let mut out = format!("{}\n", usage.trim_end());
    for s in sections {
        out.push_str(&format!("\n{}:\n{}", s.title(), s.help()));
    }
    out
}

/// Parse the process's argv: `--help` prints and exits 0, an error exits 2.
/// Returns what `positional` leaves.
pub fn parse_or_exit(usage: &str, sections: &[&dyn Section], positional: Positional) -> Vec<String> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match parse(&argv, sections, positional) {
        Ok(Parsed::Rest(rest)) => rest,
        Ok(Parsed::Help) => {
            print!("{}", help_text(usage, sections));
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("error: {e}\n(see --help)");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CrossCheck, Params, MIB};

    crate::params! {
        pub struct A("a") {
            /// Id.
            [required] id: u64, [1, u64::MAX];
            /// Port.
            port: u16 = 9000;
            /// Host.
            host: String = "0.0.0.0".into();
            /// Dirs.
            data: Vec<PathBuf>;
            peer: Option<String>;
            transport: Transport;
            cpuset: Option<CpuSet>;
            [switch] fast: bool;
            /// A size.
            [runtime] size: u64 = MIB, [1, u64::MAX];
        }
        removed {
            "old" => "--old was removed; use --port",
        }
        aliases {
            "addr" => "host",
            "ident" => "id",
        }
    }
    impl CrossCheck for A {
        fn cross_check(&self) -> Result<(), String> {
            if self.fast.get() && self.port.get() == 1 {
                return Err("--fast needs a real port".into());
            }
            Ok(())
        }
    }

    crate::params! {
        pub struct P("p") {
            /// A count.
            count: usize = 4, [1, 64];
        }
    }
    impl CrossCheck for P {}

    fn argv(s: &str) -> Vec<String> {
        s.split_whitespace().map(String::from).collect()
    }

    fn run(s: &str, mode: Positional) -> Result<(Parsed, A, usize), String> {
        let a = A::default();
        let p = P::default();
        let out = parse(&argv(s), &[&a, &p], mode)?;
        Ok((out, a, p.count.get()))
    }

    #[test]
    fn every_kind_of_field_in_one_pass() {
        let (out, a, count) = run(
            "--ident 7 --count 9 --size 4M --data /a,/b --data=/c --fast --peer h:1 --transport ucx --cpuset 0-2,5 --addr h",
            Positional::Refuse,
        )
        .unwrap();
        assert_eq!(out, Parsed::Rest(vec![]));
        assert_eq!((a.id.get(), a.port.get(), a.host.get().as_str(), count), (7, 9000, "h", 9));
        assert_eq!(a.size.get(), 4 * MIB);
        assert_eq!(a.data.get(), [PathBuf::from("/a"), "/b".into(), "/c".into()]);
        assert!(a.fast.get());
        assert_eq!(a.peer.get().as_deref(), Some("h:1"));
        assert_eq!(a.transport.get(), Transport::Ucx);
        assert_eq!(a.cpuset.get(), Some(CpuSet(vec![0, 1, 2, 5])));
    }

    #[test]
    fn refusals() {
        for (s, want) in [
            ("--port 1", "--id is required"),
            ("--id 0", "outside [1"),
            ("--id 1 --port x", "--port x"),
            ("--id 1 --port", "requires a value"),
            ("--id 1 --nope 1", "unknown argument"),
            ("--id 1 --old 3", "--old was removed"),
            ("--id 1 --fast=1", "takes no value"),
            ("--id 1 --transport rdma", "not tcp or ucx"),
            ("--id 1 --count 0", "--count 0"),
            ("--id 1 --fast --port 1", "real port"),
            ("--id 1 word", "unexpected argument"),
        ] {
            let e = run(s, Positional::Refuse).err().unwrap_or_else(|| panic!("{s}: accepted"));
            assert!(e.contains(want), "{s}: {e}");
        }
        assert_eq!(run("--id 1 --help", Positional::Refuse).unwrap().0, Parsed::Help);
    }

    crate::params! {
        pub struct B("b") {
            [required] dirs: Vec<PathBuf>;
            [required] manager: String;
        }
    }
    impl CrossCheck for B {}

    /// A required flag given an empty value is as good as missing.
    #[test]
    fn required_needs_a_value() {
        let b = B::default();
        let e = parse(&argv("--dirs= --manager m"), &[&b], Positional::Refuse).unwrap_err();
        assert!(e.contains("--dirs needs a non-empty value"), "{e}");
        let b = B::default();
        let e = parse(&argv("--dirs /d --manager="), &[&b], Positional::Refuse).unwrap_err();
        assert!(e.contains("--manager needs a non-empty value"), "{e}");
    }

    #[test]
    fn positional_modes() {
        let (out, a, _) = run("--id 2 ls -l /x --port 5", Positional::Stop).unwrap();
        assert_eq!(out, Parsed::Rest(argv("ls -l /x --port 5")));
        assert_eq!(a.port.get(), 9000, "flags after the subcommand are its own");

        let (out, a, count) = run("ls --port 5 -l --count 2 /x --id 3", Positional::Anywhere).unwrap();
        assert_eq!(out, Parsed::Rest(argv("ls -l /x")));
        assert_eq!((a.id.get(), a.port.get(), count), (3, 5, 2));
    }

    #[test]
    fn help_lists_every_flag() {
        let a = A::default();
        let p = P::default();
        let text = help_text("usage: a", &[&a, &p]);
        for want in [
            "usage: a",
            "--id <n>  (required, range [1,",
            "--port <n>  (default 9000)",
            "--host <v>  (default 0.0.0.0)",
            "--data <a,b,...>\n",
            "--transport tcp|ucx  (default tcp)",
            "--fast  (switch)\n",
            "--size <n>  (default 1048576, range [1, 18446744073709551615], runtime)",
            "p parameters:",
            "--count <n>",
        ] {
            assert!(text.contains(want), "missing {want:?}:\n{text}");
        }
        assert!(!text.contains("--old"));
        // Only numeric fields can be runtime; config keys list exactly those.
        let runtime: Vec<_> = A::specs().iter().filter(|s| s.runtime).map(|s| s.field).collect();
        assert_eq!(runtime, ["size"]);
    }
}
