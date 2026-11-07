# autumn-params Crate Guide

Every flag of every autumn binary, declared once, and the one argv parser that
reads them. One file per role: `manager.rs`, `ps.rs`, `en.rs` (servers),
`client.rs` (the SDK, shared by every binary that embeds `ClusterClient`),
`fs.rs` (the file-system layer, shared by autumn-fuse / autumnfs / autumn-s3),
`fuse.rs`, `autumnfs.rs`, `s3.rs`, `gallery.rs`, `dashboard.rs`, and for the
CLIs `op.rs`, `client_cli.rs` (autumn-client), `migrate.rs` (migratev1_v2).
A binary's struct holds every flag it takes, its ports, addresses, files,
ids and switches as well as its tunables. `lib.rs` holds the `params!` macro
and the helpers built on it; `args.rs` the flag value types and the argv
parser. There is no clap.

## Three kinds of parameter

- **Hard-coded**: a format or protocol invariant, or a value tied to another one
  (a timeout that must stay below another timeout). It stays a `const` in its
  crate and is not declared here.
- **Startup**: declared here. A CLI flag sets it; fixed for the process lifetime.
- **Runtime** (`[runtime]` in the declaration): also a startup flag, and
  overridable while the process runs (`autumn-op config set`). The override wins
  over the flag; dropping it goes back to the flag's value (`Live::base`).

## How a declaration is used

`params! { pub struct X("role") { /// help \n [runtime] name: T = default, [lo, hi]; } }`
generates the struct (one `Live<T>` or `Fixed<T>` per field, see below) and `impl Params`:

- `take_flag(&p, "--name", Some(v))`: what the parser calls through `&P`'s
  `Section` impl; `Unknown` = not a parameter of `P`, `Err` = a bad value.
  Flag = field name with `_` → `-`.
- `help::<X>()`: the `--help` section (default, range, `runtime` marker, help).
- `apply_overrides(&p, entries)`: installs the full override set of a role from
  `(role.flag, value)` pairs; runtime fields absent from it go back to their base.
  Changes apply one at a time; one that breaks the `CrossCheck` is undone and
  retried after the others (it may fit only once they are in). Unknown keys,
  rejected values and changes that never fit are returned and keep the old
  value; the rest still apply, so one bad entry does not hold back the others.
- `runtime_spec::<X>(key)` / `Spec::check`: validate a key and value without an
  instance (the manager validating `config set` for a PS or EN key).
- `Installed`: what a polling process (PS, EN) installed last; `install` calls
  `apply_overrides` only when this role's entries changed, so a poll that brings
  back the same set does nothing and logs nothing.

Overrides live in the manager (etcd `runtimeConfig/<role>.<flag>`) and are set
with `autumn-op config set|unset <role>.<flag>`; `autumn-op config keys` lists
every runtime parameter from these declarations. The PS and EN fetch the set on
their authz poll (`--config-poll-secs`, 30 s); the manager applies `manager.`
entries to itself on each change it makes and on every replay (startup and
promotion; a standby does not follow later changes until it is promoted).

Values live in atomics (`Relaxed`): a reader calls `.get()` where it needs the
value, so an override reaches it on the next read. Reading once at startup and
keeping a copy defeats `[runtime]`; only do that for startup-only parameters.

Integers accept binary suffixes (`64M`, `64MiB`, `1g`, `512k`) and `_`.

Cross-field checks are `impl CrossCheck` next to the declaration (empty for
most structs). The parser calls `cross_check` after the last flag, and
`apply_overrides` refuses each change that would fail it.

## Field types and storage

`name: T` where `T` is any `Param`: numbers and `bool` live in a `Live<T>`
(atomics, the only kind a `[runtime]` field can be: `[runtime]` on anything
else does not compile); `String`, `PathBuf`, `Transport`, `CpuSet`
(taskset syntax), `Option<T>` and `Vec<T>` (comma-separated, a repeated flag
appends, so a `Vec` field must default to empty) live in a `Fixed<T>` (a lock, set while parsing, `get()` clones).
`[required]` must be given with a non-empty value, `[switch]` takes no value,
a range is checked per value. `removed { "old" => "why" }` refuses a retired
flag with its hint; `aliases { "old" => "new" }` keeps an older spelling.
Process-identity fields (psid, ports, addresses) are read once by `main` and
passed to constructors; library code does not read them from the global.

## The parser (`args.rs`)

`parse_or_exit(usage, &[params(), autumn_fs::params(), ...], positional)`
makes one pass over argv, offering each flag to every struct in turn
(`Section`, implemented for every `params!` struct). `--help` prints `usage`
and every struct's flags (exit 0); an unknown flag, a bad value, a missing
required flag or a failed cross-check prints the error (exit 2). `positional`
says what a non-flag word means: `Refuse` (servers, daemons), `Stop` (the
command of autumn-client / autumnfs; global flags go before it), `Anywhere`
(autumn-op: global flags stand anywhere, everything else is handed back in
order for the command parser; a command's flag value spelled exactly like a
global flag is taken as that flag). Commands and their own flags stay
hand-parsed in the binary.

Fast mode (`--policy-fast-mode`) is a set of manager parameter defaults: the
manager applies it before the pass so an explicit flag still wins.
