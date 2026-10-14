# autumn-params Crate Guide

Every tunable of every autumn process, declared once. One file per role:
`manager.rs`, `ps.rs`, `en.rs` (servers), `client.rs` (the SDK, shared by every
binary that embeds `ClusterClient`), `fs.rs` (the file-system layer, shared by
autumn-fuse / autumnfs / autumn-s3), `fuse.rs`, `autumnfs.rs`, `s3.rs`,
`gallery.rs`, `dashboard.rs`. `lib.rs` holds the `params!` macro and the helpers
built on it. A clap binary pulls its parameters out of argv with `split_params`
(+ `take_any!` over several structs) before clap sees the rest.

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
generates the struct (one `Live<T>` per field) and `impl Params`:

- `take_flag(&p, "--name", Some(v))`: the binary's parser offers every argument it
  does not own itself; `Unknown` means not a parameter (the binary exits 2),
  `Err` means a bad value (exit 2). Flag = field name with `_` → `-`.
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
most structs). The binary calls `cross_check` after parsing, and
`apply_overrides` refuses each change that would fail it.
