# autumn-params Crate Guide

Every tunable of every autumn process, declared once. One file per role
(`manager.rs`, `ps.rs`, `en.rs`, ...); `lib.rs` holds the `params!` macro and the
helpers built on it.

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
  Unknown keys and rejected values are returned, the rest still apply.
- `runtime_spec::<X>(key)` / `Spec::check`: validate a key and value without an
  instance (the manager validating `config set` for a PS or EN key).

Values live in atomics (`Relaxed`): a reader calls `.get()` where it needs the
value, so an override reaches it on the next read. Reading once at startup and
keeping a copy defeats `[runtime]`; only do that for startup-only parameters.

Integers accept binary suffixes (`64M`, `64MiB`, `1g`, `512k`) and `_`.

Cross-field checks (`ManagerParams::validate`) are hand-written next to the
declaration and called by the binary after parsing.
