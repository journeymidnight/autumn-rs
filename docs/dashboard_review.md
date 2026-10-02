# Dashboard review and promotion — 2026-09-27

Reviewed the pre-move `examples/dashboard` at `5237db2d`: the Rust HTTP/CLI
bridge, embedded page, tests, `autumn-op` policy dispatch, manager policy
updates, Docker build and VKE exposure. The component now lives in
`crates/server/src/bin/autumn_dashboard`, with the same `autumn-dashboard`
binary name. `autumn-server` builds it by default; the example package is gone.

## Findings

| Priority | Finding and trigger | Disposition |
| --- | --- | --- |
| P1 | Policy names were HTML-escaped inside an inline JavaScript string. HTML entity decoding restores quotes before JavaScript executes; quotes break the handler and crafted names can execute script. The advisory argument helper also left HTML entities unescaped. | Fixed: one JSON-string encoder escapes `<`, `>`, `&` and apostrophes for the surrounding single-quoted HTML attribute. Policy names use it for both activate and delete. |
| P1 | HTTP callers are not authenticated. autumn-op calls made on the caller's behalf prove the cluster secret the dashboard holds. The VKE APIG Ingress exposes all paths, including mutations; older comments incorrectly claimed ClusterIP meant private access. | Existing documented access contract retained, with contradictory comments corrected. This promotion does not introduce a login system. |
| P1 | `autumn-op auto-policy activate` first sends SET_ACTIVE, then SET_MODE. SET_ACTIVE preserves the prior mode. If the old mode is Armed, selection intended as DryRun can temporarily inherit Armed; a failure between calls leaves partial state, and another operator can interleave a change. | Existing CLI/manager transaction gap recorded in `feature_list.md`. Not changed by moving the dashboard; requires an atomic name+mode operation and failure/concurrency tests across all callers. |
| P2 | Failed policy writes return HTTP 502 and `{ok:false, output:...}`, but Save checked only `error`, announced success, and cleared the input. Activate/Arm/Stop/Delete ignored the result. A failed status response was interpreted as an empty Off configuration. | Fixed: check HTTP status and error/ok fields, show the upstream reason, retain failed edits, render unknown when status cannot be fetched, and clear that state after a successful refresh. |
| P2 | Empty/incorrectly typed activation bodies could select the current policy and change its mode. Invalid numeric fields were silently omitted; `max_actions` above u32 was passed to a CLI parser that defaulted it. Option-like names were interpreted as CLI flags. | Fixed: typed policy write bodies, a required activation intent, validated names/switches/ranges, HTTP 400 before spawning a child; a failed status lookup for bare Arm remains HTTP 502. |
| P2 | Each refresh could start another identical read while the previous CLI call was pending, up to its 30-second deadline. A late partition detail response could overwrite a newer selection. | Fixed in the page: concurrent GETs per URL share a promise; writes are never deduplicated/retried. Detail results are installed only for the still-selected partition. |
| P2 | The old API test hard-coded a developer directory, removed a fixed scratch directory, killed arbitrary owners of its port band, used `set -u` without fail-fast assertions, and could accept empty operation history. | Replaced with a portable Python harness behind the same shell entry point: discover Cargo output, choose free ports, use a temporary directory, reap only owned children, require nonempty durable history and assert the policy lifecycle. |

The name/mode transaction finding is verified from the current call sequence
in `crates/server/src/bin/autumn_op/main.rs::cmd_auto_policy` and the mode
preservation in `crates/manager/src/lib.rs::autopolicy_set`. An actual unintended
maintenance dispatch in that inter-RPC window has **not** been reproduced here.
HTTP exposure is established from the checked-in routes/manifests; no production
endpoint or deployment was contacted.

## Validation

- `cargo build -p autumn-server --bins --locked --offline`: builds the promoted
  dashboard together with its `autumn-op` companion.
- Workspace library/binary/test-target compilation passes. Dashboard-target
  Clippy (`--no-deps -- -D warnings`), rustfmt and diff whitespace checks pass;
  unrelated existing dependency/test warnings remain.
- Existing render and six-tab/drawer smoke checks pass. New policy controls
  tests execute the shipped script, covering mutation refusals/network failures,
  unknown status and recovery, arbitrary names, and overlapping reads.
- Ablation: the pre-move page fails the save-refusal regression. Restoring only
  the old policy button interpolation executes the injected JavaScript and fails
  the assertion that policy names must never execute script.
- The real isolated API harness passes with etcd + manager + one two-disk EN +
  PS + dashboard. It verifies embedded HTML byte equality, topology/disk fields,
  partition detail, nonempty durable operation history, custom policy
  create → DryRun → Armed → Off → delete, invalid payload rejection, built-in
  edit refusal, and HTTP 502 after terminating the manager. All policy switches
  are off during this test, so the Armed check does not actuate maintenance.
- The JavaScript checks and real API harness are added to CI after the workspace
  build. These are local control-plane tests, not a deployment or a Linux/UCX
  throughput benchmark.

## Performance and component boundary

The move adds no dependency or work to client/PS/EN I/O. The executable still
proxies through `autumn-op`; it does not duplicate manager RPC definitions or
move the leader-owned controller into the HTTP process. Each invocation still
has one blocking worker and two pipe-reader threads, and polls child exit every
50 ms with the existing 30-second deadline. Browser read coalescing bounds
repeated refreshes within one page; separate users still generate separate
processes. No throughput improvement is claimed for the directory move.

## Policy controls follow-up — 2026-09-27

The old page made the operator select a policy into DryRun before offering
Arm. Each policy now has direct Start and optional Observe actions; Stop
stops the current controller. Start sends the clicked name and `enabled:true`
in one HTTP request, with no prior selection or active-policy lookup. Labels
show Running / Observing / Stopped. This uses the existing HTTP/CLI interface;
the name/mode RPC transaction finding above remains open.

The user explicitly confirmed retaining existing HTTP access and recording its
risk while fixing functionality; authentication is not part of this work.

The same follow-up separates controller policies from manager diagnoses. The
former "Policy advisories" panel is now "Operational advisories". A hot/cold
candidate such as `ps_id=3 size_ratio=45 hot=[32] cold=[21]` renders as a PS 3
partition-size imbalance: 45x largest/smallest, large partition 32, small
partition 21, sustained across five one-minute samples. It is labeled
information-only because hot/cold has no actuation command.

## Raw-capacity amplification correction — 2026-09-27

The dashboard showed an impossible 0.35x on a cluster whose layouts range from
4+1 EC to three replicas. The consumer was dividing EN-maintained extent file
lengths by logical size. `amp` now means raw filesystem capacity consumed
(`raw_total - raw_free`) divided by logical extent size (distinct sealed extent
sizes plus committed open extent sizes). The manager's scan explicitly counts
only sealed extents; open lengths come from the PS commit-length probes.
`physical_used` remains available as the separately labeled `extent files`
diagnostic. A regression fixture uses raw used 1250, logical size 1000 and
extent file lengths 350: the result must be 1.25x, while the old formula is
0.35x. The same helper drives dashboard overview and `autumn-op df`.
