// Tab smoke test: drive the REAL page script, under a minimal DOM stub, through
// EVERY tab and both lazy drawers, and assert each pane rendered the thing it
// exists to show.
//
// render_check.js lifts individual functions and checks their strings; this one
// runs the page's own init and tab switching, so it catches the failure that
// one cannot — a pane that throws, or reaches for an element the markup does
// not declare (the stub refuses any id absent from the HTML, which is how a
// renamed container is caught instead of silently painting nothing).
//
// The fixtures below are a REAL /api/* capture from
// `bash examples/dashboard/tests/api_contract.sh`, edited only to add the
// unhappy states a healthy scratch cluster never produces: a faulted disk, a
// silent partition server, an overlapping partition, an advisory.
//
//   node examples/dashboard/tests/tabs_smoke.js
const fs = require("fs"), path = require("path");
const html = fs.readFileSync(path.join(__dirname, "..", "static", "index.html"), "utf8");
const script = html.match(/<script>([\s\S]*)<\/script>/)[1];

const ids = new Set([...html.matchAll(/\sid="([^"]+)"/g)].map(m => m[1]));
const nodes = {};
const mk = id => (nodes[id] = {
  id, innerHTML: "", textContent: "", value: (id === "interval" ? "5000" : ""),
  hidden: false, checked: false, scrollTop: 0, clientHeight: 600,
  style: {}, className: "", classList: { add() {}, remove() {}, contains() { return false; } },
  appendChild() {},
});
const document = {
  querySelector(sel) {
    if (!sel.startsWith("#")) throw new Error("stub only supports #id, got " + sel);
    const id = sel.slice(1);
    if (!ids.has(id)) throw new Error(`page queried #${id}, which the markup does not declare`);
    return nodes[id] || mk(id);
  },
  createElement: () => ({ className: "", textContent: "", remove() {} }),
};

const disk = (id, extra) => Object.assign({
  disk_id: id, uuid: `uuid-${id}`, total: 3779300532224, free: 345875152896,
  extent_bytes: 12345678, reported: true, online: true, faulted: false,
}, extra || {});
const OVERVIEW = {
  ts: Math.floor(Date.now() / 1000),
  part_count: 2, ps_count: 2,
  total_req_per_sec: 12, total_write_bytes_per_sec: 1048576, total_read_bytes_per_sec: 2097152,
  errors: [],
  df: {
    raw_total: 7558601064448, raw_used: 6866850758656, raw_free: 691750305792,
    physical_used: 4096, logical_footprint: 2048, logical_wal_debt: 512,
    amplification: 2.0, node_count_online: 1,
    per_node: [{ node_id: 1, total: 7558601064448, free: 691750305792, extent_bytes: 4096, online: true, disks: [disk(2), disk(3)] }],
  },
  nodes: [
    { node_id: 1, address: "127.0.0.1:21101", extent_count: 3, total: 7558601064448,
      free: 691750305792, extent_bytes: 4096, online: true, auto_state: "Online",
      last_heartbeat_secs_ago: 1, suspected_age_secs: 0, override_kind: "-",
      override_reason: "", override_set_by: "", node_uuid: "node-uuid", shard_ports: [21101],
      // one healthy, one the node calls faulted, one the registry has but the
      // node did not describe — all three states the tab must tell apart.
      disks: [disk(2), disk(3, { faulted: true, online: false }),
              disk(4, { reported: false, online: false, total: 0, free: 0, extent_bytes: 0 })] },
  ],
  partitions: [
    { part_id: 1, ps_id: 1, ps_addr: "127.0.0.1:21201", range_start: "", range_end: "m",
      live_size: 1048576, total_extents: 3, req_per_sec: 12,
      write_bytes_per_sec: 1048576, read_bytes_per_sec: 2097152 },
    { part_id: 2, ps_id: 1, ps_addr: "127.0.0.1:21202", range_start: "m", range_end: "",
      live_size: 0, total_extents: 3, req_per_sec: 0,
      write_bytes_per_sec: 0, read_bytes_per_sec: 0 },
  ],
  ps_roll: [{ ps_id: 1, addr: "127.0.0.1:21201", n: 2, size: 1048576 }],
  ps_servers: [
    { ps_id: 1, addr: "127.0.0.1:21201", last_heartbeat_secs_ago: 1, partition_count: 2,
      n: 2, size: 1048576, req_per_sec: 12, write_bytes_per_sec: 1048576,
      read_bytes_per_sec: 2097152, total_extents: 6 },
    // registered, serving nothing, and silent — the state the partition list
    // cannot express and the reason ps_servers is on the wire at all.
    { ps_id: 2, addr: "127.0.0.1:21203", last_heartbeat_secs_ago: null, partition_count: 0,
      n: 0, size: 0, req_per_sec: 0, write_bytes_per_sec: 0, read_bytes_per_sec: 0, total_extents: 0 },
  ],
  advisories: [
    { kind: "major", primary_part_id: 1, secondary_part_id: 0,
      reason: "major compaction required before split",
      desc: "major  part 1             major compaction required before split: partition still carries CoW-shared out-of-range keys",
      action: { action: "compact", part_id: 1 }, key: "major:1" },
  ],
};
const PART = {
  part_id: 1, req_per_sec: 12, p99_us: 900, write_bytes_per_sec: 1048576,
  read_bytes_per_sec: 2097152, size_bytes: 1048576, gc_debt_bytes: 0,
  pending_compaction_bytes: 2147483648, gc_inflight: 0, compact_inflight: 0,
  has_overlap: 1, sst_out_of_range_bytes: 4096,
  extents: [{ extent_id: 9, role: "log", size: 1024, refs: 1, eversion: 1, open: true, ec: false, replicas: [1] }],
};
const OPS = {
  live: [{ op_id: 1, kind: "gc", state: "running", part_id: 1, secondary_id: 0,
           progress_done: 5, progress_total: 8, started_at: Math.floor(Date.now() / 1000) - 3,
           submitted_at: 0, finished_at: 0, message: "" }],
  history: [{ op_id: 2, kind: "compact", state: "failed", part_id: 1, secondary_id: 0,
              progress_done: 0, progress_total: 0, started_at: 0, submitted_at: 0,
              finished_at: Math.floor(Date.now() / 1000) - 9, message: "", error: "no address for part 1" }],
  history_error: null,
};
const POLICIES = {
  enabled: true, mode: "dry_run", active: "gc-only", allow_mutations: true,
  policies: [{ name: "gc-only", desc: "Reclaim space only (GC)", builtin: true,
               interval: 30, cooldown: 120, max_actions: 2,
               switches: { split: false, ec: false, compact: false, gc: true, merge: false, rebalance: false } }],
  switch_order: ["split", "ec", "compact", "gc", "merge", "rebalance"],
  log: [{ ts: Math.floor(Date.now() / 1000), level: "refused", msg: "autumn-op split 168: overlapping keys" }],
};
const payload = { "/api/overview": OVERVIEW, "/api/ops": OPS, "/api/policies": POLICIES };
const fetchStub = async p => ({ json: async () => payload[p] ?? PART });

const api = new Function(
  "document", "location", "window", "fetch", "setInterval", "clearInterval", "setTimeout", "confirm",
  script + "\nreturn {setTab, openDetail, openNode, openPs};")
  (document, { hash: "#overview" }, {}, fetchStub, () => 0, () => {}, f => f(), () => false);

const settle = () => new Promise(r => setTimeout(r, 30));
(async () => {
  await settle();
  for (const t of ["overview", "partitions", "servers", "nodes", "policy", "logs"]) {
    api.setTab(t);
    await settle();
  }
  await api.openDetail(1);
  api.openNode(1);
  api.openPs(2);
  await settle();

  let bad = 0;
  const want = (sel, needle, why) => {
    const got = (nodes[sel.slice(1)] || {}).innerHTML || "";
    if (!got.includes(needle)) { console.error(`FAIL ${sel}: ${why} — missing ${JSON.stringify(needle)}`); bad++; }
  };
  want("#tabbar", "Overview", "the tab bar names every tab");
  want("#tabbar", "Nodes", "…including Nodes");
  want("#tabbar", "Logs", "…and Logs");
  // Nodes: the per-disk state, and the fault called out in words.
  want("#nodes", "node 1", "the Nodes tab lists the node");
  want("#nodedrawer", "Disks", "the node drawer has a disk table");
  want("#nodedrawer", "faulted", "…and names the faulted disk as faulted");
  want("#nodedrawer", "not reported", "…and separates a disk the node never described");
  want("#nodedrawer", "NOT described on its last df", "…with what that actually means");
  want("#nodedrawer", "uuid-2", "…and shows each disk's identity");
  // Servers: the registered-but-silent PS, which no partition row could show.
  want("#ps_full", "PS 2", "a PS serving nothing is still listed");
  want("#psdrawer", "Serving no partition", "…and says so");
  want("#psdrawer", "never seen a heartbeat", "…and distinguishes silent from dead");
  // Overview: the health roll-up an operator reads first.
  want("#ov_fleet", "disk", "the fleet panel counts disks");
  want("#ov_fleet", "faulted", "…and surfaces the faulted one");
  want("#ov_space", "raw used", "the space panel reports capacity");
  // Partitions: the precondition warning, before the Split button is clicked.
  want("#drawer", "Split is refused until a major compaction", "an overlapping partition warns BEFORE the click");
  want("#drawer", "Extents", "…and still shows the extents");
  // Policy + Logs.
  want("#advisories", "major compaction required before split", "the advisory keeps its whole reason");
  want("#autolog", "overlapping keys", "the auto-policy log is on the Logs tab");
  want("#ops_live", "gc", "running ops are on the Logs tab");
  want("#ops_hist", "no address for part 1", "…and a failed op keeps its reason");
  console.log(bad ? `tabs smoke FAILED (${bad})` : "tabs smoke OK — six tabs + both drawers render");
  process.exit(bad ? 1 : 0);
})();
