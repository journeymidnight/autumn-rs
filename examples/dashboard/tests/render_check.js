// Render check for the panels whose job is to TURN A PAYLOAD INTO A VERDICT:
// the ops rows, a partition server's liveness, a disk's state, and an advisory
// that carries a full sentence of reasoning.
//
// The functions are LIFTED OUT OF index.html at run time, never copied — a
// copy would drift from the page and pass while the page was broken. There is
// no browser here, so `$` is stubbed to capture innerHTML; this checks the
// render logic and the strings, not pixels.
//
//   node examples/dashboard/tests/render_check.js
const fs = require("fs");
const path = require("path");
const page = fs.readFileSync(
  path.join(__dirname, "..", "static", "index.html"), "utf8");

function lift(name) {
  const i = page.indexOf(`function ${name}(`);
  if (i < 0) throw new Error(`index.html has no function ${name}`);
  let depth = 0, j = i;
  for (;; j++) {
    if (page[j] === "{") depth++;
    else if (page[j] === "}" && --depth === 0) break;
  }
  return page.slice(i, j + 1);
}
const escLine = page.split("\n").find(l => l.startsWith("const esc ="));
// Same treatment for the other one-line const helpers the lifted functions call.
const constLine = name => page.split("\n").find(l => l.startsWith(`const ${name} =`));

let OUT = {};
const ctx = { $: sel => ({ set innerHTML(v) { OUT[sel] = v; } }) };
const src = [escLine, constLine("jsAttr"), constLine("BYTE_KINDS"),
             constLine("COUNT_UNIT"), lift("fmtBytes"), lift("fmtProgress"), lift("agoStr"), lift("psHealth"),
             lift("diskRow"), lift("advRow"), lift("opsTarget"), lift("opsAgo"),
             lift("nodeAddr"), lift("extChip"),
             lift("renderLiveOps"), lift("renderOpsHistory")].join("\n");
const now = Math.floor(Date.now() / 1000);
let PURE = {};
new Function("$", "OUT2", src + `
OUT2.hb = [psHealth({}), psHealth({last_heartbeat_secs_ago:3}),
           psHealth({last_heartbeat_secs_ago:9}), psHealth({last_heartbeat_secs_ago:90})];
OUT2.disks = [
  diskRow({disk_id:3, uuid:"abcdef0123456789", total:1073741824, free:107374182, extent_bytes:536870912, reported:true, online:true, faulted:false}),
  diskRow({disk_id:4, uuid:"", total:0, free:null, extent_bytes:0, reported:true, online:false, faulted:true}),
  diskRow({disk_id:5, uuid:"deadbeef", total:0, free:0, extent_bytes:0, reported:false, online:false, faulted:false}),
].join("");
OUT2.adv = advRow({kind:"major", desc:"major  part 7             major compaction before split: partition still carries CoW-shared out-of-range keys (has_overlap), and split is REFUSED until a major compaction rewrites them",
                   action:{action:"compact", part_id:7}});
OUT2.jsattr = jsAttr("it's");
// A CoW split's shared extent, and a private one. The chip must NAME the other
// holders: refs=2 alone cannot tell an operator that collecting here frees
// nothing until part 19 collects too.
const NODES = [], DANGER = false;
OUT2.extShared = extChip({extent_id:14, role:"log", size:4297548796, open:false, ec:false,
                          refs:2, eversion:3, replicas:[5,3,1], shared_by_parts:[13,19]}, 13);
OUT2.extMany = extChip({extent_id:14, role:"log", size:1, open:false, ec:false,
                        refs:3, eversion:3, replicas:[5], shared_by_parts:[13,19,42]}, 13);
// A shared ROW extent: gc_debt is log-stream accounting, and a row extent is
// released by compaction's head truncate, so the debt sentence must not appear.
OUT2.extRow = extChip({extent_id:10, role:"row", size:1, open:false, ec:false,
                       refs:2, eversion:2, replicas:[5], shared_by_parts:[13,19]}, 13);
OUT2.extPrivate = extChip({extent_id:8, role:"log", size:1, open:false, ec:false,
                           refs:1, eversion:1, replicas:[1], shared_by_parts:[13]}, 13);
` )(ctx.$, PURE);
new Function("$", src + `
renderLiveOps([
  {kind:"ec-convert",state:"running",part_id:0,secondary_id:12,
   progress_done:268435456,progress_total:360712397,started_at:${now - 14},message:""},
  {kind:"merge",state:"running",part_id:7,secondary_id:9,
   progress_done:0,progress_total:0,started_at:${now - 2},message:"merging"},
  {kind:"gc",state:"running",part_id:7,secondary_id:0,
   progress_done:5,progress_total:8,started_at:${now - 1},message:""},
  {kind:"compact",state:"running",part_id:9,secondary_id:0,
   progress_done:3,progress_total:6,started_at:${now - 5},message:""}]);
renderOpsHistory([
  {kind:"recovery",state:"failed",part_id:0,secondary_id:31,
   progress_done:3,progress_total:8,finished_at:${now - 9},message:"",error:"disk offline"}], null);
`)(ctx.$);

const text = s => s.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
const live = text(OUT["#ops_live"] || ""), hist = text(OUT["#ops_hist"] || "");
let bad = 0;
const want = (hay, needle, why) => {
  if (!hay.includes(needle)) { console.error(`FAIL: ${why} — missing ${JSON.stringify(needle)}`); bad++; }
};
// The percentage AND the raw counts: "74%" alone cannot tell two tables from
// fifty gigabytes, and the magnitude is what decides whether an operator waits.
// The wire carries RAW counts; the panel owes them a unit. Eleven digits is
// what this assertion exists to keep out.
want(live, "74% · 256.0 MiB / 344.0 MiB", "ec-convert shows percent + BYTES, not raw counts");
want(OUT["#ops_live"], 'style="width:74%"', "ec-convert draws its bar");
// secondary_id means different things per kind — an extent must not render as
// a partition move.
want(live, "ec-convert extent 12", "extent-scoped kinds name their extent");
want(live, "merge 7→9", "merge keeps survivor→victim");
want(live, "gc 7 63% · 5 B / 8 B", "gc measures bytes too");
// …and a kind that does NOT measure bytes must not be dressed up as one.
want(live, "compact 9 50% · 3 / 6 blocks", "compact counts SST data blocks");
// A finished op's reason is the whole point of the history list.
want(hist, "recovery extent 31 disk offline", "failed history row shows the reason");

// A PS with no heartbeat entry is UNKNOWN, not dead: the state is defensive
// (replay and registration both seed one), and it must not paint the fleet red.
const wantEq = (got, exp, why) => {
  if (got !== exp) { console.error(`FAIL: ${why} — got ${JSON.stringify(got)} want ${JSON.stringify(exp)}`); bad++; }
};
wantEq(PURE.hb[0].cls, "unknown", "no heartbeat ever seen is not 'dead'");
wantEq(PURE.hb[0].text, "no heartbeat seen", "…and says so plainly");
wantEq(PURE.hb[1].cls, "ok", "3s is healthy (heartbeat cadence is 2s)");
wantEq(PURE.hb[2].cls, "warn", "9s is late but inside the 10s eviction window");
wantEq(PURE.hb[3].cls, "bad", "90s is past eviction");

// The two disk states the node-level rollup CANNOT express.
const d = text(PURE.disks);
want(d, "#3", "a healthy disk shows its id");
want(d, "online", "…and its state");
want(d, "1.0 GiB", "…and its capacity, in a unit that names the divisor");
want(d, "#4 faulted", "a disk its own node calls faulted is FAULTED, not merely offline");
want(PURE.disks, 'class="pill bad"', "faulted is styled as a fault");
// The third state is the one the node-level rollup and the faulted bit both
// miss: the registry assigns this disk to the node and the node never mentioned
// it. Its capacity fields are meaningless, so they must not render as zeroes.
want(d, "#5", "a registry disk the node never described is listed");
want(d, "not reported", "…and is named as not reported, not as offline");
if (/#5[^#]*0 B/.test(d)) { console.error("FAIL: an unreported disk renders a fake 0-byte capacity"); bad++; }

// An advisory's reason can be a whole sentence; the row must lead with the
// action and keep the reasoning, not truncate one into the other.
want(PURE.adv, "major  part 7", "advisory leads with kind + target");
want(PURE.adv, "major compaction before split", "…and keeps the whole reason");
want(PURE.adv, "Apply", "an actionable advisory offers its action");
// The Apply handler lives in a SINGLE-quoted attribute, so an apostrophe would
// end the attribute and break the button. No advisory target can contain one
// today (they are "part N" / "extent N" / "cluster"), which is exactly why the
// helper is tested directly rather than through a contrived advisory.
wantEq(PURE.jsattr, '"it\\u0027s"', "jsAttr escapes the apostrophe");

// Shared-extent chip. This is asserted on the RENDERED string, not on the
// helper that builds it: the first version of this feature computed the line
// correctly and never interpolated it into the template, so every other test
// here passed while the panel showed nothing.
want(text(PURE.extShared), "shared with part 19",
     "a shared extent names the OTHER holder");
want(text(PURE.extShared), "freed only once every holder drops it",
     "…and says why releasing here alone frees nothing");
want(text(PURE.extShared), "each counts whatever is dead here as its own debt",
     "a shared LOG extent explains the doubled debt");
want(text(PURE.extRow), "freed only once every holder drops it",
     "a shared row extent still names its holders");
if (/own debt/.test(PURE.extRow)) {
  console.error("FAIL: a row extent claims log-stream debt it cannot have"); bad++;
}
want(text(PURE.extMany), "shared with parts 19, 42",
     "several holders are listed, pluralised");
if (/shared with/.test(PURE.extPrivate)) {
  console.error("FAIL: a private extent claims to be shared"); bad++;
}
console.log("live:", live);
console.log("hist:", hist);
console.log(bad ? `render check FAILED (${bad})` : "render check OK");
process.exit(bad ? 1 : 0);
