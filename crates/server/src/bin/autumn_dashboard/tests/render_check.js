// Render check for the panels whose job is to TURN A PAYLOAD INTO A VERDICT:
// the ops rows, a partition server's liveness, a disk's state, and an advisory
// that carries a full sentence of reasoning.
//
// The functions are LIFTED OUT OF index.html at run time, never copied — a
// copy would drift from the page and pass while the page was broken. There is
// no browser here, so `$` is stubbed to capture innerHTML; this checks the
// render logic and the strings, not pixels.
//
//   node crates/server/src/bin/autumn_dashboard/tests/render_check.js
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
             lift("diskRow"), lift("hotColdAdvisory"), lift("advRow"), lift("opsTarget"), lift("opsAgo"),
             lift("nodeAddr"), lift("extChip"), lift("extentHealthRows"), lift("extentHealthBad"), lift("statusRows"), lift("repairBtn"), lift("cancelBtn"),
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
OUT2.hotcold = advRow({kind:"hotcold", primary_part_id:32, secondary_part_id:21,
                       reason:"ps_id=3 size_ratio=45 hot=[32] cold=[21]",
                       desc:"hotcold part 32 ps_id=3 size_ratio=45 hot=[32] cold=[21]", action:null});
OUT2.hotcoldBoth = advRow({kind:"hotcold", primary_part_id:8, secondary_part_id:4,
                           reason:"ps_id=2 qps_ratio=12 hot=[8, 9] cold=[4] size_ratio=20 hot=[8] cold=[4, 5]",
                           desc:"", action:null});
OUT2.jsattr = jsAttr("it's");
OUT2.ehDegraded = extentHealthRows({sealed_extents:10, degraded:2, no_redundancy:1,
  unavailable:0, recovering:1, degraded_bytes:134217728,
  problems:[{extent_id:77, serving:1, total:3, needed:1, recovering:true,
             slots:[{slot:1, node_id:5, state:"unreachable", degraded_secs:812},
                    {slot:2, node_id:6, state:"behind", degraded_secs:0}]}]});
OUT2.ehErr = extentHealthRows({sealed_extents:4, degraded:0, no_redundancy:0,
  unavailable:1, recovering:0, degraded_bytes:1,
  problems:[{extent_id:9, serving:3, total:6, needed:4, recovering:false,
             slots:[{slot:3, node_id:2, state:"unreachable", degraded_secs:60}]}]});
OUT2.ehRequested = extentHealthRows({sealed_extents:3, degraded:1, no_redundancy:0,
  unavailable:0, recovering:0, degraded_bytes:1, repair_requested_slots:1,
  problems:[{extent_id:41, serving:2, total:3, needed:1, recovering:false,
             slots:[{slot:2, node_id:7, state:"unreachable", degraded_secs:900, repair_requested:true}]}]});
OUT2.ehClean = extentHealthRows({sealed_extents:4, degraded:0, no_redundancy:0,
  unavailable:0, recovering:0, degraded_bytes:0, problems:[]});
OUT2.ehUnknown = extentHealthRows(null);
const fleet = (members) => members.map(([id, state, age]) => ({id, address:"h"+id+":1", state, age_secs:age}));
const status = (o) => Object.assign({sampled_at_ms:0, recovery_inflight:0,
  managers:{leader:1, standby:1, expected:2, members:fleet([[1,"leader",0],[2,"standby",0]])},
  partition_servers:{ready:3, expected:3, members:fleet([[1,"ready",1],[2,"ready",1],[3,"ready",2]])},
  extent_nodes:{online:2, expected:2, members:fleet([[1,"online",1],[2,"online",2]])},
  extents:{sealed:12, clean:12, degraded:0, unavailable:0}}, o);
OUT2.stOk = statusRows(status({}));
OUT2.stDown = statusRows(status({
  managers:{leader:1, standby:0, expected:2, members:fleet([[1,"leader",0],[2,"absent",180]])},
  partition_servers:{ready:2, expected:3, members:fleet([[1,"ready",1],[2,"ready",1],[7,"evicted",720]])},
  extent_nodes:{online:1, expected:2, members:fleet([[1,"online",1],[4,"unknown (no df yet)",null]])},
  extents:{sealed:12, clean:10, degraded:1, unavailable:1}, recovery_inflight:3}));
OUT2.stEmpty = statusRows(status({partition_servers:{ready:0, expected:0, members:[]}}));
OUT2.stUnknown = statusRows(null);
OUT2.stNotLoaded = statusRows(undefined);
OUT2.ehBad = [extentHealthBad(undefined), extentHealthBad(null),
              extentHealthBad({degraded:0, unavailable:0}), extentHealthBad({degraded:1, unavailable:0})];
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

// A PS with no heartbeat entry (and no eviction recorded) is UNKNOWN, not
// dead, and must not paint the fleet red.
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

// Extent health: the counts an operator acts on, and the extent to look at.
const ehd = text(PURE.ehDegraded);
want(ehd, "2 extents degraded (128.0 MiB), 1 with no redundancy left", "degraded row counts and sizes");
want(ehd, "extent 77 1/3 serving (rebuilding): slot1 node 5 unreachable", "…and names the worst extent and slot");
want(ehd, "slot2 node 6 behind", "every non-serving slot is named");
want(PURE.ehDegraded, "var(--bad)", "no redundancy left is styled as bad");
want(ehd, "1 extent rebuilding", "a running rebuild is shown");
// The worst problem is already rebuilding, so it gets no Repair button; a
// readable, idle one does.
if (PURE.ehDegraded.includes("Repair #")) { console.error("FAIL: a rebuilding extent offered a Repair button"); bad++; }
const rb = new Function(src + `return repairBtn({problems:[{extent_id:9,serving:1,needed:1,recovering:false}]});`)();
want(rb, 'action:"repair",extent_id:9', "a readable idle problem extent offers a Repair action");
const rbNone = new Function(src + `return repairBtn({problems:[{extent_id:9,serving:0,needed:1,recovering:false}]});`)();
wantEq(rbNone, "", "an unreadable extent has nothing to rebuild from — no button");
want(text(PURE.ehErr), "1 extent unavailable — fewer serving copies than a read needs", "an unreadable extent is an error");
want(text(PURE.ehErr), "extent 9 3/6 serving", "…named with its shard count");
wantEq(PURE.ehClean, "", "a clean summary raises nothing");
want(text(PURE.ehUnknown), "Extent health unknown", "an unread summary is unknown, never healthy");
wantEq(PURE.ehBad.join(","), "false,true,false,true", "the all-clear line needs a summary that says clean");

const ehr = text(PURE.ehRequested);
want(ehr, "slot2 node 7 unreachable", "the requested slot is named");
want(ehr, "(repair requested)", "…and marked as queued to move");
want(ehr, "1 slot queued to be rebuilt on another node", "the standing requests are counted");
want(PURE.ehRequested, 'action:"repair_cancel",extent_id:41', "a standing request can be cancelled from the page");

// An advisory's reason can be a whole sentence; the row must lead with the
// action and keep the reasoning, not truncate one into the other.
want(PURE.adv, "major  part 7", "advisory leads with kind + target");
want(PURE.adv, "major compaction before split", "…and keeps the whole reason");
want(PURE.adv, "Apply", "an actionable advisory offers its action");
const hotcold = text(PURE.hotcold);
want(hotcold, "PS 3 partition size imbalance", "hot/cold names the affected PS and measured dimension");
want(hotcold, "45× largest/smallest", "hot/cold explains the ratio");
want(hotcold, "large: part 32", "hot/cold explains the hot side without jargon");
want(hotcold, "small: part 21", "hot/cold explains the cold side without jargon");
want(hotcold, "five 1-minute policy samples", "hot/cold states its observation window");
want(hotcold, "Information only", "hot/cold says it cannot execute an operation");
if (/\bhotcold\b|\bhot=|\bcold=/.test(hotcold)) {
  console.error("FAIL: hot/cold leaks wire-oriented jargon into the operator explanation"); bad++;
}
const hotcoldBoth = text(PURE.hotcoldBoth);
want(hotcoldBoth, "request rate and partition size imbalance", "both triggering dimensions are named");
want(hotcoldBoth, "busy: part 8, part 9", "QPS hot list is decoded");
want(hotcoldBoth, "small: part 4, part 5", "size cold list is decoded");
// The Apply handler lives in a SINGLE-quoted attribute, so an apostrophe would
// end the attribute and break the button. No advisory target can contain one
// today (they are "part N" / "extent N" / "cluster"), which is exactly why the
// helper is tested directly rather than through a contrived advisory.
wantEq(PURE.jsattr, '"it\\u0027s"', "jsAttr escapes the apostrophe");

// Status bar: every denominator is the expected member count, and each member
// that is not up is named with its state. Unknown is never rendered as healthy.
const stOk = text(PURE.stOk), stDown = text(PURE.stDown);
want(stOk, "leader 1 / standby 1 (2 expected)", "managers: leader, standby, expected");
want(stOk, "Ready 3/3", "PS ready over expected");
want(stOk, "Online 2/2", "EN online over expected");
want(stOk, "clean 12 / degraded 0 / unavailable 0", "extent counts");
want(stOk, "inflight 0", "recovery in flight");
want(stOk, "by manager 1 (h1:1)", "names the leader that sampled it");
if (/dot (warn|bad)/.test(PURE.stOk)) { console.error("FAIL: a whole fleet is not flagged"); bad++; }
want(stDown, "leader 1 / standby 0 (2 expected)", "a stopped standby stays expected");
want(stDown, "2 h2:1 absent 3m ago", "…and is named");
want(stDown, "Ready 2/3", "an evicted PS stays in the denominator");
want(stDown, "7 h7:1 evicted 12m ago", "…and is named with its age");
want(stDown, "Online 1/2", "an unanswered node is not online");
want(stDown, "4 h4:1 unknown (no df yet)", "…and says why");
want(PURE.stDown, 'dot bad"></span><span class="k">Extent', "an unavailable extent is red");
want(text(PURE.stUnknown), "unknown the leader did not answer", "null is unknown, not healthy");
want(PURE.stEmpty, 'dot warn"></span><span class="k">PS', "no expected PS is not a healthy fleet");
wantEq(PURE.stNotLoaded, "", "nothing before the first load");

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
