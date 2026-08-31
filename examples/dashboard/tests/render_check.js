// Render check for the ops panel: does the page turn an /api/ops payload into
// the row an operator reads?
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

let OUT = {};
const ctx = { $: sel => ({ set innerHTML(v) { OUT[sel] = v; } }) };
const src = [escLine, lift("opsTarget"), lift("opsAgo"),
             lift("renderLiveOps"), lift("renderOpsHistory")].join("\n");
const now = Math.floor(Date.now() / 1000);
new Function("$", src + `
renderLiveOps([
  {kind:"ec-convert",state:"running",part_id:0,secondary_id:12,
   progress_done:268435456,progress_total:360712397,started_at:${now - 14},message:""},
  {kind:"merge",state:"running",part_id:7,secondary_id:9,
   progress_done:0,progress_total:0,started_at:${now - 2},message:"merging"},
  {kind:"gc",state:"running",part_id:7,secondary_id:0,
   progress_done:5,progress_total:8,started_at:${now - 1},message:""}]);
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
want(live, "74% · 268435456 / 360712397", "ec-convert shows percent + raw counts");
want(OUT["#ops_live"], 'style="width:74%"', "ec-convert draws its bar");
// secondary_id means different things per kind — an extent must not render as
// a partition move.
want(live, "ec-convert extent 12", "extent-scoped kinds name their extent");
want(live, "merge 7→9", "merge keeps survivor→victim");
want(live, "gc 7 63% · 5 / 8", "gc shows its partition and ratio");
// A finished op's reason is the whole point of the history list.
want(hist, "recovery extent 31 disk offline", "failed history row shows the reason");
console.log("live:", live);
console.log("hist:", hist);
console.log(bad ? `render check FAILED (${bad})` : "render check OK");
process.exit(bad ? 1 : 0);
