#!/usr/bin/env python3
"""Unit tests for autumn_dashboard.py pure logic (no live cluster needed).

Run: python3 -m pytest python/test_autumn_dashboard.py   (or: python3 python/test_autumn_dashboard.py)
"""
import time

import autumn_dashboard as d


# ── candidate → actuation command mapping ────────────────────────────────────
def test_candidate_to_cmd_ec_uses_extent_in_secondary():
    # EC advisory: primary_part_id=0, secondary_part_id=extent_id (per policy.rs).
    c = {"kind": "ec", "primary_part_id": 0, "secondary_part_id": 12345}
    assert d.candidate_to_cmd(c) == ["force-ec-convert", "--extent", "12345"]


def test_candidate_to_cmd_split_merge_gc_compact():
    assert d.candidate_to_cmd({"kind": "split", "primary_part_id": 7}) == ["split", "7"]
    assert d.candidate_to_cmd(
        {"kind": "merge", "primary_part_id": 7, "secondary_part_id": 8}
    ) == ["merge", "7", "8"]
    assert d.candidate_to_cmd({"kind": "gc", "primary_part_id": 7}) == ["gc", "7"]
    assert d.candidate_to_cmd({"kind": "major", "primary_part_id": 7}) == ["compact", "7"]
    assert d.candidate_to_cmd({"kind": "minor", "primary_part_id": 7}) == ["compact", "7"]


def test_candidate_to_cmd_hotcold_and_unknown_are_advisory_only():
    assert d.candidate_to_cmd({"kind": "hotcold", "primary_part_id": 7}) is None
    assert d.candidate_to_cmd({"kind": "bogus", "primary_part_id": 7}) is None
    # merge / ec with a missing target id must not actuate a malformed command
    assert d.candidate_to_cmd({"kind": "merge", "primary_part_id": 7, "secondary_part_id": 0}) is None
    assert d.candidate_to_cmd({"kind": "ec", "secondary_part_id": 0}) is None


# ── decide_actions: filtering, cooldown, priority, rate cap ──────────────────
def _cand(kind, prim=0, sec=0):
    return {"kind": kind, "primary_part_id": prim, "secondary_part_id": sec, "reason": "t"}


def test_decide_respects_enabled_whitelist():
    cands = [_cand("split", 1), _cand("merge", 2, 3), _cand("ec", 0, 99)]
    picked = d.decide_actions(cands, {}, ("split",), now=1000.0, cooldown_secs=300, max_actions=10)
    assert [p[0]["kind"] for p in picked] == ["split"]


def test_decide_priority_split_before_merge():
    # split must be picked before merge (auto-split-before-merge invariant).
    cands = [_cand("merge", 2, 3), _cand("split", 1)]
    picked = d.decide_actions(cands, {}, d.ACTIONABLE_KINDS, now=1000.0, cooldown_secs=300, max_actions=1)
    assert len(picked) == 1
    assert picked[0][0]["kind"] == "split"


def test_decide_cooldown_skips_recent():
    cands = [_cand("gc", 5)]
    cd = {"gc:5": 1000.0}
    # 100s later, still inside the 300s cooldown → skipped
    assert d.decide_actions(cands, cd, d.ACTIONABLE_KINDS, now=1100.0, cooldown_secs=300, max_actions=10) == []
    # 400s later, cooldown elapsed → picked
    picked = d.decide_actions(cands, cd, d.ACTIONABLE_KINDS, now=1400.0, cooldown_secs=300, max_actions=10)
    assert len(picked) == 1


def test_decide_max_actions_caps():
    cands = [_cand("split", i) for i in range(10)]
    picked = d.decide_actions(cands, {}, d.ACTIONABLE_KINDS, now=1000.0, cooldown_secs=300, max_actions=3)
    assert len(picked) == 3


def test_decide_dedups_same_target_within_tick():
    cands = [_cand("gc", 5), _cand("gc", 5)]
    picked = d.decide_actions(cands, {}, d.ACTIONABLE_KINDS, now=1000.0, cooldown_secs=300, max_actions=10)
    assert len(picked) == 1


# ── apply_actions: dry-run vs apply, cooldown stamping, refusal handling ──────
def test_apply_dry_run_does_not_call_op_or_stamp():
    calls = []
    op = lambda cmd, json_out=True: calls.append(cmd)
    cd = {}
    actions = [(_cand("split", 1), ["split", "1"], "split:1")]
    issued = d.apply_actions(actions, op, cd, now=1000.0, dry_run=True)
    assert issued == 0 and calls == [] and cd == {}


def test_apply_real_calls_op_and_stamps_cooldown():
    calls = []
    op = lambda cmd, json_out=True: calls.append(cmd)
    cd = {}
    actions = [(_cand("split", 1), ["split", "1"], "split:1")]
    issued = d.apply_actions(actions, op, cd, now=1000.0, dry_run=False)
    assert issued == 1 and calls == [["split", "1"]] and cd["split:1"] == 1000.0


def test_apply_refusal_is_not_fatal_and_not_stamped():
    def op(cmd, json_out=True):
        raise d.OpError("precondition failed: ec conversion in flight")
    cd = {}
    actions = [(_cand("gc", 5), ["gc", "5"], "gc:5")]
    issued = d.apply_actions(actions, op, cd, now=1000.0, dry_run=False)
    # refused → not counted, not stamped (so it retries next tick)
    assert issued == 0 and "gc:5" not in cd


# ── gather + render against a fully mocked op ────────────────────────────────
def _fake_op_factory():
    def op(args, json_out=True):
        if args == ["df"]:
            return {
                "raw_total": 1 << 40, "raw_used": 1 << 39, "raw_free": 1 << 39,
                "physical_used": 1 << 38, "logical_stored_sealed": 1 << 37,
                "amplification": 2.0,
                "per_node": [{"node_id": 1, "total": 1 << 40, "free": 1 << 39, "extent_bytes": 1 << 38, "online": True}],
            }
        if args == ["info"]:
            # Real wire shape (verified e2e 2026-06-28): partitions carry
            # `ps_addr` (per-partition listener address, F099-K) + `live_size`
            # (authoritative total bytes), NOT an integer `ps_id`.
            return {"nodes": [], "extents": [], "streams": [],
                    "partitions": [
                        {"part_id": 9001, "ps_addr": "10.0.0.1:9301", "live_size": 1 << 30},
                        {"part_id": 9002, "ps_addr": "10.0.0.2:9302", "live_size": 1 << 29},
                    ]}
        if args == ["policy-candidates"]:
            return [
                {"kind": "split", "primary_part_id": 9001, "secondary_part_id": 0, "reason": "qps high"},
                {"kind": "ec", "primary_part_id": 0, "secondary_part_id": 50, "reason": "sealed 128M"},
            ]
        if args[:2] == ["info", "--part"]:
            pid = int(args[2])
            # Real wire: PartitionLoad.size_bytes is SST-flushed bytes and is 0
            # before the first flush — the render must fall back to topo live_size.
            return {"part_id": pid, "req_per_sec": 20000 if pid == 9001 else 100,
                    "p99_us": 500, "size_bytes": 0, "gc_debt_bytes": 1 << 20,
                    "pending_compaction_bytes": 0, "gc_inflight": 0, "compact_inflight": 0,
                    "sealed_log_extent_count": 3}
        raise d.OpError(f"unexpected op call: {args}")
    return op


def test_gather_collects_full_snapshot():
    state = d.gather(_fake_op_factory())
    assert state["df"]["raw_total"] == 1 << 40
    assert len(state["partitions"]) == 2
    assert {p["part_id"] for p in state["partitions"]} == {9001, 9002}
    assert len(state["candidates"]) == 2
    assert state["errors"] == []


def test_render_dashboard_includes_capacity_partitions_advisories():
    state = d.gather(_fake_op_factory())
    out = d.render_dashboard(state, now=time.mktime((2026, 6, 28, 12, 0, 0, 0, 0, 0)))
    assert "cluster dashboard" in out
    assert "capacity:" in out
    assert "9001" in out and "9002" in out
    # hottest partition (9001, 20000 IOPS) must sort above 9002
    assert out.index("9001") < out.index("9002")
    assert "policy advisories (2)" in out
    assert "split" in out and "extent 50" in out
    # e2e regression: PS column reads `ps_addr`, SIZE falls back to topo
    # live_size when the detail size_bytes is 0 (1 GiB → "1.0G").
    assert "10.0.0.1:9301" in out
    assert "1.0G" in out


def test_render_amp_shows_na_when_zero():
    # The manager sets amplification=0 when no sealed logical data has been
    # measured yet; render must show "n/a", not a misleading "0.00x".
    state = {"df": {"raw_total": 100, "raw_used": 50, "raw_free": 50,
                    "physical_used": 10, "logical_stored_sealed": 0, "amplification": 0},
             "partitions": [], "candidates": [], "errors": []}
    out = d.render_dashboard(state)
    assert "amp n/a" in out
    assert "0.00x" not in out


def test_gather_records_errors_without_crashing():
    def op(args, json_out=True):
        if args == ["df"]:
            raise d.OpError("manager down")
        if args == ["info"]:
            return {"partitions": []}
        if args == ["policy-candidates"]:
            return []
        raise d.OpError("x")
    state = d.gather(op)
    assert state["df"] is None
    assert any("df:" in e for e in state["errors"])
    # render must not crash on partial state
    d.render_dashboard(state)


if __name__ == "__main__":
    import sys
    failed = 0
    g = dict(globals())
    for name, fn in sorted(g.items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  ok   {name}")
            except Exception as e:
                failed += 1
                print(f"  FAIL {name}: {e}")
    print(f"\n{'FAILED' if failed else 'OK'}: {sum(1 for n in g if n.startswith('test_'))} tests, {failed} failed")
    sys.exit(1 if failed else 0)
