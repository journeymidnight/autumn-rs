//! Partition servers' and extent nodes' metrics as the leader last heard them,
//! and their Prometheus rendering.
//!
//! Neither serves a `/metrics` of its own: a PS sends its counters on the load
//! report (every 5 s), an EN on its `df` answer (every 2 s). The leader keeps
//! the latest of each in memory; its `/metrics`, `MSG_GET_FLEET_METRICS` and
//! `autumn-op metrics` all render from here. A new leader starts empty and
//! fills within one report; the counters are the reporters' own, so rates
//! carry on across a failover.

use std::collections::HashMap;

use autumn_rpc::extent_rpc::DfResp;
use autumn_rpc::manager_rpc::{
    EnDiskMetrics, EnMetricsReport, FleetMetricsResp, LatencyHist, PartitionLoad,
    PsMetricsReport, ReportPartitionLoadReq, CODE_OK, LATENCY_BUCKET_BOUNDS_NS,
};
use serde_json::{json, Value};

/// A report older than this is left out: its reporter has stopped (or lost
/// the leader), and its frozen counters would read as zero rates.
pub(crate) const REPORT_MAX_AGE_MS: u64 = 30_000;

#[derive(Default)]
pub(crate) struct FleetMetrics {
    ps: HashMap<u64, PsMetricsReport>,
    nodes: HashMap<u64, EnMetricsReport>,
}

impl FleetMetrics {
    pub(crate) fn record_ps(&mut self, now_ms: u64, req: &ReportPartitionLoadReq) {
        self.ps.insert(
            req.ps_id,
            PsMetricsReport {
                ps_id: req.ps_id,
                reported_at_ms: now_ms,
                block_cache: req.block_cache.clone(),
                partitions: req.partitions.clone(),
            },
        );
        self.prune(now_ms);
    }

    pub(crate) fn record_node(&mut self, now_ms: u64, node_id: u64, df: &DfResp) {
        self.nodes.insert(
            node_id,
            EnMetricsReport {
                node_id,
                reported_at_ms: now_ms,
                counters: df.counters.clone(),
                disks: df
                    .disk_status
                    .iter()
                    .map(|(disk_id, st)| EnDiskMetrics {
                        disk_id: *disk_id,
                        online: st.online,
                        full: st.full,
                    })
                    .collect(),
            },
        );
        self.prune(now_ms);
    }

    pub(crate) fn forget_ps(&mut self, ps_id: u64) {
        self.ps.remove(&ps_id);
    }

    pub(crate) fn clear(&mut self) {
        self.ps.clear();
        self.nodes.clear();
    }

    fn prune(&mut self, now_ms: u64) {
        let fresh = |t: u64| now_ms.saturating_sub(t) <= REPORT_MAX_AGE_MS;
        self.ps.retain(|_, r| fresh(r.reported_at_ms));
        self.nodes.retain(|_, r| fresh(r.reported_at_ms));
    }

    /// The fresh reports, ordered by id.
    pub(crate) fn snapshot(&self, now_ms: u64) -> FleetMetricsResp {
        let fresh = |t: u64| now_ms.saturating_sub(t) <= REPORT_MAX_AGE_MS;
        let mut ps: Vec<PsMetricsReport> =
            self.ps.values().filter(|r| fresh(r.reported_at_ms)).cloned().collect();
        ps.sort_by_key(|r| r.ps_id);
        let mut nodes: Vec<EnMetricsReport> =
            self.nodes.values().filter(|r| fresh(r.reported_at_ms)).cloned().collect();
        nodes.sort_by_key(|r| r.node_id);
        FleetMetricsResp {
            code: CODE_OK,
            message: String::new(),
            ps,
            nodes,
        }
    }
}

/// One sample: full metric name (a histogram's carry `_bucket`/`_sum`/
/// `_count`), labels, value.
pub struct Sample {
    pub name: String,
    pub labels: Vec<(&'static str, String)>,
    pub value: f64,
}

/// One metric family, its samples contiguous after its `# TYPE` line.
pub struct Family {
    pub name: &'static str,
    pub kind: &'static str,
    pub samples: Vec<Sample>,
}

impl Family {
    fn new(name: &'static str, kind: &'static str) -> Self {
        Family { name, kind, samples: Vec::new() }
    }

    fn push(&mut self, labels: Vec<(&'static str, String)>, value: f64) {
        self.samples.push(Sample { name: self.name.to_string(), labels, value });
    }
}

type PartSel = fn(&PartitionLoad) -> f64;

/// Every family of a fleet snapshot: PS-wide series labelled `ps`, partition
/// series `ps` + `part`, extent-node series `node`.
pub fn families(r: &FleetMetricsResp) -> Vec<Family> {
    let mut out = Vec::new();

    let cache: [(&'static str, &'static str, fn(&PsMetricsReport) -> f64); 5] = [
        ("autumn_ps_partitions", "gauge", |p| p.partitions.len() as f64),
        ("autumn_ps_block_cache_hits_total", "counter", |p| p.block_cache.hits as f64),
        ("autumn_ps_block_cache_misses_total", "counter", |p| p.block_cache.misses as f64),
        ("autumn_ps_block_cache_bytes", "gauge", |p| p.block_cache.bytes as f64),
        ("autumn_ps_block_cache_capacity_bytes", "gauge", |p| p.block_cache.capacity as f64),
    ];
    for (name, kind, sel) in cache {
        let mut f = Family::new(name, kind);
        for p in &r.ps {
            f.push(vec![("ps", p.ps_id.to_string())], sel(p));
        }
        out.push(f);
    }

    let parts = || {
        r.ps.iter()
            .flat_map(|p| p.partitions.iter().map(move |l| (p.ps_id, l)))
    };
    let labels = |ps: u64, l: &PartitionLoad| vec![("ps", ps.to_string()), ("part", l.part_id.to_string())];
    let plain: [(&'static str, &'static str, PartSel); 22] = [
        ("autumn_ps_partition_requests_total", "counter", |l| l.counters.requests as f64),
        ("autumn_ps_partition_write_bytes_total", "counter", |l| l.counters.write_bytes as f64),
        ("autumn_ps_partition_read_bytes_total", "counter", |l| l.counters.read_bytes as f64),
        ("autumn_ps_partition_size_bytes", "gauge", |l| l.size_bytes as f64),
        ("autumn_ps_partition_sst_tables", "gauge", |l| l.counters.sst_tables as f64),
        ("autumn_ps_partition_memtable_bytes", "gauge", |l| l.counters.memtable_bytes as f64),
        ("autumn_ps_partition_imm_memtables", "gauge", |l| l.counters.imm_memtables as f64),
        ("autumn_ps_partition_flushes_total", "counter", |l| l.counters.flushes as f64),
        ("autumn_ps_partition_flush_bytes_total", "counter", |l| l.counters.flush_bytes as f64),
        ("autumn_ps_partition_major_compactions_total", "counter", |l| {
            l.counters.major_compactions as f64
        }),
        ("autumn_ps_partition_vp_reads_total", "counter", |l| l.counters.vp_reads as f64),
        ("autumn_ps_partition_vp_read_bytes_total", "counter", |l| l.counters.vp_read_bytes as f64),
        ("autumn_ps_partition_gc_debt_bytes", "gauge", |l| l.gc_debt_bytes as f64),
        ("autumn_ps_partition_open_tail_dead_bytes", "gauge", |l| l.open_tail_dead_bytes as f64),
        ("autumn_ps_partition_pending_compaction_bytes", "gauge", |l| {
            l.pending_compaction_bytes as f64
        }),
        ("autumn_ps_partition_minor_compact_pending_bytes", "gauge", |l| {
            l.minor_compact_pending_bytes as f64
        }),
        ("autumn_ps_partition_gc_inflight", "gauge", |l| l.gc_inflight as f64),
        ("autumn_ps_partition_compact_inflight", "gauge", |l| l.compact_inflight as f64),
        ("autumn_ps_partition_sealed_log_extents", "gauge", |l| l.sealed_log_extent_count as f64),
        ("autumn_ps_partition_unsettled_deletes", "gauge", |l| l.unsettled_deletes as f64),
        ("autumn_ps_partition_sst_tombstone_bytes", "gauge", |l| l.sst_tombstone_bytes as f64),
        ("autumn_ps_partition_open_tail_bytes", "gauge", |l| l.open_tail_bytes as f64),
    ];
    for (name, kind, sel) in plain {
        let mut f = Family::new(name, kind);
        for (ps, l) in parts() {
            f.push(labels(ps, l), sel(l));
        }
        out.push(f);
    }

    type Labeled = fn(&PartitionLoad) -> [(&'static str, f64); 4];
    let labeled: [(&'static str, &'static str, usize, Labeled); 3] = [
        ("autumn_ps_partition_minor_compactions_total", "trigger", 4, |l| {
            let c = &l.counters;
            [
                ("flush", c.minor_flush as f64),
                ("tick", c.minor_tick as f64),
                ("blocking", c.minor_blocking as f64),
                ("reclaim", c.minor_reclaim as f64),
            ]
        }),
        ("autumn_ps_partition_compaction_write_bytes_total", "kind", 2, |l| {
            let c = &l.counters;
            [
                ("minor", c.compaction_bytes_minor as f64),
                ("major", c.compaction_bytes_major as f64),
                ("", 0.0),
                ("", 0.0),
            ]
        }),
        ("autumn_ps_partition_get_sst_checks_total", "result", 3, |l| {
            let c = &l.counters;
            [
                ("skipped", c.sst_skipped as f64),
                ("hit", c.sst_hits as f64),
                ("false_positive", c.sst_false_positives as f64),
                ("", 0.0),
            ]
        }),
    ];
    for (name, label, n, sel) in labeled {
        let mut f = Family::new(name, "counter");
        for (ps, l) in parts() {
            for (value, v) in sel(l).into_iter().take(n) {
                let mut lb = labels(ps, l);
                lb.push((label, value.to_string()));
                f.push(lb, v);
            }
        }
        out.push(f);
    }

    let hists: [(&'static str, fn(&PartitionLoad) -> &LatencyHist); 2] = [
        ("autumn_ps_write_duration_seconds", |l| &l.counters.write_lat),
        ("autumn_ps_get_duration_seconds", |l| &l.counters.get_lat),
    ];
    for (name, sel) in hists {
        let mut f = Family::new(name, "histogram");
        for (ps, l) in parts() {
            let h = sel(l);
            let mut cum = 0u64;
            for (i, bound) in LATENCY_BUCKET_BOUNDS_NS.iter().enumerate() {
                cum += h.buckets.get(i).copied().unwrap_or(0);
                let mut lb = labels(ps, l);
                lb.push(("le", format!("{}", *bound as f64 / 1e9)));
                f.samples.push(Sample { name: format!("{name}_bucket"), labels: lb, value: cum as f64 });
            }
            let mut lb = labels(ps, l);
            lb.push(("le", "+Inf".to_string()));
            f.samples.push(Sample { name: format!("{name}_bucket"), labels: lb, value: h.count as f64 });
            f.samples.push(Sample {
                name: format!("{name}_sum"),
                labels: labels(ps, l),
                value: h.sum_ns as f64 / 1e9,
            });
            f.samples.push(Sample { name: format!("{name}_count"), labels: labels(ps, l), value: h.count as f64 });
        }
        out.push(f);
    }

    let node = |n: &EnMetricsReport| vec![("node", n.node_id.to_string())];
    let en: [(&'static str, &'static str, fn(&EnMetricsReport) -> f64); 4] = [
        ("autumn_en_append_batches_total", "counter", |n| n.counters.append_batches as f64),
        ("autumn_en_append_bytes_total", "counter", |n| n.counters.append_bytes as f64),
        ("autumn_en_append_ns_total", "counter", |n| n.counters.append_ns as f64),
        ("autumn_en_extents", "gauge", |n| {
            n.counters.shard_extents.iter().map(|(_, e)| *e).sum::<u64>() as f64
        }),
    ];
    for (name, kind, sel) in en {
        let mut f = Family::new(name, kind);
        for n in &r.nodes {
            f.push(node(n), sel(n));
        }
        out.push(f);
    }
    let mut rejects = Family::new("autumn_en_auth_rejects_total", "counter");
    let mut shards = Family::new("autumn_en_shard_extents", "gauge");
    let mut online = Family::new("autumn_en_disk_online", "gauge");
    let mut full = Family::new("autumn_en_disk_full", "gauge");
    for n in &r.nodes {
        let c = &n.counters;
        for (class, v) in [
            ("opcode_denied", c.rejects_opcode_denied),
            ("peer_auth", c.rejects_peer_auth),
            ("client_read", c.rejects_client_read),
            ("client_token", c.rejects_client_token),
        ] {
            let mut lb = node(n);
            lb.push(("class", class.to_string()));
            rejects.push(lb, v as f64);
        }
        for (shard, e) in &c.shard_extents {
            let mut lb = node(n);
            lb.push(("shard", shard.to_string()));
            shards.push(lb, *e as f64);
        }
        for d in &n.disks {
            let mut lb = node(n);
            lb.push(("disk_id", d.disk_id.to_string()));
            online.push(lb.clone(), u8::from(d.online) as f64);
            full.push(lb, u8::from(d.full) as f64);
        }
    }
    out.extend([rejects, shards, online, full]);
    out
}

/// Prometheus text exposition of `families`.
pub fn render_text(families: &[Family]) -> String {
    use autumn_common::metrics_http::{push_metric, push_type};
    let mut out = String::new();
    for f in families {
        push_type(&mut out, f.name, f.kind);
        for s in &f.samples {
            push_metric(&mut out, &s.name, &s.labels, s.value);
        }
    }
    out
}

/// `families` as JSON, with when each reporter was last heard from.
pub fn to_json(r: &FleetMetricsResp, families: &[Family]) -> Value {
    json!({
        "ps": r.ps.iter().map(|p| json!({
            "ps_id": p.ps_id,
            "reported_at_ms": p.reported_at_ms,
            "partitions": p.partitions.len(),
        })).collect::<Vec<_>>(),
        "nodes": r.nodes.iter().map(|n| json!({
            "node_id": n.node_id,
            "reported_at_ms": n.reported_at_ms,
        })).collect::<Vec<_>>(),
        "families": families.iter().map(|f| json!({
            "name": f.name,
            "type": f.kind,
            "samples": f.samples.iter().map(|s| json!({
                "name": s.name,
                "labels": s.labels.iter().map(|(k, v)| (k.to_string(), json!(v))).collect::<serde_json::Map<_, _>>(),
                "value": s.value,
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_rpc::extent_rpc::{DiskStatus, EnCounters};
    use autumn_rpc::manager_rpc::{BlockCacheStats, PartitionCounters};

    fn sample(text: &str, series: &str) -> f64 {
        text.lines()
            .find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
            .unwrap_or_else(|| panic!("no sample {series} in\n{text}"))
    }

    fn load(part_id: u64) -> PartitionLoad {
        PartitionLoad {
            part_id,
            size_bytes: 1000,
            counters: PartitionCounters {
                sst_tables: 7,
                minor_blocking: 2,
                compaction_bytes_major: 900,
                sst_skipped: 5,
                get_lat: LatencyHist { buckets: vec![3, 1], sum_ns: 2_000_000, count: 5 },
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn reports_render_as_prometheus_text_and_stale_ones_drop_out() {
        let mut fm = FleetMetrics::default();
        fm.record_ps(
            1_000,
            &ReportPartitionLoadReq {
                ps_id: 3,
                partitions: vec![load(1), load(2)],
                block_cache: BlockCacheStats { hits: 10, misses: 2, bytes: 64, capacity: 512 },
            },
        );
        let df = DfResp {
            counters: EnCounters { rejects_peer_auth: 4, shard_extents: vec![(0, 2), (1, 3)], ..Default::default() },
            disk_status: vec![(9, DiskStatus { online: true, full: true, ..Default::default() })],
            ..Default::default()
        };
        fm.record_node(1_000, 6, &df);
        let snap = fm.snapshot(2_000);
        let text = render_text(&families(&snap));

        assert_eq!(sample(&text, r#"autumn_ps_block_cache_hits_total{ps="3"}"#), 10.0);
        assert_eq!(sample(&text, r#"autumn_ps_partitions{ps="3"}"#), 2.0);
        assert_eq!(sample(&text, r#"autumn_ps_partition_sst_tables{ps="3",part="2"}"#), 7.0);
        assert_eq!(
            sample(&text, r#"autumn_ps_partition_minor_compactions_total{ps="3",part="1",trigger="blocking"}"#),
            2.0
        );
        assert_eq!(
            sample(&text, r#"autumn_ps_partition_compaction_write_bytes_total{ps="3",part="1",kind="major"}"#),
            900.0
        );
        assert_eq!(
            sample(&text, r#"autumn_ps_partition_get_sst_checks_total{ps="3",part="1",result="skipped"}"#),
            5.0
        );
        assert_eq!(sample(&text, r#"autumn_ps_get_duration_seconds_bucket{ps="3",part="1",le="0.001"}"#), 4.0);
        assert_eq!(sample(&text, r#"autumn_ps_get_duration_seconds_bucket{ps="3",part="1",le="+Inf"}"#), 5.0);
        assert_eq!(sample(&text, r#"autumn_en_auth_rejects_total{node="6",class="peer_auth"}"#), 4.0);
        assert_eq!(sample(&text, r#"autumn_en_extents{node="6"}"#), 5.0);
        assert_eq!(sample(&text, r#"autumn_en_disk_full{node="6",disk_id="9"}"#), 1.0);

        // Each family once, its samples together.
        let mut seen = std::collections::HashSet::new();
        let mut current = String::new();
        for l in text.lines() {
            if let Some(rest) = l.strip_prefix("# TYPE ") {
                current = rest.split(' ').next().unwrap().to_string();
                assert!(seen.insert(current.clone()), "{current} typed twice");
            } else {
                assert!(l.starts_with(&current), "{l} outside its group ({current})");
            }
        }

        let later = fm.snapshot(1_000 + REPORT_MAX_AGE_MS + 1);
        assert!(later.ps.is_empty() && later.nodes.is_empty(), "stale reports must drop out");
    }
}
