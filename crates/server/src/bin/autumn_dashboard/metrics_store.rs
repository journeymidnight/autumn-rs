//! In-memory history of every partition server's metrics.
//!
//! Once per round the poller asks the manager (`autumn-op metrics`) for each
//! PS's latest report. A report is summed over its partitions as it arrives —
//! the page shows the cluster and single PSes, never one partition — and a
//! gauge also keeps its largest partition as `name:max`. Every series is a
//! column of one shared round axis, one `f32` per round (NaN = none), so series
//! from different PSes line up and add up point for point. Counters are stored
//! as per-second rates over the PS's own report times (a sum that went down —
//! a restart or a reopened partition — yields no point); gauges as read.
//! Nothing is persisted: a dashboard restart starts an empty history.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use serde_json::{json, Map, Value};

/// How a series combines across PSes in a cluster view.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Agg {
    Sum,
    Max,
}

struct Series {
    agg: Agg,
    /// Absolute round index of `values[0]`.
    start: u64,
    values: VecDeque<f32>,
    /// The counter's last raw reading `(report ms, value)`, to rate the next.
    last_raw: Option<(u64, f64)>,
}

/// The latest report of one PS, for the page's reporter list.
#[derive(Clone, Debug, Default)]
pub struct PsStatus {
    pub last_report_ms: Option<u64>,
    pub error: Option<String>,
}

pub enum Scope {
    Cluster,
    Ps(u64),
}

/// One sample of a report: metric name, labels other than `ps` / `part`
/// folded into the name, whether it is a counter, its partition (if any).
pub struct Sample {
    pub name: String,
    pub counter: bool,
    pub per_partition: bool,
    pub value: f64,
}

pub struct MetricsStore {
    retention_ms: u64,
    interval: Duration,
    /// Round start times; `rounds[0]` has absolute index `first_round`.
    rounds: VecDeque<u64>,
    first_round: u64,
    /// Per round, the PSes that sent no new report although they had before:
    /// a sum missing one of them would read as a drop, so such a round is
    /// left out of it.
    failed: VecDeque<Vec<u64>>,
    /// Keyed by (ps, series name).
    series: HashMap<(u64, String), Series>,
    ps: BTreeMap<u64, PsStatus>,
}

impl MetricsStore {
    pub fn new(interval: Duration, retention: Duration) -> Self {
        Self {
            retention_ms: retention.as_millis() as u64,
            interval,
            rounds: VecDeque::new(),
            first_round: 0,
            failed: VecDeque::new(),
            series: HashMap::new(),
            ps: BTreeMap::new(),
        }
    }

    /// Start a round at `t_ms` and drop what has aged out.
    pub fn begin_round(&mut self, t_ms: u64) {
        self.rounds.push_back(t_ms);
        self.failed.push_back(Vec::new());
        let floor = t_ms.saturating_sub(self.retention_ms);
        while self.rounds.front().is_some_and(|t| *t < floor) {
            self.rounds.pop_front();
            self.failed.pop_front();
            self.first_round += 1;
        }
        let first = self.first_round;
        self.series.retain(|_, s| {
            while s.start < first && !s.values.is_empty() {
                s.values.pop_front();
                s.start += 1;
            }
            s.values.iter().any(|v| !v.is_nan()) || s.last_raw.is_some_and(|(t, _)| t >= floor)
        });
    }

    /// The PS had reported before but sent nothing new this round.
    pub fn record_failure(&mut self, ps_id: u64) {
        if let Some(f) = self.failed.back_mut() {
            f.push(ps_id);
        }
    }

    /// Forget PSes the manager no longer reports.
    pub fn retain_ps(&mut self, live: &[u64]) {
        self.ps.retain(|id, _| live.contains(id));
    }

    pub fn set_status(&mut self, ps_id: u64, status: PsStatus) {
        self.ps.insert(ps_id, status);
    }

    /// Fold one PS's report, taken at `report_ms`, into the current round.
    pub fn ingest(&mut self, ps_id: u64, report_ms: u64, samples: &[Sample]) {
        // name -> (counter, sum over partitions, largest partition)
        let mut folded: BTreeMap<&str, (bool, f64, Option<f64>)> = BTreeMap::new();
        for s in samples {
            let e = folded.entry(&s.name).or_insert((s.counter, 0.0, None));
            e.1 += s.value;
            if s.per_partition && !s.counter {
                e.2 = Some(e.2.map_or(s.value, |m: f64| m.max(s.value)));
            }
        }
        for (name, (counter, sum, max)) in folded {
            self.record(ps_id, name.to_string(), counter, Agg::Sum, report_ms, sum);
            if let Some(m) = max {
                self.record(ps_id, format!("{name}:max"), false, Agg::Max, report_ms, m);
            }
        }
    }

    fn record(&mut self, ps: u64, name: String, counter: bool, agg: Agg, t_ms: u64, v: f64) {
        let Some(idx) = (self.first_round + self.rounds.len() as u64).checked_sub(1) else {
            return;
        };
        let s = self.series.entry((ps, name)).or_insert_with(|| Series {
            agg,
            start: idx,
            values: VecDeque::new(),
            last_raw: None,
        });
        let point = if !counter {
            Some(v)
        } else {
            let rate = match s.last_raw {
                Some((t0, v0)) if t_ms > t0 && v >= v0 => Some((v - v0) * 1000.0 / (t_ms - t0) as f64),
                _ => None,
            };
            s.last_raw = Some((t_ms, v));
            rate
        };
        let Some(point) = point else { return };
        if s.values.is_empty() {
            s.start = idx;
        }
        let slot = (idx - s.start) as usize;
        while s.values.len() <= slot {
            s.values.push_back(f32::NAN);
        }
        s.values[slot] = point as f32;
    }

    /// The scope's series on the common round axis: summed over the scope's
    /// PSes (a `name:max` series takes the largest). `null` = nothing reported
    /// at that round, or a PS of the scope sent no new report then.
    pub fn query(&self, scope: &Scope) -> Value {
        let len = self.rounds.len();
        let mut out: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();
        for ((ps, name), s) in &self.series {
            if let Scope::Ps(id) = scope {
                if ps != id {
                    continue;
                }
            }
            let off = (s.start - self.first_round) as usize;
            let col = out.entry(name.clone()).or_insert_with(|| vec![None; len]);
            for (i, v) in s.values.iter().enumerate() {
                if v.is_nan() {
                    continue;
                }
                let v = *v as f64;
                let slot = &mut col[off + i];
                *slot = Some(match (s.agg, *slot) {
                    (_, None) => v,
                    (Agg::Sum, Some(acc)) => acc + v,
                    (Agg::Max, Some(acc)) => acc.max(v),
                });
            }
        }
        let incomplete: Vec<bool> = self
            .failed
            .iter()
            .map(|f| match scope {
                Scope::Cluster => !f.is_empty(),
                Scope::Ps(id) => f.contains(id),
            })
            .collect();
        let mut series = Map::new();
        for (name, mut vals) in out {
            for (v, gap) in vals.iter_mut().zip(&incomplete) {
                if *gap {
                    *v = None;
                }
            }
            series.insert(name, json!(vals));
        }
        let ps: Vec<Value> = self
            .ps
            .iter()
            .map(|(id, st)| json!({"ps_id": id, "last_report_ms": st.last_report_ms, "error": st.error}))
            .collect();
        json!({
            "interval_secs": self.interval.as_secs(),
            "retention_secs": self.retention_ms / 1000,
            "t": self.rounds.iter().collect::<Vec<_>>(),
            "series": series,
            "ps": ps,
        })
    }
}

/// `autumn_ps_partition_flushes_total` → `flushes`.
pub fn short_name(name: &str) -> &str {
    let n = name
        .strip_prefix("autumn_ps_partition_")
        .or_else(|| name.strip_prefix("autumn_ps_"))
        .unwrap_or(name);
    n.strip_suffix("_total").unwrap_or(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(name: &str, counter: bool, per_partition: bool, value: f64) -> Sample {
        Sample { name: name.into(), counter, per_partition, value }
    }

    /// Two partitions of one PS: tables 3 and 5, cache hits, a minor counter.
    fn report(hits: f64, minors: f64) -> Vec<Sample> {
        vec![
            s("block_cache_hits", true, false, hits),
            s("sst_tables", false, true, 3.0),
            s("sst_tables", false, true, 5.0),
            s("minor_compactions{trigger=flush}", true, true, minors),
            s("minor_compactions{trigger=flush}", true, true, 1.0),
        ]
    }

    fn series(v: &Value, name: &str) -> Vec<Option<f64>> {
        let raw: Vec<Option<f64>> = serde_json::from_value(v["series"][name].clone())
            .unwrap_or_else(|_| panic!("no {name}: {v}"));
        raw.into_iter().map(|x| x.map(|x| (x * 1e4).round() / 1e4)).collect()
    }

    #[test]
    fn a_report_is_summed_over_partitions_and_counters_become_rates() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(3600));
        st.begin_round(10_000);
        st.ingest(1, 9_000, &report(100.0, 1.0));
        st.begin_round(20_000);
        st.ingest(1, 19_000, &report(150.0, 3.0));
        let q = st.query(&Scope::Ps(1));
        assert_eq!(series(&q, "block_cache_hits"), vec![None, Some(5.0)]);
        assert_eq!(series(&q, "minor_compactions{trigger=flush}"), vec![None, Some(0.2)]);
        assert_eq!(series(&q, "sst_tables"), vec![Some(8.0), Some(8.0)]);
        assert_eq!(series(&q, "sst_tables:max"), vec![Some(5.0), Some(5.0)]);
    }

    #[test]
    fn the_cluster_view_sums_servers_and_takes_the_largest_partition() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(3600));
        st.begin_round(10_000);
        st.ingest(1, 9_000, &report(0.0, 0.0));
        st.ingest(2, 9_500, &[s("sst_tables", false, true, 9.0)]);
        let q = st.query(&Scope::Cluster);
        assert_eq!(series(&q, "sst_tables"), vec![Some(17.0)]);
        assert_eq!(series(&q, "sst_tables:max"), vec![Some(9.0)]);
    }

    #[test]
    fn a_round_without_a_new_report_is_left_out_of_its_sums() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(3600));
        st.begin_round(10_000);
        st.ingest(1, 9_000, &report(0.0, 0.0));
        st.ingest(2, 9_000, &[s("sst_tables", false, true, 9.0)]);
        st.begin_round(20_000);
        st.ingest(1, 19_000, &report(0.0, 0.0));
        st.record_failure(2);
        assert_eq!(series(&st.query(&Scope::Cluster), "sst_tables"), vec![Some(17.0), None]);
        assert_eq!(series(&st.query(&Scope::Ps(1)), "sst_tables"), vec![Some(8.0), Some(8.0)]);
        assert_eq!(series(&st.query(&Scope::Ps(2)), "sst_tables"), vec![Some(9.0), None]);
    }

    #[test]
    fn a_reset_sum_yields_no_point_and_a_late_series_lines_up() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(3600));
        st.begin_round(10_000);
        st.begin_round(20_000);
        st.ingest(1, 19_000, &[s("flushes", true, true, 100.0)]);
        st.begin_round(30_000);
        st.ingest(1, 29_000, &[s("flushes", true, true, 5.0)]);
        st.begin_round(40_000);
        st.ingest(1, 39_000, &[s("flushes", true, true, 25.0)]);
        assert_eq!(series(&st.query(&Scope::Cluster), "flushes"), vec![None, None, None, Some(2.0)]);
    }

    #[test]
    fn history_older_than_the_retention_is_dropped() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(20));
        for t in [0u64, 10_000, 20_000, 30_000] {
            st.begin_round(t);
            st.ingest(1, t, &report(0.0, 0.0));
        }
        let q = st.query(&Scope::Cluster);
        assert_eq!(q["t"], json!([10_000, 20_000, 30_000]));
        assert_eq!(series(&q, "sst_tables").len(), 3);
    }

    #[test]
    fn short_names() {
        assert_eq!(short_name("autumn_ps_partition_flushes_total"), "flushes");
        assert_eq!(short_name("autumn_ps_block_cache_hits_total"), "block_cache_hits");
        assert_eq!(short_name("autumn_ps_get_duration_seconds_bucket"), "get_duration_seconds_bucket");
    }
}
