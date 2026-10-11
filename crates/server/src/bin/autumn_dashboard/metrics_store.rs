//! In-memory history of every partition server's `/metrics`.
//!
//! The poller scrapes all PSes once per round. Every series is a column of one
//! shared round axis: a series stores one `f32` per round from its first
//! (NaN = no value that round), so series from different PSes and partitions
//! line up and add up point for point, at 4 bytes a point. Counters are stored
//! as per-second rates (a counter that went down was reset by a restart or
//! reopen and yields no point); gauges as read. Nothing is persisted: a
//! dashboard restart starts an empty history.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use serde_json::{json, Map, Value};

/// One series: a metric of one PS, PS-wide (`part == None`) or one partition's.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Key {
    ps: u64,
    part: Option<u64>,
    /// Short metric name plus its other labels, e.g.
    /// `minor_compactions{trigger=flush}`.
    name: String,
}

struct Series {
    counter: bool,
    /// Absolute round index of `values[0]`.
    start: u64,
    values: VecDeque<f32>,
    /// The counter's last raw reading `(t_ms, value)`, to rate the next one.
    last_raw: Option<(u64, f64)>,
}

/// The latest scrape of one PS, for the page's endpoint list.
#[derive(Clone, Debug, Default)]
pub struct PsStatus {
    pub metrics_addr: Option<String>,
    pub last_ok_ms: Option<u64>,
    pub error: Option<String>,
}

pub enum Scope {
    Cluster,
    Ps(u64),
    Part(u64),
}

pub struct MetricsStore {
    retention_ms: u64,
    interval: Duration,
    /// Round start times; `rounds[0]` has absolute index `first_round`.
    rounds: VecDeque<u64>,
    first_round: u64,
    /// Per round, the PSes whose scrape failed: a sum over a scope missing one
    /// of them would read as a drop, so such a round is left out of it.
    failed: VecDeque<Vec<u64>>,
    series: HashMap<Key, Series>,
    ps: BTreeMap<u64, PsStatus>,
}

/// One parsed sample line.
struct Sample {
    name: String,
    labels: Vec<(String, String)>,
    value: f64,
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

    /// Start a scrape round at `t_ms` and drop what has aged out.
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

    /// The PS could not be scraped this round.
    pub fn record_failure(&mut self, ps_id: u64) {
        if let Some(f) = self.failed.back_mut() {
            f.push(ps_id);
        }
    }

    /// Forget PSes no longer registered.
    pub fn retain_ps(&mut self, live: &[u64]) {
        self.ps.retain(|id, _| live.contains(id));
    }

    pub fn set_status(&mut self, ps_id: u64, status: PsStatus) {
        self.ps.insert(ps_id, status);
    }

    pub fn status(&self, ps_id: u64) -> Option<&PsStatus> {
        self.ps.get(&ps_id)
    }

    /// Fold one PS's `/metrics` text into the current round, taken at `t_ms`.
    pub fn ingest(&mut self, ps_id: u64, t_ms: u64, text: &str) -> usize {
        let mut counters: HashMap<String, bool> = HashMap::new();
        let mut n = 0;
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let mut it = rest.split_whitespace();
                if let (Some(family), Some(kind)) = (it.next(), it.next()) {
                    counters.insert(family.to_string(), kind == "counter" || kind == "histogram");
                }
                continue;
            }
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some(s) = parse_sample(line) else { continue };
            let family = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|sfx| s.name.strip_suffix(sfx).filter(|f| counters.contains_key(*f)))
                .unwrap_or(&s.name);
            let counter = counters.get(family).copied().unwrap_or(false);
            let mut part = None;
            let mut extra = Vec::new();
            for (k, v) in &s.labels {
                match k.as_str() {
                    "ps" => {}
                    "part" => part = v.parse().ok(),
                    _ => extra.push(format!("{k}={v}")),
                }
            }
            let mut name = short_name(&s.name).to_string();
            if !extra.is_empty() {
                name = format!("{name}{{{}}}", extra.join(","));
            }
            self.record(Key { ps: ps_id, part, name }, counter, t_ms, s.value);
            n += 1;
        }
        n
    }

    fn record(&mut self, key: Key, counter: bool, t_ms: u64, v: f64) {
        let Some(idx) = (self.first_round + self.rounds.len() as u64).checked_sub(1) else {
            return;
        };
        let s = self.series.entry(key).or_insert_with(|| Series {
            counter,
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

    /// The scope's series, summed over the PSes / partitions it covers, on the
    /// common round axis. A partition gauge also gets `name:max`, the largest
    /// single partition's value. `null` = nothing reported at that round, or a
    /// PS of the scope could not be scraped then.
    pub fn query(&self, scope: &Scope) -> Value {
        let len = self.rounds.len();
        let mut sums: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();
        let mut maxes: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();
        for (k, s) in &self.series {
            let wanted = match scope {
                Scope::Cluster => true,
                Scope::Ps(id) => k.ps == *id,
                Scope::Part(id) => k.part == Some(*id),
            };
            if !wanted {
                continue;
            }
            let off = (s.start - self.first_round) as usize;
            let sum = sums.entry(k.name.clone()).or_insert_with(|| vec![None; len]);
            for (i, v) in s.values.iter().enumerate() {
                if !v.is_nan() {
                    let slot = &mut sum[off + i];
                    *slot = Some(slot.unwrap_or(0.0) + *v as f64);
                }
            }
            if !s.counter && k.part.is_some() && !matches!(scope, Scope::Part(_)) {
                let max = maxes.entry(format!("{}:max", k.name)).or_insert_with(|| vec![None; len]);
                for (i, v) in s.values.iter().enumerate() {
                    if !v.is_nan() {
                        let slot = &mut max[off + i];
                        *slot = Some(slot.map_or(*v as f64, |m| m.max(*v as f64)));
                    }
                }
            }
        }
        let incomplete: Vec<bool> = self
            .failed
            .iter()
            .map(|f| match scope {
                Scope::Cluster => !f.is_empty(),
                Scope::Ps(id) => f.contains(id),
                Scope::Part(_) => false,
            })
            .collect();
        let mut series = Map::new();
        for (name, mut vals) in sums.into_iter().chain(maxes) {
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
            .map(|(id, st)| {
                json!({
                    "ps_id": id,
                    "metrics_addr": st.metrics_addr,
                    "last_ok_ms": st.last_ok_ms,
                    "error": st.error,
                })
            })
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
fn short_name(name: &str) -> &str {
    let n = name
        .strip_prefix("autumn_ps_partition_")
        .or_else(|| name.strip_prefix("autumn_ps_"))
        .unwrap_or(name);
    n.strip_suffix("_total").unwrap_or(n)
}

/// `name{k="v",...} value` (labels optional). Label values here never contain
/// escaped quotes, but a backslash escape is still honoured.
fn parse_sample(line: &str) -> Option<Sample> {
    let (head, value) = line.rsplit_once(' ')?;
    let value: f64 = value.parse().ok()?;
    let (name, labels) = match head.split_once('{') {
        None => (head.to_string(), Vec::new()),
        Some((name, rest)) => {
            let body = rest.strip_suffix('}')?;
            let mut labels = Vec::new();
            let mut chars = body.chars().peekable();
            while chars.peek().is_some() {
                let key: String = chars.by_ref().take_while(|c| *c != '=').collect();
                if chars.next() != Some('"') {
                    return None;
                }
                let mut val = String::new();
                loop {
                    match chars.next()? {
                        '\\' => val.push(chars.next()?),
                        '"' => break,
                        c => val.push(c),
                    }
                }
                labels.push((key.trim_start_matches(',').to_string(), val));
                if chars.peek() == Some(&',') {
                    chars.next();
                }
            }
            (name.to_string(), labels)
        }
    };
    Some(Sample { name, labels, value })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT_A: &str = "\
# TYPE autumn_ps_block_cache_hits_total counter
autumn_ps_block_cache_hits_total{ps=\"1\"} 100
# TYPE autumn_ps_partition_sst_tables gauge
autumn_ps_partition_sst_tables{ps=\"1\",part=\"7\"} 3
autumn_ps_partition_sst_tables{ps=\"1\",part=\"8\"} 5
# TYPE autumn_ps_partition_minor_compactions_total counter
autumn_ps_partition_minor_compactions_total{ps=\"1\",part=\"7\",trigger=\"flush\"} 2
# TYPE autumn_ps_get_duration_seconds histogram
autumn_ps_get_duration_seconds_bucket{ps=\"1\",part=\"7\",le=\"0.0005\"} 10
autumn_ps_get_duration_seconds_count{ps=\"1\",part=\"7\"} 10
";

    /// The series, rounded to 4 decimals (values are stored as `f32`).
    fn series(v: &Value, name: &str) -> Vec<Option<f64>> {
        let raw: Vec<Option<f64>> = serde_json::from_value(v["series"][name].clone())
            .unwrap_or_else(|_| panic!("no {name}: {v}"));
        raw.into_iter().map(|x| x.map(|x| (x * 1e4).round() / 1e4)).collect()
    }

    #[test]
    fn counters_become_rates_and_gauges_sum_and_max() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(3600));
        st.begin_round(10_000);
        st.ingest(1, 10_000, TEXT_A);
        st.begin_round(20_000);
        st.ingest(1, 20_000, &TEXT_A.replace("} 100", "} 150").replace("trigger=\"flush\"} 2", "trigger=\"flush\"} 4"));

        let ps = st.query(&Scope::Ps(1));
        assert_eq!(series(&ps, "block_cache_hits"), vec![None, Some(5.0)]);
        assert_eq!(series(&ps, "minor_compactions{trigger=flush}"), vec![None, Some(0.2)]);
        assert_eq!(series(&ps, "sst_tables"), vec![Some(8.0), Some(8.0)]);
        assert_eq!(series(&ps, "sst_tables:max"), vec![Some(5.0), Some(5.0)]);
        assert_eq!(series(&ps, "get_duration_seconds_bucket{le=0.0005}"), vec![None, Some(0.0)]);

        let part = st.query(&Scope::Part(8));
        assert_eq!(series(&part, "sst_tables"), vec![Some(5.0), Some(5.0)]);
        assert!(part["series"].get("block_cache_hits").is_none(), "PS-wide series in a partition view");
        assert!(part["series"].get("sst_tables:max").is_none());
    }

    #[test]
    fn a_reset_counter_yields_no_point() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(3600));
        for (t, v) in [(10_000u64, "100"), (20_000, "5"), (30_000, "25")] {
            st.begin_round(t);
            st.ingest(1, t, &format!("# TYPE autumn_ps_block_cache_hits_total counter\nautumn_ps_block_cache_hits_total{{ps=\"1\"}} {v}\n"));
        }
        let q = st.query(&Scope::Cluster);
        assert_eq!(series(&q, "block_cache_hits"), vec![None, None, Some(2.0)]);
    }

    #[test]
    fn history_older_than_the_retention_is_dropped() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(20));
        for t in [0u64, 10_000, 20_000, 30_000] {
            st.begin_round(t);
            st.ingest(1, t, TEXT_A);
        }
        let q = st.query(&Scope::Cluster);
        assert_eq!(q["t"], json!([10_000, 20_000, 30_000]));
        assert_eq!(series(&q, "sst_tables").len(), 3);
    }

    #[test]
    fn a_round_missing_a_server_is_left_out_of_its_sums() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(3600));
        let b = TEXT_A.replace("ps=\"1\"", "ps=\"2\"").replace("part=\"7\"", "part=\"9\"").replace("part=\"8\"", "part=\"10\"");
        st.begin_round(10_000);
        st.ingest(1, 10_000, TEXT_A);
        st.ingest(2, 10_000, &b);
        st.begin_round(20_000);
        st.ingest(1, 20_000, TEXT_A);
        st.record_failure(2);
        assert_eq!(series(&st.query(&Scope::Cluster), "sst_tables"), vec![Some(16.0), None]);
        assert_eq!(series(&st.query(&Scope::Ps(1)), "sst_tables"), vec![Some(8.0), Some(8.0)]);
        assert_eq!(series(&st.query(&Scope::Ps(2)), "sst_tables"), vec![Some(8.0), None]);
        assert_eq!(series(&st.query(&Scope::Part(9)), "sst_tables"), vec![Some(3.0), None]);
    }

    #[test]
    fn a_series_that_starts_late_lines_up_with_the_rounds() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(3600));
        st.begin_round(10_000);
        st.begin_round(20_000);
        st.ingest(1, 20_000, TEXT_A);
        assert_eq!(series(&st.query(&Scope::Cluster), "sst_tables"), vec![None, Some(8.0)]);
    }

    #[test]
    fn two_servers_add_up_on_the_shared_round() {
        let mut st = MetricsStore::new(Duration::from_secs(10), Duration::from_secs(3600));
        st.begin_round(10_000);
        st.ingest(1, 10_000, TEXT_A);
        st.ingest(2, 10_000, &TEXT_A.replace("ps=\"1\"", "ps=\"2\"").replace("part=\"7\"", "part=\"9\"").replace("part=\"8\"", "part=\"10\""));
        let q = st.query(&Scope::Cluster);
        assert_eq!(series(&q, "sst_tables"), vec![Some(16.0)]);
    }
}
