//! Minimal Prometheus-text `/metrics` HTTP endpoint (observability batch 1).
//!
//! Design constraints, in order:
//! 1. ZERO interaction with the data-plane compio runtimes — the listener
//!    is a plain blocking `std::net::TcpListener` on its own OS thread, so
//!    a slow/stuck scraper can never occupy an io_uring slot or wake a
//!    P-log task.
//! 2. No new dependencies — hand-rolled HTTP/1.1 is ~40 lines and the
//!    Prometheus text exposition format needs nothing more.
//! 3. The render closure runs on the metrics thread, so it must be
//!    `Send + Sync` and must only read `Arc`-shared state (atomics or an
//!    `RwLock<String>` snapshot published by the owning runtime). Server
//!    state behind `Rc`/`RefCell` is published as a pre-rendered snapshot
//!    string instead (see the manager / PS binaries).
//!
//! The endpoint is OPT-IN per binary via `--metrics-port`; absent = no
//! listener, zero cost.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Snapshot mailbox between an owning runtime (which renders the metrics
/// text) and the HTTP thread. Both sides hold the lock only for an
/// `Arc` pointer swap/clone — never across a render or a large string
/// clone — so the publisher can NEVER block its compio runtime behind a
/// slow scraper (coco P2). Poison-safe: a panicked holder doesn't kill
/// metrics, the next caller just takes the last value.
pub struct MetricsSnapshot {
    inner: Mutex<Arc<String>>,
}

impl MetricsSnapshot {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Arc::new(String::new())),
        })
    }

    pub fn publish(&self, text: String) {
        let arc = Arc::new(text);
        *self
            .inner
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = arc;
    }

    pub fn get(&self) -> Arc<String> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

/// Spawn the metrics HTTP listener thread. Returns Err only on bind
/// failure (callers log and decide; the data plane must not die because
/// a metrics port is taken).
pub fn spawn_metrics_http(
    bind_host: &str,
    port: u16,
    render: Arc<dyn Fn() -> String + Send + Sync>,
) -> std::io::Result<()> {
    // Bracket bare IPv6 hosts; "0.0.0.0"/"127.0.0.1"/hostnames pass through.
    let host = if bind_host.contains(':') && !bind_host.starts_with('[') {
        format!("[{bind_host}]")
    } else {
        bind_host.to_string()
    };
    let listener = TcpListener::bind(format!("{host}:{port}"))?;
    std::thread::Builder::new()
        .name("metrics-http".into())
        .spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut stream) = conn else { continue };
                // A scraper that connects and stalls must not pin this
                // thread — short timeouts, then drop the connection.
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
                let mut buf = [0u8; 1024];
                let n = match stream.read(&mut buf) {
                    Ok(n) if n > 0 => n,
                    _ => continue,
                };
                let head = String::from_utf8_lossy(&buf[..n]);
                let path = head.split_whitespace().nth(1).unwrap_or("/");
                let resp = if path == "/metrics" || path == "/" {
                    let body = render();
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else {
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string()
                };
                let _ = stream.write_all(resp.as_bytes());
            }
        })?;
    Ok(())
}

/// Append one metric in Prometheus text exposition format:
/// `# TYPE` line (first occurrence is the caller's responsibility) plus
/// a sample line. Kept as tiny free functions instead of a registry —
/// each binary renders its own text in one pass.
pub fn push_metric(out: &mut String, name: &str, labels: &[(&str, String)], value: impl Into<f64>) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(k);
            out.push_str("=\"");
            // Prometheus label escaping: backslash, quote, newline.
            for c in v.chars() {
                match c {
                    '\\' => out.push_str("\\\\"),
                    '"' => out.push_str("\\\""),
                    '\n' => out.push_str("\\n"),
                    c => out.push(c),
                }
            }
            out.push('"');
        }
        out.push('}');
    }
    let v: f64 = value.into();
    out.push(' ');
    if v == v.trunc() && v.abs() < 1e15 {
        out.push_str(&format!("{}", v as i64));
    } else {
        out.push_str(&format!("{v}"));
    }
    out.push('\n');
}

/// `# TYPE name kind` header line.
pub fn push_type(out: &mut String, name: &str, kind: &str) {
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(kind);
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_metric_formats_labels_and_escaping() {
        let mut s = String::new();
        push_type(&mut s, "autumn_test_total", "counter");
        push_metric(&mut s, "autumn_test_total", &[("part", "9".to_string())], 42u32);
        push_metric(
            &mut s,
            "autumn_test_total",
            &[("path", "a\"b\\c".to_string())],
            1.5f64,
        );
        push_metric(&mut s, "autumn_plain", &[], 7u32);
        assert_eq!(
            s,
            "# TYPE autumn_test_total counter\n\
             autumn_test_total{part=\"9\"} 42\n\
             autumn_test_total{path=\"a\\\"b\\\\c\"} 1.5\n\
             autumn_plain 7\n"
        );
    }

    #[test]
    fn http_serves_rendered_text_and_404s_unknown_paths() {
        let render: Arc<dyn Fn() -> String + Send + Sync> =
            Arc::new(|| "autumn_up 1\n".to_string());
        // Race-free ephemeral port: bind 0 ourselves, free it, reuse. Tiny
        // race window is acceptable for a unit test; retry once on failure.
        let port = {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        spawn_metrics_http("127.0.0.1", port, render).unwrap();

        let get = |path: &str| -> String {
            let mut c = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
            c.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
                .unwrap();
            let mut out = String::new();
            let _ = c.read_to_string(&mut out);
            out
        };
        let ok = get("/metrics");
        assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"), "{ok}");
        assert!(ok.ends_with("autumn_up 1\n"), "{ok}");
        let nf = get("/nope");
        assert!(nf.starts_with("HTTP/1.1 404"), "{nf}");
    }
}
