//! Minimal Prometheus-text `/metrics` HTTP endpoint (observability batch 1).
//!
//! Design constraints, in order:
//! 1. ZERO interaction with the data-plane runtimes — the listener runs on
//!    its own thread with its own compio runtime, pinned to no core, so a
//!    slow scraper never occupies a data-plane io_uring or wakes a P-log
//!    task.
//! 2. No new dependencies — hand-rolled HTTP/1.1 is ~40 lines and the
//!    Prometheus text exposition format needs nothing more.
//! 3. The render closure runs on that thread, so it must be `Send + Sync`
//!    and read only `Arc`-shared state (atomics) — never a runtime's
//!    `Rc`/`RefCell` state.
//!
//! Every PS and EN serves `/metrics` by default on its service port plus
//! `METRICS_PORT_OFFSET` (`--metrics-port` overrides, 0 turns it off). The
//! manager serves none; `autumn-op info --file-sd` lists these endpoints for
//! Prometheus.

use std::sync::Arc;
use std::time::Duration;

/// A PS or EN serves `/metrics` on its service port plus this, unless
/// `--metrics-port` says otherwise. Clear of the EN shard grid
/// (9100 + i + shard·stride) and of the PS partition listeners
/// (`--port` + ord) while a PS holds fewer than 500 partitions.
pub const METRICS_PORT_OFFSET: u16 = 500;

/// The `/metrics` port for a process on `service_port`: `flag` when given
/// (0 = none), else `service_port + METRICS_PORT_OFFSET`.
pub fn metrics_port(flag: Option<u16>, service_port: u16) -> Option<u16> {
    match flag {
        Some(0) => None,
        Some(p) => Some(p),
        None => service_port.checked_add(METRICS_PORT_OFFSET),
    }
}

/// Where others reach a `/metrics` bound on `bind_host:port`: that host when
/// it names one, else — bound to every address — the advertised host.
pub fn metrics_reach_addr(bind_host: &str, advertise_host: &str, port: u16) -> String {
    let host = match bind_host {
        h if !matches!(h, "0.0.0.0" | "::" | "[::]") => {
            if h.contains(':') && !h.starts_with('[') {
                format!("[{h}]")
            } else {
                h.to_string()
            }
        }
        _ => advertise_host.to_string(),
    };
    format!("{host}:{port}")
}

/// A request head longer than this is dropped unanswered.
const MAX_REQUEST_HEAD: usize = 8192;

pub type Render = Arc<dyn Fn() -> String + Send + Sync>;

/// Spawn the HTTP listener thread serving `render` at `/metrics`; any other
/// path is a 404. Returns Err on bind or runtime failure (callers log and
/// decide; the data plane must not die because a metrics port is taken).
pub fn spawn_metrics_http(bind_host: &str, port: u16, render: Render) -> std::io::Result<()> {
    // Bracket bare IPv6 hosts; "0.0.0.0"/"127.0.0.1"/hostnames pass through.
    let host = if bind_host.contains(':') && !bind_host.starts_with('[') {
        format!("[{bind_host}]")
    } else {
        bind_host.to_string()
    };
    // Bound here so a taken port is the caller's error, not the thread's.
    let listener = std::net::TcpListener::bind(format!("{host}:{port}"))?;
    // The runtime is built on its own thread; it reports back whether it
    // came up so a failure is the caller's error too.
    let (up_tx, up_rx) = std::sync::mpsc::sync_channel::<std::io::Result<()>>(1);
    std::thread::Builder::new()
        .name("metrics-http".into())
        .spawn(move || {
            let rt = match compio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    up_tx.send(Err(e)).expect("caller waits for the startup result");
                    return;
                }
            };
            rt.block_on(async move {
                let listener = match compio::net::TcpListener::from_std(listener) {
                    Ok(l) => l,
                    Err(e) => {
                        up_tx.send(Err(e)).expect("caller waits for the startup result");
                        return;
                    }
                };
                up_tx.send(Ok(())).expect("caller waits for the startup result");
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            compio::runtime::spawn(serve_one(stream, render.clone())).detach();
                        }
                        Err(e) => {
                            // EMFILE and the like: back off instead of spinning.
                            tracing::debug!("metrics accept: {e}");
                            compio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            })
        })?;
    up_rx
        .recv()
        .map_err(|_| std::io::Error::other("metrics thread exited before starting"))?
}

/// Answer one request and close. A scraper that stalls is dropped after a
/// short timeout.
async fn serve_one(mut stream: compio::net::TcpStream, render: Render) {
    use compio::buf::BufResult;
    use compio::io::{AsyncRead, AsyncWriteExt};

    // Read the whole request head: closing with part of it unread makes the
    // kernel answer with a reset, which can discard the reply in flight.
    let read_head = async {
        let mut head = Vec::with_capacity(1024);
        loop {
            if head.len() >= MAX_REQUEST_HEAD {
                return None;
            }
            let BufResult(r, buf) = stream.read(Vec::with_capacity(1024)).await;
            match r {
                Ok(0) => return None,
                Ok(_) => {
                    // Only new bytes are scanned, plus the last 3 old ones in
                    // case the blank line straddles two reads.
                    let from = head.len().saturating_sub(3);
                    head.extend_from_slice(&buf);
                    if head[from..].windows(4).any(|w| w == b"\r\n\r\n") {
                        return Some(head);
                    }
                }
                Err(e) => {
                    tracing::debug!("metrics read: {e}");
                    return None;
                }
            }
        }
    };
    let head = match compio::time::timeout(Duration::from_secs(2), read_head).await {
        Ok(Some(head)) => head,
        Ok(None) | Err(_) => return,
    };
    let head = String::from_utf8_lossy(&head);
    let path = head.split_whitespace().nth(1).unwrap_or("/");
    let resp = if path == "/metrics" {
        let body = render();
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    } else {
        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
    };
    match compio::time::timeout(Duration::from_secs(5), stream.write_all(resp.into_bytes())).await {
        Ok(BufResult(Ok(()), _)) | Err(_) => {}
        Ok(BufResult(Err(e), _)) => tracing::debug!("metrics write: {e}"),
    }
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
    use std::io::{Read, Write};

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
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        spawn_metrics_http("127.0.0.1", port, render).unwrap();

        let get = |path: &str| -> String {
            let mut c = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
            c.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
                .unwrap();
            let mut out = String::new();
            c.read_to_string(&mut out).unwrap();
            out
        };
        let ok = get("/metrics");
        assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"), "{ok}");
        assert!(ok.ends_with("autumn_up 1\n"), "{ok}");
        let nf = get("/nope");
        assert!(nf.starts_with("HTTP/1.1 404"), "{nf}");
    }

    #[test]
    fn a_request_arriving_in_pieces_gets_the_whole_reply() {
        // The second piece lands after the server's first read, as across
        // nodes: answering and closing with it unread resets the connection.
        let body = "autumn_x 1\n".repeat(20_000);
        let b = body.clone();
        let render: Render = Arc::new(move || b.clone());
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        spawn_metrics_http("127.0.0.1", port, render).unwrap();
        let mut c = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.write_all(b"GET /metrics HTTP/1.0\r\nHost: ").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        c.write_all(b"10.0.0.1:9801\r\n\r\n").unwrap();
        let mut out = String::new();
        c.read_to_string(&mut out).expect("whole reply, no reset");
        assert!(out.starts_with("HTTP/1.1 200 OK\r\n"), "{}", &out[..out.len().min(80)]);
        assert!(out.ends_with(&body));
    }

    #[test]
    fn a_blank_line_split_across_two_reads_ends_the_head() {
        let render: Render = Arc::new(|| "autumn_up 1\n".to_string());
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        spawn_metrics_http("127.0.0.1", port, render).unwrap();
        let mut c = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.write_all(b"GET /metrics HTTP/1.0\r\n\r").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        c.write_all(b"\n").unwrap();
        let mut out = String::new();
        c.read_to_string(&mut out).unwrap();
        assert!(out.starts_with("HTTP/1.1 200 OK\r\n"), "{out}");
    }

    #[test]
    fn the_metrics_port_follows_the_service_port_unless_given() {
        assert_eq!(metrics_port(None, 9301), Some(9801));
        assert_eq!(metrics_port(Some(9000), 9301), Some(9000));
        assert_eq!(metrics_port(Some(0), 9301), None);
        assert_eq!(metrics_port(None, 65500), None);
    }

    #[test]
    fn a_specific_bind_host_is_reported_and_a_wildcard_falls_back_to_the_advertised_one() {
        assert_eq!(metrics_reach_addr("10.0.0.5", "1.2.3.4", 9701), "10.0.0.5:9701");
        assert_eq!(metrics_reach_addr("0.0.0.0", "1.2.3.4", 9701), "1.2.3.4:9701");
        assert_eq!(metrics_reach_addr("::", "[fd00::1]", 9701), "[fd00::1]:9701");
        assert_eq!(metrics_reach_addr("fd00::2", "x", 9701), "[fd00::2]:9701");
    }
}
