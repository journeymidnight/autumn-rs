use anyhow::{Context, Result};
use autumn_manager::AutumnManager;
use autumn_transport::TransportKind;

struct Args {
    port: u16,
    etcd: Vec<String>,
    bind_host: String,
    transport: TransportKind,
    /// F184: Stage 2 — auto-dispatch SPLIT to owning PS based on
    /// policy-engine candidates. Default off.
    auto_split: bool,
    /// F184: Stage 3 — auto-orchestrate MERGE for same-PS adjacent
    /// candidates. Default off.
    auto_merge: bool,
}

fn parse_args() -> Args {
    let mut port: u16 = 9001;
    let mut etcd: Vec<String> = Vec::new();
    let mut bind_host = String::from("0.0.0.0");
    let mut transport = TransportKind::Tcp;
    let mut auto_split = false;
    let mut auto_merge = false;

    let raw: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < raw.len() {
        match raw[i].as_str() {
            "--port" => {
                i += 1;
                port = raw[i].parse().expect("--port must be a number");
            }
            "--etcd" => {
                i += 1;
                for ep in raw[i].split(',') {
                    etcd.push(ep.trim().to_string());
                }
            }
            "--listen" => {
                i += 1;
                bind_host = raw[i].clone();
            }
            "--transport" => {
                i += 1;
                transport = autumn_transport::parse_transport_flag(&raw[i])
                    .unwrap_or_else(|bad| {
                        eprintln!("--transport must be `tcp` or `ucx`, got {bad:?}");
                        std::process::exit(2);
                    });
            }
            "--auto-split" => auto_split = true,
            "--auto-merge" => auto_merge = true,
            other => eprintln!("unknown arg: {other}"),
        }
        i += 1;
    }

    Args { port, etcd, bind_host, transport, auto_split, auto_merge }
}

#[compio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args();
    let _ = autumn_transport::init_with(args.transport);
    let addr = autumn_transport::format_listen_addr(&args.bind_host, args.port)
        .context("parse listen address")?;
    autumn_transport::check_listen_addr(addr, autumn_transport::current().kind())
        .ok();

    let manager = if args.etcd.is_empty() {
        tracing::warn!(
            "no --etcd endpoints given; running in-memory only (metadata will be lost on restart)"
        );
        AutumnManager::new()
    } else {
        tracing::info!("connecting to etcd: {:?}", args.etcd);
        AutumnManager::new_with_etcd(args.etcd)
            .await
            .context("connect to etcd")?
    };

    if args.auto_split {
        tracing::warn!("F184: --auto-split enabled; manager will dispatch SPLIT for hot partitions");
        manager.set_auto_split(true);
    }
    if args.auto_merge {
        tracing::warn!("F184: --auto-merge enabled; manager will orchestrate MERGE for cold same-PS pairs");
        manager.set_auto_merge(true);
    }

    tracing::info!("autumn-manager-server listening on {addr}");
    manager.serve(addr).await?;

    Ok(())
}
