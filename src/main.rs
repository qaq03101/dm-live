use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use tokio::signal;
use tracing::{info, subscriber::NoSubscriber};
use tracing_chrome::{ChromeLayerBuilder, TraceStyle};
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

use dm_live::config;
use dm_live::server::{RtspServer, rtsp_server::RtspServerConfig};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn init_tracing_for_chrome() -> Option<tracing_chrome::FlushGuard> {
    let (chrome_layer, guard) = ChromeLayerBuilder::new()
        .trace_style(TraceStyle::Async) // async
        .include_args(true) //include span
        .file("trace.json")
        .build();

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,my_rtsp=trace"));

    let subscriber = Registry::default().with(filter).with(chrome_layer);

    tracing::subscriber::set_global_default(subscriber).expect("set global subscriber");
    Some(guard)
}

fn init_tracing() -> Option<tracing_chrome::FlushGuard> {
    match std::env::var("RUST_TRACE") {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "chrome" => init_tracing_for_chrome(),
            "off" | "none" | "disable" => {
                tracing::subscriber::set_global_default(NoSubscriber::default())
                    .expect("set noop subscriber");
                None
            }
            other => {
                if let Some(level) = normalize_level(other) {
                    install_fmt_with_filter(
                        EnvFilter::try_new(&level).unwrap_or_else(|_| EnvFilter::new("warn")),
                    );
                } else {
                    install_fmt_with_filter(default_filter());
                }
                None
            }
        },
        Err(_) => {
            install_fmt_with_filter(default_filter());
            None
        }
    }
}

fn default_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"))
}

fn install_fmt_with_filter(filter: EnvFilter) {
    fmt::fmt()
        .with_env_filter(filter)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_file(true)
        .init();
}

fn normalize_level(value: &str) -> Option<String> {
    match value {
        "trace" | "debug" | "info" | "warn" | "error" => Some(value.to_string()),
        _ => None,
    }
}

#[derive(Parser, Debug)]
#[command(name = "dm-live", version = "0.1.0", about = "RTSP Server")]
struct Cli {
    #[arg(long, default_value = "config.toml", help = "配置文件路径")]
    config: String,

    #[arg(long, help = "服务监听IP或完整地址")]
    ip: Option<String>,

    #[arg(long, help = "服务端口")]
    port: Option<u16>,

    #[arg(long, help = "工作线程数")]
    workers: Option<usize>,

    #[arg(long, help = "最大连接数")]
    max_connections: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let _guard = init_tracing();

    let args = Cli::parse();
    let settings = config::init(Some(&args.config))?;

    let server_cfg = &settings.server;
    let bind_ip = args.ip.unwrap_or_else(|| server_cfg.bind_ip.clone());
    let port = args.port.unwrap_or(server_cfg.port);

    let bind_addr: SocketAddr = match bind_ip.parse::<SocketAddr>() {
        Ok(addr) => addr,
        Err(_) => format!("{}:{}", bind_ip, port).parse::<SocketAddr>()?,
    };

    let config = RtspServerConfig {
        bind_addr,
        max_connections: args.max_connections.unwrap_or(server_cfg.max_connections),
        worker_count: args.workers.or(server_cfg.workers),
    };

    let server = RtspServer::new(config);

    tokio::select! {
        res = server.start() => {
            res?;
        }
        _ = signal::ctrl_c() => {
            info!("shutting down server");
        }
    }

    Ok(())
}
