use anyhow::{Context, Result, anyhow};
use clap::Parser;
use rand::Rng;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, UdpSocket},
    time::Duration,
};
use url::Url;

const UDP_PORT_START: u16 = 10000;
const UDP_PORT_END: u16 = 65535;
const RANDOM_UDP_BINDS: usize = 200;

#[derive(Clone, Copy, Debug)]
enum TransportMode {
    Tcp,
    Udp,
}

impl FromStr for TransportMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "tcp" => Ok(Self::Tcp),
            "udp" => Ok(Self::Udp),
            other => Err(anyhow!("unsupported transport '{}'", other)),
        }
    }
}

fn parse_transport(val: &str) -> std::result::Result<TransportMode, String> {
    TransportMode::from_str(val).map_err(|e| e.to_string())
}

#[derive(Clone, Debug, Parser)]
#[command(name = "rtsp_client", about = "Lightweight RTSP pull tester")]
struct ClientConfig {
    /// RTSP URL to pull
    #[arg(long, default_value = "rtsp://127.0.0.1:9090/app/streamps")]
    url: String,

    /// Transport mode: tcp or udp
    #[arg(long, value_parser = parse_transport, default_value = "udp")]
    transport: TransportMode,

    /// Number of concurrent clients
    #[arg(long, default_value_t = 1)]
    client_count: usize,

    /// Interval between launching clients (ms)
    #[arg(long, default_value_t = 1)]
    launch_interval_ms: u64,
}

impl ClientConfig {
    fn parsed() -> Result<Self> {
        Ok(ClientConfig::parse())
    }
}

#[derive(Debug)]
struct RtspResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

#[derive(Default)]
struct Metrics {
    active_clients: AtomicUsize,
    total_bytes: AtomicU64,
}

impl Metrics {
    fn record_bytes(&self, bytes: usize) {
        self.total_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

struct ActiveClientGuard {
    metrics: Arc<Metrics>,
}

impl ActiveClientGuard {
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.active_clients.fetch_add(1, Ordering::SeqCst);
        Self { metrics }
    }
}

impl Drop for ActiveClientGuard {
    fn drop(&mut self) {
        self.metrics.active_clients.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let config = ClientConfig::parsed()?;
    tracing::info!(
        "Starting RTSP client example: url={}, transport={:?}, clients={}, launch_interval_ms={}",
        config.url,
        config.transport,
        config.client_count,
        config.launch_interval_ms
    );

    let metrics = Arc::new(Metrics::default());
    let logger_handle = tokio::spawn(metrics_logger(metrics.clone()));

    let udp_port_pool = Arc::new(PortPool::new(config.client_count));

    let cancel_token = tokio_util::sync::CancellationToken::new();

    let mut tasks = Vec::new();
    for idx in 0..config.client_count {
        if config.launch_interval_ms > 0 && idx > 0 {
            tokio::time::sleep(Duration::from_millis(config.launch_interval_ms)).await;
        }
        let cfg = config.clone();
        let metrics = metrics.clone();
        let token = cancel_token.clone(); // 传递 token
        let port_pool = udp_port_pool.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(err) = run_single_client(idx, cfg, metrics, token, port_pool).await {
                tracing::error!(client = idx, error = %err, "client failed");
            }
        }));
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down clients...");
            cancel_token.cancel();
        }
        _ = async {
             for task in tasks {
                 let _ = task.await;
             }
        } => {}
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    logger_handle.abort();

    Ok(())
}

async fn run_single_client(
    client_id: usize,
    config: ClientConfig,
    metrics: Arc<Metrics>,
    token: tokio_util::sync::CancellationToken,
    port_pool: Arc<PortPool>,
) -> Result<()> {
    let url = Url::parse(&config.url).context("invalid RTSP URL")?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("RTSP URL must include host"))?;
    let port = url.port_or_known_default().unwrap_or(554);

    tracing::info!(client = client_id, "connecting to {}:{}", host, port);
    let stream = TcpStream::connect((host, port)).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut cseq = 1u32;
    let mut client_guard: Option<ActiveClientGuard> = None;

    send_request(
        &mut writer,
        "OPTIONS",
        url.as_str(),
        &mut cseq,
        &[("User-Agent", "dm-live-example/0.1".to_string())],
        None,
    )
    .await?;
    let _ = read_rtsp_response(&mut reader).await?;

    send_request(
        &mut writer,
        "DESCRIBE",
        url.as_str(),
        &mut cseq,
        &[
            ("Accept", "application/sdp".to_string()),
            ("User-Agent", "dm-live-example/0.1".to_string()),
        ],
        None,
    )
    .await?;
    let describe = read_rtsp_response(&mut reader).await?;
    let track_controls = parse_sdp_controls(&describe.body);
    let content_base = describe
        .headers
        .get("Content-Base")
        .cloned()
        .unwrap_or_else(|| config.url.clone());

    let mut setup_targets: Vec<String> = if track_controls.is_empty() {
        vec![url.as_str().to_string()]
    } else {
        track_controls
            .iter()
            .filter_map(
                |control| match resolve_control_url(&content_base, control, &url) {
                    Ok(resolved) => Some(resolved),
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to resolve control url");
                        None
                    }
                },
            )
            .collect()
    };

    if setup_targets.is_empty() {
        setup_targets.push(url.as_str().to_string());
    }

    struct TrackContext {
        transport: String,
        control_url: String,
        udp_socket: Option<Arc<UdpSocket>>,
    }
    let mut track_contexts = Vec::new();
    for (idx, target) in setup_targets.into_iter().enumerate() {
        let (transport, socket) = match config.transport {
            TransportMode::Tcp => {
                let base_channel = idx * 2;
                (
                    format!(
                        "RTP/AVP/TCP;unicast;interleaved={}-{}",
                        base_channel,
                        base_channel + 1
                    ),
                    None,
                )
            }
            TransportMode::Udp => {
                let (socket, rtp_port, rtcp_port) = bind_udp_pair(&port_pool).await?;
                (
                    format!("RTP/AVP;unicast;client_port={}-{}", rtp_port, rtcp_port),
                    Some(socket),
                )
            }
        };
        track_contexts.push(TrackContext {
            transport,
            control_url: target,
            udp_socket: socket,
        });
    }

    let mut session_id: Option<String> = None;
    for ctx in &track_contexts {
        let mut headers = vec![
            ("Transport", ctx.transport.clone()),
            ("User-Agent", "dm-live-example/0.1".to_string()),
        ];
        if let Some(ref session) = session_id {
            headers.push(("Session", session.clone()));
        }

        send_request(
            &mut writer,
            "SETUP",
            &ctx.control_url,
            &mut cseq,
            &headers,
            None,
        )
        .await?;
        let setup_response = read_rtsp_response(&mut reader).await?;
        if session_id.is_none() {
            if let Some(header) = setup_response.headers.get("Session") {
                session_id = Some(header.clone());
            }
        }
    }

    let session_id = session_id.unwrap_or_else(|| "default-session".to_string());

    send_request(
        &mut writer,
        "PLAY",
        url.as_str(),
        &mut cseq,
        &[
            ("Session", session_id.clone()),
            ("User-Agent", "dm-live-example/0.1".to_string()),
        ],
        None,
    )
    .await?;
    let _ = read_rtsp_response(&mut reader).await?;

    client_guard = Some(ActiveClientGuard::new(metrics.clone()));

    tracing::info!(client = client_id, "RTSP session started");

    let drain_future = async {
        match config.transport {
            TransportMode::Tcp => drain_stream(reader, metrics.clone()).await,
            TransportMode::Udp => {
                let mut udp_handles = Vec::new();
                for ctx in track_contexts {
                    if let Some(socket) = ctx.udp_socket {
                        let metrics = metrics.clone();
                        udp_handles.push(tokio::spawn(async move {
                            if let Err(err) = drain_udp(socket, metrics).await {
                                tracing::warn!(?err, "UDP drain finished");
                            }
                        }));
                    }
                }
                let result = drain_stream(reader, metrics.clone()).await;

                for handle in udp_handles {
                    handle.abort();
                }

                result
            }
        }
    };

    tokio::select! {
        res = drain_future => res?,
        _ = token.cancelled() => {
            tracing::info!(client = client_id, "Stopping, sending TEARDOWN...");
            // 发送 TEARDOWN
            let _ = send_request(
                &mut writer,
                "TEARDOWN",
                url.as_str(),
                &mut cseq,
                &[("Session", session_id), ("User-Agent", "dm-live-example/0.1".to_string())],
                None,
            ).await;
        }
    }

    Ok(())
}

async fn send_request(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    method: &str,
    url: &str,
    cseq: &mut u32,
    headers: &[(&str, String)],
    body: Option<&str>,
) -> Result<()> {
    let mut request = format!("{method} {url} RTSP/1.0\r\nCSeq: {}\r\n", cseq);
    *cseq += 1;
    for (key, value) in headers {
        request.push_str(&format!("{key}: {value}\r\n"));
    }
    if let Some(body) = body {
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
        request.push_str("\r\n");
        request.push_str(body);
    } else {
        request.push_str("\r\n");
    }

    writer.write_all(request.as_bytes()).await?;
    Ok(())
}

async fn read_rtsp_response(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<RtspResponse> {
    let mut status_line = String::new();
    let bytes = reader.read_line(&mut status_line).await?;
    if bytes == 0 {
        return Err(anyhow!("unexpected EOF when reading RTSP status line"));
    }
    if !status_line.starts_with("RTSP/1.0") {
        return Err(anyhow!("invalid RTSP status line: {status_line:?}"));
    }
    let mut parts = status_line.split_whitespace();
    let _ = parts.next();
    let status = parts
        .next()
        .ok_or_else(|| anyhow!("missing status code"))?
        .parse::<u16>()
        .context("invalid RTSP status code")?;

    let mut headers = HashMap::new();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 || line == "\r\n" {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            let key_trimmed = key.trim().to_string();
            let value_trimmed = value.trim().to_string();
            if key_trimmed.eq_ignore_ascii_case("Content-Length") {
                content_length = value_trimmed.parse().unwrap_or(0);
            }
            headers.insert(key_trimmed, value_trimmed);
        }
    }

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).await?;
    }

    Ok(RtspResponse {
        status,
        headers,
        body,
    })
}

fn parse_sdp_controls(body: &[u8]) -> Vec<String> {
    let sdp = String::from_utf8_lossy(body);
    let mut controls = Vec::new();
    let mut in_media_section = false;

    for raw_line in sdp.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("m=") {
            in_media_section = true;
        }

        if in_media_section {
            if let Some(rest) = line.strip_prefix("a=control:") {
                controls.push(rest.trim().to_string());
            }
        }
    }

    controls
}

fn resolve_control_url(base: &str, control: &str, request_url: &Url) -> Result<String> {
    if control.starts_with("rtsp://") || control.starts_with("rtspu://") {
        return Ok(control.to_string());
    }

    if control == "*" {
        return Ok(base.to_string());
    }

    let base_url = Url::parse(base).unwrap_or_else(|_| request_url.clone());
    let resolved = base_url
        .join(control)
        .map_err(|err| anyhow!("failed to resolve control url: {}", err))?;

    Ok(resolved.to_string())
}

async fn drain_stream(
    mut reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    metrics: Arc<Metrics>,
) -> Result<()> {
    let mut buffer = [0u8; 2048];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        metrics.record_bytes(read);
    }
    Ok(())
}

async fn drain_udp(socket: Arc<UdpSocket>, metrics: Arc<Metrics>) -> Result<()> {
    let mut buf = [0u8; 1500];
    loop {
        let (size, _) = socket.recv_from(&mut buf).await?;
        metrics.record_bytes(size);
    }
}

struct PortPool {
    start: u16,
    end: u16,
    total_pairs: usize,
    used: Mutex<HashSet<u16>>,
}

impl PortPool {
    fn new(_client_count: usize) -> Self {
        let start = if UDP_PORT_START % 2 == 0 {
            UDP_PORT_START
        } else {
            UDP_PORT_START.saturating_add(1)
        };
        // end 作为上界，rtp 取偶数，rtcp = rtp + 1
        let end = if UDP_PORT_END % 2 == 1 {
            UDP_PORT_END
        } else {
            UDP_PORT_END.saturating_sub(1)
        };

        let total_pairs = if end > start {
            ((end - start) / 2 + 1) as usize
        } else {
            1
        };

        Self {
            start,
            end,
            total_pairs,
            used: Mutex::new(HashSet::new()),
        }
    }

    fn next_pair(&self) -> Option<(u16, u16)> {
        let mut used = self.used.lock().unwrap();
        if used.len() / 2 >= self.total_pairs {
            return None;
        }

        let max_iter = self.total_pairs.saturating_mul(4).max(8);
        let mut rng = rand::thread_rng();
        for _ in 0..max_iter {
            let idx = rng.gen_range(0..self.total_pairs) as u16;
            let rtp_port = self.start + idx.saturating_mul(2);
            let rtcp_port = rtp_port + 1;
            if !used.contains(&rtp_port) && !used.contains(&rtcp_port) {
                used.insert(rtp_port);
                used.insert(rtcp_port);
                return Some((rtp_port, rtcp_port));
            }
        }
        None
    }

    fn range(&self) -> (u16, u16) {
        (self.start, self.end)
    }
}

async fn bind_udp_pair(port_pool: &PortPool) -> Result<(Arc<UdpSocket>, u16, u16)> {
    while let Some((rtp_port, rtcp_port)) = port_pool.next_pair() {
        match bind_explicit_udp_pair(rtp_port, rtcp_port).await {
            Ok(pair) => return Ok(pair),
            Err(err) => {
                tracing::debug!(
                    rtp_port,
                    rtcp_port,
                    error = %err,
                    "failed to bind UDP ports from pool, trying another"
                );
                continue;
            }
        }
    }

    for attempt in 1..=RANDOM_UDP_BINDS {
        match bind_random_udp_pair().await {
            Ok(pair) => return Ok(pair),
            Err(err) => {
                tracing::warn!(attempt, error = %err, "random UDP bind attempt failed");
            }
        }
    }

    let (start, end) = port_pool.range();
    Err(anyhow!(
        "failed to bind UDP port pair in range {start}-{end} and after {RANDOM_UDP_BINDS} random attempts"
    ))
}

async fn bind_explicit_udp_pair(
    rtp_port: u16,
    rtcp_port: u16,
) -> Result<(Arc<UdpSocket>, u16, u16)> {
    match (
        UdpSocket::bind(("0.0.0.0", rtp_port)).await,
        UdpSocket::bind(("0.0.0.0", rtcp_port)).await,
    ) {
        (Ok(rtp_socket), Ok(rtcp_socket)) => {
            drop(rtcp_socket);
            Ok((Arc::new(rtp_socket), rtp_port, rtcp_port))
        }
        (Err(err), _) | (_, Err(err)) => Err(anyhow!(err)),
    }
}

async fn bind_random_udp_pair() -> Result<(Arc<UdpSocket>, u16, u16)> {
    let rtp_socket = UdpSocket::bind(("0.0.0.0", 0)).await?;
    let rtp_port = rtp_socket
        .local_addr()
        .context("failed to read random RTP local port")?
        .port();

    if rtp_port % 2 != 0 {
        return Err(anyhow!(
            "system assigned odd RTP port {rtp_port}, retrying for even/odd pair"
        ));
    }

    let rtcp_port = rtp_port
        .checked_add(1)
        .ok_or_else(|| anyhow!("RTP port overflowed when computing RTCP port"))?;

    match UdpSocket::bind(("0.0.0.0", rtcp_port)).await {
        Ok(rtcp_socket) => {
            drop(rtcp_socket);
            Ok((Arc::new(rtp_socket), rtp_port, rtcp_port))
        }
        Err(err) => Err(anyhow!(err)),
    }
}

async fn metrics_logger(metrics: Arc<Metrics>) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    let mut prev_total = 0u64;
    loop {
        interval.tick().await;
        let total = metrics.total_bytes.load(Ordering::Relaxed);
        let delta = total.saturating_sub(prev_total);
        prev_total = total;
        let throughput_per_sec = delta as f64 / 5.0;
        let active = metrics.active_clients.load(Ordering::Relaxed);
        let per_player = if active > 0 {
            throughput_per_sec / active as f64
        } else {
            0.0
        };
        println!(
            "[Metrics] active_players={}, total_throughput={:.2} KB/s, per_player={:.2} KB/s",
            active,
            throughput_per_sec / 1024.0,
            per_player / 1024.0
        );
    }
}
