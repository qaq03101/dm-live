use anyhow::{Context, Result};
use bytes::Bytes;
use bytes::BytesMut;
use std::{
    collections::HashMap,
    future::pending,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::io::AsyncReadExt;
use tokio::sync::Notify;
use tokio::{io::AsyncWriteExt, net::TcpListener};
use tracing::{Level, error, event, info, instrument, trace_span, warn};
use tracing_futures::Instrument;

use crate::config;
use crate::event::{RuntimeHandle, RuntimePool};

#[derive(Debug, Clone)]
pub struct RtspServerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
    pub worker_count: Option<usize>,
}

impl Default for RtspServerConfig {
    fn default() -> Self {
        let server_cfg = config::get().server.clone();
        let bind_addr = server_cfg
            .bind_ip
            .parse::<SocketAddr>()
            .unwrap_or_else(|_| {
                format!("{}:{}", server_cfg.bind_ip, server_cfg.port)
                    .parse()
                    .unwrap()
            });
        Self {
            bind_addr,
            max_connections: server_cfg.max_connections,
            worker_count: server_cfg.workers,
        }
    }
}

/// RTSP 服务
///
/// 负责：
/// 创建,初始化服务.
pub struct RtspServer {
    /// RTSP 服务器配置
    config: RtspServerConfig,
    /// 活跃连接计数器
    connection_counter: Arc<AtomicUsize>,
    /// 运行时池
    runtime_pool: Arc<RuntimePool>,
}

impl RtspServer {
    /// 创建运行时池
    ///
    /// # Arguments
    /// * `config` - RTSP 服务器配置。
    ///
    /// # Returns
    /// 初始化好的 `RtspServer`
    pub fn new(config: RtspServerConfig) -> Self {
        let runtime_pool = Arc::new(RuntimePool::new(config.worker_count));

        Self {
            config,
            connection_counter: Arc::new(AtomicUsize::new(0)),
            runtime_pool,
        }
    }

    /// 服务初始化并启动
    #[instrument(level = "trace", name = "rtsp_server_running", skip(self))]
    pub async fn start(&self) -> Result<()> {
        info!("Starting RTSP server on {}", self.config.bind_addr);
        info!("Configuration: {:?}", self.config);

        let listener = Arc::new(
            TcpListener::bind(self.config.bind_addr)
                .await
                .with_context(|| format!("Failed to bind to {}", self.config.bind_addr))?,
        );

        info!("RTSP server listening on {}", self.config.bind_addr);

        let handles = self.runtime_pool.handles();

        for handle in handles {
            let listener_clone = listener.clone();
            let runtime_pool = self.runtime_pool.clone();
            let connection_counter = self.connection_counter.clone();
            let config = self.config.clone();
            let runtime_handle = handle.clone();

            handle.spawn_local_factory(move || async move {
                RtspServer::accept_loop_per_runtime(
                    listener_clone,
                    runtime_handle,
                    runtime_pool,
                    connection_counter,
                    config,
                )
                .await;
            });
        }

        // 挂起，保持 server 一直运行
        pending::<()>().await;
        Ok(())
    }

    /// 每个运行时内的连接接受循环
    ///
    /// # Arguments
    /// * `listener` - 共享的 TCP 监听器。
    /// * `runtime_handle` - 当前运行时的句柄。
    /// * `runtime_pool` - 运行时池。
    /// * `connection_counter` - 活跃连接计数器。
    /// * `config` - RTSP 服务器配置。
    #[instrument(
        level = "trace",
        name = "accept_loop_per_runtime",
        skip(listener, runtime_pool, connection_counter, config)
    )]
    async fn accept_loop_per_runtime(
        listener: Arc<TcpListener>,
        runtime_handle: RuntimeHandle,
        runtime_pool: Arc<RuntimePool>,
        connection_counter: Arc<AtomicUsize>,
        config: RtspServerConfig,
    ) {
        info!("Runtime {} accept loop started", runtime_handle.idx());

        loop {
            match listener.accept().await {
                Ok((mut socket, addr)) => {
                    let cur = connection_counter.fetch_add(1, Ordering::Relaxed) + 1;

                    event!(
                        Level::TRACE,
                        kind = "new_connection",
                        runtime = runtime_handle.idx(),
                        %addr,
                        connection_num = cur
                    );

                    if cur > config.max_connections {
                        warn!(
                            "Max connections reached: {}. Reject {}",
                            config.max_connections, addr
                        );
                        let response = b"RTSP/1.0 503 Service Unavailable\r\nCSeq: 1\r\n\r\n";

                        if let Err(e) = socket.write_all(response).await {
                            error!("Failed to send 503 response to {}: {}", addr, e);
                        }

                        connection_counter.fetch_sub(1, Ordering::Relaxed);
                        drop(socket);
                        continue;
                    }

                    let ctr = connection_counter.clone();
                    let pool = runtime_pool.clone();
                    let connection_handle = runtime_handle.clone();
                    let conn_span = trace_span!("handle_connection", addr = %addr, runtime = connection_handle.idx());

                    tokio::task::spawn_local(
                        async move {
                            if let Err(e) =
                                handle_connection_local(socket, addr, ctr, pool, connection_handle)
                                    .await
                            {
                                error!("连接处理错误 {}: {:?}", addr, e);
                            }
                        }
                        .instrument(conn_span),
                    );
                }
                Err(err) => {
                    error!("runtime {} accept error: {}", runtime_handle.idx(), err);
                }
            }
        }
    }
}

/// 处理连接请求
#[instrument(
    level = "trace",
    name = "connection",
    skip(socket, connection_counter, runtime_pool, runtime_handle)
)]
async fn handle_connection_local(
    socket: tokio::net::TcpStream,
    addr: SocketAddr,
    connection_counter: Arc<AtomicUsize>,
    runtime_pool: Arc<RuntimePool>,
    runtime_handle: RuntimeHandle,
) -> Result<()> {
    let server_header = config::get().session.rtsp.server_header.clone();
    use crate::rtsp::parser::RtspResponse;
    use crate::rtsp::{RtspMessage, RtspSplitter};
    use crate::session::RtspSession;
    use crate::session::io::{SharedTcpWriter, TcpWriter};
    use crate::session::state::RtspError;

    macro_rules! respond_and_continue {
        ($writer:expr, $cseq:expr, $status:expr, $reason:expr) => {{
            let mut headers = HashMap::new();
            headers.insert("CSeq".to_string(), $cseq.to_string());
            headers.insert("Server".to_string(), server_header.clone());
            let resp = RtspResponse {
                version: "RTSP/1.0".to_string(),
                status_code: $status,
                reason_phrase: $reason.to_string(),
                headers,
                body: None,
            };
            let response_data = crate::rtsp::RtspParser::build_response(
                resp.status_code,
                &resp.reason_phrase,
                &resp.headers,
                resp.body.as_deref(),
            );
            let response_bytes = Bytes::from(response_data);
            let mut guard = $writer.acquire().await;
            let _ = guard.write(&response_bytes).await;
            continue;
        }};
        ($writer:expr, $cseq:expr, $err:expr) => {{
            let (status, reason) = $err.into_response();
            respond_and_continue!($writer, $cseq, status, reason);
        }};
    }

    let peer_addr = socket.peer_addr().context("peer addr")?;
    let (mut read_half, write_half) = socket.into_split();
    let tcp_writer = SharedTcpWriter::new(TcpWriter::new(write_half));

    let mut session_opt: Option<RtspSession> = None;
    let mut udp_notifier: Option<Arc<Notify>> = None;
    let mut message_buffer = BytesMut::new();
    let mut splitter = RtspSplitter::new();

    loop {
        tokio::select! {
            read_result = read_half.read_buf(&mut message_buffer) => {
                match read_result {
                    Ok(0) => {
                        event!(Level::DEBUG, kind="peer_close", peer=%peer_addr);
                        break;
                    }
                    Ok(_bytes_read) => {
                        loop {
                            let decoded = match splitter.decode_message(&mut message_buffer) {
                                Ok(msg) => msg,
                                Err(err) => {
                                    tracing::warn!("RTSP 解析失败 peer={} err={:?}", peer_addr, err);
                                    message_buffer.clear();
                                    respond_and_continue!(tcp_writer, "0", 400, "Bad Request - Malformed RTSP message");
                                }
                            };

                            let Some(message) = decoded else {
                                break;
                            };
                            match message {
                                RtspMessage::Request(request) => {
                                    let cseq_header = match request.headers.get("cseq") {
                                        Some(v) => v.clone(),
                                        None => respond_and_continue!(tcp_writer, "0", RtspError::MissingCSeq),
                                    };

                                    if cseq_header.parse::<u32>().is_err() {
                                        respond_and_continue!(tcp_writer, cseq_header.clone(), RtspError::InvalidCSeq);
                                    }

                                    // 如果没有 session，创建新 session
                                    if session_opt.is_none() {
                                        let session_id = crate::utils::generate_session_id();
                                        let mut session = RtspSession::new(
                                            session_id.clone(),
                                            runtime_pool.clone(),
                                            runtime_handle.clone(),
                                        );
                                        session.set_tcp_writer(tcp_writer.clone());
                                        session.set_client_addr(peer_addr);
                                        session_opt = Some(session);
                                        event!(Level::INFO, kind="session_created", session.id=%session_id);
                                    }

                                    let cseq = cseq_header.clone();

                                    let response = if let Some(ref mut session) = session_opt {

                                        if request.method == "SETUP" {
                                            splitter.enable_rtp(true);
                                        }

                                        if request.method == "OPTIONS" {
                                            let mut headers = HashMap::new();
                                            headers.insert(
                                                "Public".to_string(),
                                                "OPTIONS, DESCRIBE, SETUP, TEARDOWN, PLAY, PAUSE, ANNOUNCE, RECORD".to_string(),
                                            );
                                            headers.insert(
                                                "CSeq".to_string(),
                                                request.headers.get("cseq").cloned().unwrap_or_else(|| "1".to_string()),
                                            );
                                            headers.insert("Server".to_string(), server_header.clone());

                                            RtspResponse {
                                                version: "RTSP/1.0".to_string(),
                                                status_code: 200,
                                                reason_phrase: "OK".to_string(),
                                                headers,
                                                body: None,
                                            }
                                        } else {
                                            session.handle_request(request, cseq).await?
                                        }
                                    } else {
                                        anyhow::bail!("Session should exist");
                                    };

                                    if let Some(ref session) = session_opt {
                                        udp_notifier = session.udp_notifier();
                                    }

                                    // 创建响应
                                    let response_data = crate::rtsp::RtspParser::build_response(
                                        response.status_code,
                                        &response.reason_phrase,
                                        &response.headers,
                                        response.body.as_deref(),
                                    );

                                    let response_bytes = Bytes::from(response_data);
                                    let mut guard = tcp_writer.acquire().await;

                                    event!(Level::TRACE, kind="send_response_over_tcp", peer=%peer_addr);
                                    if let Err(e) = guard.write(&response_bytes).await {
                                        tracing::error!("发送RTSP响应失败: {}", e);
                                        break;
                                    }
                                }
                                RtspMessage::Rtp { interleaved, data } => {
                                    if let Some(ref mut session) = session_opt {
                                        session.handle_rtp_over_tcp(interleaved, data).await?;
                                    } else {
                                        tracing::warn!("RTP 数据到达但没有活跃的 session");
                                    }
                                }
                                RtspMessage::Response(_) => {
                                    tracing::warn!("从客户端收到意外的 RTSP 响应");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("读取错误 {}: {}", peer_addr, e);
                        break;
                    }
                }
            }
            _udp_ready = async {
                if let Some(notifier) = udp_notifier.clone() {
                    notifier.notified().await;
                    true
                } else {
                    pending::<bool>().await
                }
            } => {
                if let Some(ref mut session) = session_opt {
                    for (track_id, payload) in session.drain_udp_queue() {
                        session.process_udp_payload(track_id, payload)?;
                    }
                }
            }
        }
    }

    // 尝试关闭写半部
    if let Some(mut guard) = tcp_writer.try_acquire() {
        if let Err(e) = guard.shutdown().await {
            tracing::debug!("tcp shutdown error {}: {}", peer_addr, e);
        }
    }

    let prev = connection_counter.fetch_sub(1, Ordering::Relaxed);
    tracing::info!("连接 {} 关闭。活跃连接数: {}", addr, prev.saturating_sub(1));

    Ok(())
}
