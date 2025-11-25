use anyhow::Result;
use bytes::{Bytes, BytesMut};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    rc::Rc,
    sync::Arc,
    time::Duration,
};
use tokio::net::UdpSocket;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::{info, instrument};

use crate::{
    config,
    event::{RuntimeHandle, RuntimePool},
    media::{MediaSource, source::MediaInfo},
    rtsp::{
        TrackType,
        parser::{RtspRequest, RtspResponse},
        sdp::{SdpParser, SdpTrack},
    },
    session::{
        PullSession, PushSession, SharedTcpWriter,
        state::{RtspError, RtspResult},
    },
    utils::parse_transport,
};

// socket相关配置
#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub is_tcp: bool,
    pub interleaved_channel: Option<u8>,
    pub connection_type: ConnectionType,
    pub interleaved_rtp: Option<u8>,
    pub interleaved_rtcp: Option<u8>,
    pub client_rtp_port: Option<u16>,
    pub client_rtcp_port: Option<u16>,
    pub server_rtp_port: Option<u16>,
    pub server_rtcp_port: Option<u16>,
    pub rtp_socket: Option<Arc<UdpSocket>>,
    pub rtcp_socket: Option<Arc<UdpSocket>>,
    pub udp_worker_started: bool,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            is_tcp: true,
            interleaved_channel: None,
            connection_type: ConnectionType::Tcp,
            interleaved_rtp: None,
            interleaved_rtcp: None,
            client_rtp_port: None,
            client_rtcp_port: None,
            server_rtp_port: None,
            server_rtcp_port: None,
            rtp_socket: None,
            rtcp_socket: None,
            udp_worker_started: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionType {
    Tcp,
    Udp,
    TcpInterleaved,
    UdpUnicast,
}

/// RTSP会话
pub struct RtspSession {
    /// 会话ID
    pub session_id: String,
    /// 媒体信息
    pub media_info: MediaInfo,
    /// 会话配置
    session_cfg: config::RtspSessionConfig,
    /// 每个Track的SETUP配置
    pub setup_config: HashMap<String, TransportConfig>,
    /// SDP轨道列表
    pub sdp_tracks: Vec<SdpTrack>,
    /// CSeq序号
    pub cseq: u32,
    /// 最后活动时间
    pub last_activity: Instant,
    /// 连接类型
    pub connection_type: ConnectionType,
    /// TCP交错通道到Track的映射
    pub tcp_interleaved_2_track: Option<HashMap<u8, String>>,
    /// 后台任务通知器列表
    pub background_tasks: Vec<Arc<Notify>>,
    /// 订阅ID
    pub subscription_id: Option<u64>,

    /// 运行时池引用
    runtime_pool: Arc<RuntimePool>,
    /// 运行时句柄
    runtime_handle: RuntimeHandle,
    /// 推流连接注册状态
    push_registered: bool,
    /// 拉流连接注册状态
    pull_registered: bool,
    /// 推流Session
    push_session: Option<PushSession>,
    /// 拉流Session
    pull_session: Option<PullSession>,
    /// UDP数据队列（用于拉流接收RTP）
    udp_queue: Option<Rc<RefCell<VecDeque<(String, Bytes)>>>>,
    /// UDP通知器（用于拉流接收RTP）
    udp_notify: Option<Arc<Notify>>,

    /// IO资源（用于拉流发送RTP）
    tcp_writer: Option<SharedTcpWriter>,
    client_addr: Option<SocketAddr>,
}

impl RtspSession {
    #[inline]
    fn resp(&self, status: u16, reason: &'static str) -> RtspResponse {
        self.create_response(status, reason, None, None)
    }

    #[inline]
    fn resp_from_error(&self, err: RtspError) -> RtspResponse {
        let (status, reason) = err.into_response();
        self.resp(status, reason)
    }

    fn parse_setup_context(
        &self,
        request: &RtspRequest,
    ) -> RtspResult<(String, HashMap<String, String>)> {
        let transport_header = request
            .headers
            .get("transport")
            .ok_or(RtspError::MissingTransport)?;

        let track_index = self.extract_track_index(&request.uri).map_err(|err| {
            tracing::warn!("SETUP 无法解析track: uri={} err={:?}", request.uri, err);
            err
        })?;

        Ok((track_index, parse_transport(transport_header)))
    }

    fn validate_session_header(&self, request: &RtspRequest) -> RtspResult<()> {
        if let Some(session) = request.headers.get("session") {
            let session_id = session.split(';').next().unwrap_or(session);
            if session_id != self.session_id {
                return Err(RtspError::SessionNotFound);
            }
        }
        Ok(())
    }

    pub fn new(
        session_id: String,
        runtime_pool: Arc<RuntimePool>,
        runtime_handle: RuntimeHandle,
    ) -> Self {
        let session_cfg = config::get().session.rtsp.clone();
        Self {
            session_id,
            media_info: MediaInfo {
                schema: session_cfg.default_media.schema.clone(),
                host: session_cfg.default_media.host.clone(),
                port: session_cfg.default_media.port,
                app: session_cfg.default_media.app.clone(),
                stream: session_cfg.default_media.stream.clone(),
                full_url: String::new(),
                protocol: session_cfg.default_media.protocol.clone(),
            },
            session_cfg,
            sdp_tracks: Vec::new(),
            cseq: 0,
            last_activity: Instant::now(),
            connection_type: ConnectionType::Tcp,
            setup_config: HashMap::new(),
            tcp_interleaved_2_track: Some(HashMap::new()),
            background_tasks: Vec::new(),
            subscription_id: None,
            runtime_handle,
            runtime_pool,
            push_registered: false,
            pull_registered: false,
            push_session: None,
            pull_session: None,
            udp_queue: None,
            udp_notify: None,
            tcp_writer: None,
            client_addr: None,
        }
    }

    pub fn set_tcp_writer(&mut self, writer: SharedTcpWriter) {
        self.tcp_writer = Some(writer);
    }

    pub fn set_client_addr(&mut self, addr: SocketAddr) {
        self.client_addr = Some(addr);
    }

    fn mark_push_active(&mut self) {
        if !self.push_registered {
            self.runtime_pool.inc_push(&self.runtime_handle);
            self.push_registered = true;
        }
    }

    fn unmark_push_active(&mut self) {
        if self.push_registered {
            self.runtime_pool.dec_push(&self.runtime_handle);
            self.push_registered = false;
        }
    }

    fn mark_pull_active(&mut self) {
        if !self.pull_registered {
            self.runtime_pool.inc_pull(&self.runtime_handle);
            self.pull_registered = true;
        }
    }

    fn unmark_pull_active(&mut self) {
        if self.pull_registered {
            self.runtime_pool.dec_pull(&self.runtime_handle);
            self.pull_registered = false;
        }
    }

    #[instrument(level = "trace", skip(self, request), fields(request_method=%request.method))]
    pub async fn handle_request(
        &mut self,
        request: RtspRequest,
        _cseq: String,
    ) -> Result<RtspResponse> {
        self.last_activity = Instant::now();

        let response = match self.dispatch_request(request).await {
            Ok(resp) => resp,
            Err(err) => {
                tracing::warn!("RTSP 请求处理失败: err={:?}", err);
                self.resp_from_error(err)
            }
        };

        tracing::debug!("Response: {:#?}", response);
        Ok(response)
    }

    async fn dispatch_request(&mut self, request: RtspRequest) -> RtspResult<RtspResponse> {
        let cseq_header = request.headers.get("cseq").ok_or(RtspError::MissingCSeq)?;
        let parsed_cseq: u32 = cseq_header.parse().map_err(|_| RtspError::InvalidCSeq)?;
        self.cseq = parsed_cseq;

        info!("Handling RTSP request: {} {}", request.method, request.uri);

        match request.method.as_str() {
            "DESCRIBE" => self.handle_describe(&request).await,
            "ANNOUNCE" => self.handle_announce(&request).await,
            "SETUP" => self.handle_setup(&request).await,
            "PLAY" => self.handle_play(&request).await,
            "RECORD" => self.handle_record(&request).await,
            "TEARDOWN" => self.handle_teardown(&request).await,
            _ => Err(RtspError::MethodNotImplemented),
        }
    }

    /// 处理 RTP over TCP 数据
    pub async fn handle_rtp_over_tcp(&mut self, interleaved: u8, data: Bytes) -> Result<()> {
        if interleaved % 2 == 1 {
            if let Some(map) = self.tcp_interleaved_2_track.as_ref() {
                if map.contains_key(&(interleaved - 1)) {
                    tracing::debug!("忽略RTCP包: channel={}, size={}", interleaved, data.len());
                    return Ok(());
                }
            }
            tracing::warn!("interleaved {} 未映射到 track", interleaved);
            return Ok(());
        }

        let track_id = match self
            .tcp_interleaved_2_track
            .as_ref()
            .and_then(|map| map.get(&interleaved))
            .cloned()
        {
            Some(id) => id,
            None => {
                tracing::warn!("interleaved {} 未映射到 track(偶数通道)", interleaved);
                return Ok(());
            }
        };

        // tracing::debug!(
        //     target: "rtp_tcp",
        //     track = %track_id,
        //     channel = interleaved,
        //     size = data.len(),
        //     "收到 RTP over TCP"
        // );

        if let Some(push_session) = &mut self.push_session {
            push_session.input_rtp(&track_id, data)?;
        } else {
            tracing::warn!("收到RTP数据但不存在有效的推流Session");
        }

        Ok(())
    }

    async fn handle_describe(&mut self, request: &RtspRequest) -> RtspResult<RtspResponse> {
        self.media_info = MediaInfo::parse(&request.uri).map_err(|err| {
            tracing::warn!("DESCRIBE 解析URL失败 uri={} err={:?}", request.uri, err);
            RtspError::InvalidUri
        })?;

        let stream_key = self.media_info.short_url();
        let info = MediaSource::find_info(&stream_key).ok_or_else(|| {
            tracing::warn!("Stream not found: {}", self.media_info.short_url());
            RtspError::StreamNotFound
        })?;

        let sdp = info.sdp_info.as_ref().ok_or_else(|| {
            tracing::warn!(
                "SDP info not ready for stream: {}",
                self.media_info.short_url()
            );
            RtspError::SdpNotReady
        })?;

        // 更新本地SDP track信息，供后续SETUP/PLAY使用
        self.sdp_tracks = info.tracks.iter().map(|track| (**track).clone()).collect();

        let sdp_body = sdp.clone();
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/sdp".to_string());
        headers.insert("Content-Length".to_string(), sdp_body.len().to_string());
        headers.insert(
            "Content-Base".to_string(),
            format!("{}/", request.uri.trim_end_matches('/')),
        );

        tracing::info!("Serving SDP for stream: {}", self.media_info.short_url());
        Ok(self.create_response(200, "OK", Some(headers), Some(sdp_body)))
    }

    async fn handle_announce(&mut self, request: &RtspRequest) -> RtspResult<RtspResponse> {
        let sdp_content = request.body.clone().ok_or(RtspError::MissingSdp)?;

        let sdp_session = SdpParser::parse(&sdp_content).map_err(|err| {
            tracing::warn!("ANNOUNCE SDP解析失败: err={:?}", err);
            RtspError::InvalidSdpOrTransport
        })?;

        self.sdp_tracks = SdpParser::extract_tracks(&sdp_session);

        self.media_info = MediaInfo::parse(&request.uri).map_err(|err| {
            tracing::warn!("ANNOUNCE URL解析失败 uri={} err={:?}", request.uri, err);
            RtspError::InvalidUri
        })?;

        // 创建PushSession
        let stream_key = self.media_info.short_url();
        let mut push_session = PushSession::new(
            self.session_id.clone(),
            stream_key,
            self.runtime_handle.idx(),
        );

        let sdp_tracks_arc: Vec<Arc<SdpTrack>> = self
            .sdp_tracks
            .iter()
            .map(|t| Arc::new(t.clone()))
            .collect();

        push_session.configure_media_source(sdp_content, sdp_tracks_arc)?;

        self.push_session = Some(push_session);
        self.mark_push_active();

        tracing::info!("ANNOUNCE完成，创建PushSession: {}", self.session_id);
        Ok(self.create_response(200, "OK", None, None))
    }

    async fn handle_setup(&mut self, request: &RtspRequest) -> RtspResult<RtspResponse> {
        let (track_index, transport_params) = self.parse_setup_context(request)?;

        // SDP/track
        let (
            _track_type,
            _codec,
            _payload_type,
            _sample_rate,
            tcp_interleaved,
            _aac_size_len,
            _aac_index_len,
        ) = self
            .infer_setup_track_params(&request, &track_index)
            .map_err(|err| {
                tracing::warn!(
                    "SETUP 参数非法: uri={} track={} err={:?}",
                    request.uri,
                    track_index,
                    err
                );
                err
            })?;

        let mut config = TransportConfig::default();

        // TCP interleaved 或 UDP
        let response_transport: String = if transport_params.contains_key("tcp")
            || transport_params.contains_key("interleaved")
        {
            config.connection_type = ConnectionType::TcpInterleaved;

            let ch = tcp_interleaved.ok_or(RtspError::MissingInterleaved)?;

            self.tcp_interleaved_2_track
                .get_or_insert_with(HashMap::new)
                .insert(ch, track_index.clone());

            let mut ssrc: u32 = 0;
            for sdp_track in &mut self.sdp_tracks {
                if track_index == sdp_track.control_url {
                    sdp_track.interleaved = ch;
                    ssrc = sdp_track.ssrc;
                }
            }

            let rtp_ch = ch;
            let rtcp_ch = rtp_ch + 1;
            config.interleaved_rtp = Some(rtp_ch);
            config.interleaved_rtcp = Some(rtcp_ch);

            format!(
                "RTP/AVP/TCP;unicast;interleaved={}-{};ssrc={:08X}",
                rtp_ch, rtcp_ch, ssrc
            )
        } else {
            config.connection_type = ConnectionType::UdpUnicast;

            let client_port_raw = transport_params
                .get("client_port")
                .ok_or(RtspError::MissingClientPort)?;

            let ports: Vec<&str> = client_port_raw.split('-').collect();
            if ports.len() != 2 {
                return Err(RtspError::InvalidClientPort);
            }
            let rtp_port: u16 = ports[0]
                .parse::<u16>()
                .ok()
                .filter(|v| *v > 0)
                .ok_or(RtspError::InvalidClientPort)?;
            let rtcp_port: u16 = ports[1]
                .parse::<u16>()
                .ok()
                .filter(|v| *v > 0)
                .ok_or(RtspError::InvalidClientPort)?;
            config.client_rtp_port = Some(rtp_port);
            config.client_rtcp_port = Some(rtcp_port);

            let rtp_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.map_err(|err| {
                tracing::error!("RTP socket bind failed track={} err={:?}", track_index, err);
                RtspError::InternalServerError
            })?);
            let rtcp_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.map_err(|err| {
                tracing::error!(
                    "RTCP socket bind failed track={} err={:?}",
                    track_index,
                    err
                );
                RtspError::InternalServerError
            })?);
            let server_rtp_port = rtp_socket
                .local_addr()
                .map_err(|err| {
                    tracing::error!(
                        "RTP socket local_addr failed track={} err={:?}",
                        track_index,
                        err
                    );
                    RtspError::InternalServerError
                })?
                .port();
            let server_rtcp_port = rtcp_socket
                .local_addr()
                .map_err(|err| {
                    tracing::error!(
                        "RTCP socket local_addr failed track={} err={:?}",
                        track_index,
                        err
                    );
                    RtspError::InternalServerError
                })?
                .port();

            config.server_rtp_port = Some(server_rtp_port);
            config.server_rtcp_port = Some(server_rtcp_port);
            config.rtp_socket = Some(rtp_socket);
            config.rtcp_socket = Some(rtcp_socket);

            let mut ssrc: u32 = 0;
            for sdp_track in &self.sdp_tracks {
                if track_index == sdp_track.control_url {
                    ssrc = sdp_track.ssrc;
                }
            }

            format!(
                "RTP/AVP;unicast;client_port={};server_port={}-{};ssrc={:08X}",
                client_port_raw, server_rtp_port, server_rtcp_port, ssrc
            )
        };

        self.setup_config
            .insert(track_index.clone(), config.clone());

        for track in &mut self.sdp_tracks {
            if track.control_url == track_index {
                track.initialized = true;
            }
        }

        // 如果是推流Session, 注册到PushSession
        if let Some(push_session) = self.push_session.as_mut() {
            let track_clone = self
                .sdp_tracks
                .iter()
                .find(|t| t.control_url == track_index)
                .cloned()
                .ok_or_else(|| {
                    tracing::error!("PushSession SETUP 缺少track {}", track_index);
                    RtspError::InvalidTrack
                })?;

            push_session.register_track(track_clone).map_err(|err| {
                tracing::error!("PushSession 处理SETUP失败: err={:?}", err);
                err
            })?;
        }

        let mut headers: HashMap<String, String> = HashMap::new();
        headers.insert("Transport".to_string(), response_transport);
        headers.insert(
            "Session".to_string(),
            format!(
                "{};timeout={}",
                self.session_id, self.session_cfg.session_timeout_secs
            ),
        );

        info!(
            "Setup track {} for session {}",
            track_index, self.session_id
        );
        Ok(self.create_response(200, "OK", Some(headers), None))
    }

    async fn handle_play(&mut self, request: &RtspRequest) -> RtspResult<RtspResponse> {
        self.validate_session_header(request)?;

        // 创建PullSession并启动拉流
        let stream_key = self.media_info.short_url();
        let mut pull_session = PullSession::new(
            self.session_id.clone(),
            stream_key,
            self.runtime_handle.clone(),
        );

        if let Some(writer) = self.tcp_writer.clone() {
            pull_session.set_tcp_writer(writer);
        }

        pull_session.load_media_source().map_err(|err| {
            match err {
                RtspError::SdpNotReady => {
                    tracing::warn!("PullSession DESCRIBE SDP未就绪: err={:?}", err);
                }
                RtspError::StreamNotFound => {
                    tracing::warn!("PullSession DESCRIBE 未找到流: err={:?}", err);
                }
                _ => {
                    tracing::warn!("PullSession DESCRIBE 失败: err={:?}", err);
                }
            }
            err
        })?;

        pull_session.set_track_channels(self.collect_track_channels());
        pull_session.set_udp_targets(self.collect_udp_targets());

        pull_session.initialize_playback().map_err(|err| {
            tracing::error!("PullSession PLAY 失败: err={:?}", err);
            RtspError::InternalServerError
        })?;

        self.pull_session = Some(pull_session);
        self.mark_pull_active();

        let mut rtp_info_parts = Vec::new();
        for track in self.sdp_tracks.iter() {
            if track.initialized {
                let control_url = track.get_control_url(&request.uri);
                rtp_info_parts.push(format!(
                    "url={};seq={};rtptime={}",
                    control_url, track.sequence, track.timestamp
                ));
            }
        }

        let mut headers = HashMap::new();
        headers.insert(
            "Session".to_string(),
            format!(
                "{};timeout={}",
                self.session_id, self.session_cfg.session_timeout_secs
            ),
        );
        if !rtp_info_parts.is_empty() {
            headers.insert("RTP-Info".to_string(), rtp_info_parts.join(","));
        }
        headers.insert("Range".to_string(), "npt=0.000-".to_string());

        info!("Started playing session {}", self.session_id);
        Ok(self.create_response(200, "OK", Some(headers), None))
    }

    async fn handle_record(&mut self, _request: &RtspRequest) -> RtspResult<RtspResponse> {
        let push_session = self
            .push_session
            .as_mut()
            .ok_or(RtspError::PushSessionNotInitialized)?;

        push_session.start_recording(self.runtime_handle.clone())?;
        self.start_udp_receivers()?;

        let mut headers = HashMap::new();
        headers.insert(
            "Session".to_string(),
            format!(
                "{};timeout={}",
                self.session_id, self.session_cfg.session_timeout_secs
            ),
        );

        tracing::info!("Started recording session {}", self.session_id);
        Ok(self.create_response(200, "OK", Some(headers), None))
    }

    fn ensure_udp_buffers(&mut self) -> (Rc<RefCell<VecDeque<(String, Bytes)>>>, Arc<Notify>) {
        if self.udp_queue.is_none() {
            self.udp_queue = Some(Rc::new(RefCell::new(VecDeque::with_capacity(
                self.session_cfg.udp_queue_capacity,
            ))));
        }
        if self.udp_notify.is_none() {
            self.udp_notify = Some(Arc::new(Notify::new()));
        }
        (
            self.udp_queue.as_ref().unwrap().clone(),
            self.udp_notify.as_ref().unwrap().clone(),
        )
    }

    pub(crate) fn udp_notifier(&self) -> Option<Arc<Notify>> {
        self.udp_notify.as_ref().map(Arc::clone)
    }

    pub(crate) fn drain_udp_queue(&self) -> Vec<(String, Bytes)> {
        if let Some(queue) = &self.udp_queue {
            let mut slot = queue.borrow_mut();
            slot.drain(..).collect()
        } else {
            Vec::new()
        }
    }

    fn start_udp_receivers(&mut self) -> RtspResult<()> {
        let (udp_queue, udp_notify) = self.ensure_udp_buffers();

        let stream_key = self.media_info.short_url();
        let session_cfg = self.session_cfg.clone();

        for (track_id, config) in self.setup_config.iter_mut() {
            if config.connection_type != ConnectionType::UdpUnicast || config.udp_worker_started {
                continue;
            }

            if let Some(socket) = config.rtp_socket.clone() {
                let track = track_id.clone();
                let stream = stream_key.clone();
                let cancel = Arc::new(Notify::new());
                let cancel_token = cancel.clone();
                self.background_tasks.push(cancel);
                let queue = udp_queue.clone();
                let notifier = udp_notify.clone();
                self.runtime_handle.clone().spawn_local_current(async move {
                    let mut buf = BytesMut::with_capacity(session_cfg.udp_recv_buffer);
                    loop {
                        tokio::select! {
                            _ = cancel_token.notified() => {
                                tracing::debug!(
                                    "UDP接收任务已取消 stream={} track={}",
                                    stream,
                                    track
                                );
                                break;
                            }
                            result = async {
                                let target = session_cfg.udp_recv_buffer;
                                if buf.capacity() < target {
                                    buf.reserve(target - buf.capacity());
                                }
                                let cap = buf.capacity();
                                unsafe { buf.set_len(cap); }
                                socket.recv_from(&mut buf[..]).await
                            } => {
                                match result {
                                    Ok((size, _from)) => {
                                        if size == 0 {
                                            buf.clear();
                                            continue;
                                        }
                                        buf.truncate(size);
                                        let payload = buf.split().freeze();
                                        {
                                            let mut slot = queue.borrow_mut();
                                            if slot.len() >= session_cfg.max_pending_udp {
                                                slot.pop_front();
                                            }
                                            slot.push_back((track.clone(), payload));
                                        }
                                        notifier.notify_one();
                                    }
                                    Err(err) => {
                                        tracing::error!(
                                            "UDP接收错误 stream={} track={} err={:?}",
                                            stream,
                                            track,
                                            err
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    }
                });

                config.udp_worker_started = true;
            }
        }

        Ok(())
    }

    pub(crate) fn process_udp_payload(&mut self, track_id: String, payload: Bytes) -> Result<()> {
        if let Some(push_session) = self.push_session.as_mut() {
            push_session.input_rtp(&track_id, payload)?;
        } else {
            tracing::warn!("收到UDP数据但不存在有效的推流Session track={}", track_id);
        }
        Ok(())
    }

    async fn handle_teardown(&mut self, _request: &RtspRequest) -> RtspResult<RtspResponse> {
        self.stop_background_tasks();

        if let Some(pull) = self.pull_session.as_mut() {
            let _ = pull.shutdown();
        }
        if let Some(push) = self.push_session.as_mut() {
            let _ = push.shutdown();
        }

        self.unmark_pull_active();
        self.unmark_push_active();
        self.pull_session = None;
        self.push_session = None;
        self.udp_queue = None;
        self.udp_notify = None;

        let mut headers = HashMap::new();
        headers.insert("Session".to_string(), self.session_id.clone());
        info!("Teardown session {}", self.session_id);
        Ok(self.create_response(200, "OK", Some(headers), None))
    }

    fn extract_track_index(&self, uri: &str) -> RtspResult<String> {
        uri.rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .ok_or(RtspError::InvalidUri)
    }

    /// 收集Track到RTP通道的映射（用于TCP交错模式）
    fn collect_track_channels(&self) -> HashMap<String, u8> {
        let mut map = HashMap::new();
        for (track, config) in &self.setup_config {
            if let Some(channel) = config.interleaved_rtp {
                map.insert(track.clone(), channel);
            }
        }
        map
    }

    /// 收集UDP目标地址列表（用于UDP模式）
    fn collect_udp_targets(&self) -> Vec<(String, Arc<UdpSocket>, SocketAddr)> {
        let mut result = Vec::new();
        let peer = match self.client_addr {
            Some(addr) => addr.ip(),
            None => return result,
        };

        for (track, config) in &self.setup_config {
            if config.connection_type != ConnectionType::UdpUnicast {
                continue;
            }

            let socket = match config.rtp_socket.clone() {
                Some(sock) => sock,
                None => continue,
            };

            let client_port = match config.client_rtp_port {
                Some(port) => port,
                None => continue,
            };

            let destination = SocketAddr::new(peer, client_port);
            result.push((track.clone(), socket, destination));
        }

        result
    }

    fn create_response(
        &self,
        status_code: u16,
        reason_phrase: &str,
        headers: Option<HashMap<String, String>>,
        body: Option<String>,
    ) -> RtspResponse {
        let mut response_headers = headers.unwrap_or_default();

        response_headers.insert("CSeq".to_string(), self.cseq.to_string());
        response_headers.insert("Server".to_string(), self.session_cfg.server_header.clone());
        response_headers.insert(
            "Date".to_string(),
            chrono::Utc::now()
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        );

        if let Some(ref body) = body {
            response_headers.insert("Content-Length".to_string(), body.len().to_string());
        }

        RtspResponse {
            version: "RTSP/1.0".to_string(),
            status_code,
            reason_phrase: reason_phrase.to_string(),
            headers: response_headers,
            body,
        }
    }

    pub fn is_expired(&self, timeout: Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }

    fn infer_setup_track_params(
        &self,
        req: &RtspRequest,
        track_id: &str,
    ) -> Result<
        (
            TrackType,
            crate::media::AvCodec,
            Option<u8>,
            u32,
            Option<u8>,
            Option<u8>,
            Option<u8>,
        ),
        RtspError,
    > {
        let meta = self
            .sdp_tracks
            .iter()
            .find(|t| t.control_url == track_id)
            .ok_or(RtspError::InvalidTrack)?;

        if meta.payload_type == 0 || meta.sample_rate == 0 || meta.codec.is_empty() {
            return Err(RtspError::InvalidSdpOrTransport);
        }

        let codec = match meta.codec.to_ascii_lowercase().as_str() {
            "h264" | "avc" => crate::media::AvCodec::H264,
            //"h265" | "hevc" => crate::media::AvCodec::H265,
            "aac" | "mpeg4-generic" => crate::media::AvCodec::Aac,
            _ => return Err(RtspError::InvalidSdpOrTransport),
        };

        let payload_type = Some(meta.payload_type as u8);
        let sample_rate = meta.sample_rate as u32;

        let mut aac_size_len: Option<u8> = None;
        let mut aac_index_len: Option<u8> = None;
        if matches!(codec, crate::media::AvCodec::Aac) {
            if let Some(bits) = parse_aac_fmtp_bits(meta.fmtp.as_deref()) {
                aac_size_len = bits.0;
                aac_index_len = bits.1;
            }
        }

        let tcp_interleaved = req
            .headers
            .get("transport")
            .and_then(|v| parse_interleaved_channel(v));

        Ok((
            meta.track_type,
            codec,
            payload_type,
            sample_rate,
            tcp_interleaved,
            aac_size_len,
            aac_index_len,
        ))
    }
}

impl Drop for RtspSession {
    fn drop(&mut self) {
        info!("Dropping RTSP session {}", self.session_id);
        self.stop_background_tasks();
        self.unmark_pull_active();
        self.unmark_push_active();
    }
}

impl RtspSession {
    fn stop_background_tasks(&mut self) {
        for notifier in self.background_tasks.drain(..) {
            notifier.notify_waiters();
        }
    }
}

fn parse_interleaved_channel(transport: &str) -> Option<u8> {
    let lower = transport.to_ascii_lowercase();
    if let Some(pos) = lower.find("interleaved=") {
        let v = &lower[pos + "interleaved=".len()..];
        let end = v.find(';').unwrap_or(v.len());
        let pair = &v[..end];
        if let Some(dash) = pair.find('-') {
            pair[..dash].parse::<u8>().ok()
        } else {
            pair.parse::<u8>().ok()
        }
    } else {
        None
    }
}

/// 解析AAC的FMTP参数，提取sizeLength和indexLength
fn parse_aac_fmtp_bits(fmtp: Option<&str>) -> Option<(Option<u8>, Option<u8>)> {
    let s = fmtp?.to_ascii_lowercase();
    let mut size_len: Option<u8> = None;
    let mut index_len: Option<u8> = None;
    for part in s.split(';') {
        let p = part.trim();
        if let Some(eq) = p.find('=') {
            let key = &p[..eq].trim();
            let val = &p[eq + 1..].trim();
            if *key == "sizelength" {
                if let Ok(v) = val.parse::<u8>() {
                    size_len = Some(v);
                }
            } else if *key == "indexlength" {
                if let Ok(v) = val.parse::<u8>() {
                    index_len = Some(v);
                }
            }
        }
    }
    if size_len.is_some() || index_len.is_some() {
        Some((size_len, index_len))
    } else {
        None
    }
}
