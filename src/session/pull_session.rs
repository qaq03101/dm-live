use anyhow::{Context, Result, bail};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;

use crate::session::state::FlushError;
use crate::{
    config,
    event::RuntimeHandle,
    media::{
        AttachCallback, AttachCommand, Dispatcher, MediaSource, MediaSourceInfo, Reader,
        StorageSnapshot, reader::FrameRef,
    },
    rtp::RtpPacket,
    session::{
        io::{SharedTcpWriter, UdpWriter},
        state::{RtspError, RtspResult},
    },
};
use rustc_hash::FxHashMap;
use tokio::net::UdpSocket;
use tracing::instrument;
#[derive(Clone)]
struct UdpSendTarget {
    writer: Arc<UdpWriter>,
    destination: SocketAddr,
}

/// 拉流会话状态管理
///
pub struct PullSession {
    /// 会话ID
    pub session_id: String,
    /// 拉取的流标识
    pub stream_key: String,
    /// 关联的MediaSource信息
    pub media_source_info: Option<Arc<MediaSourceInfo>>,
    /// Reader的ID，用于从Dispatcher注销
    pub reader_id: Option<String>,
    /// 允许传输的track集合          
    pub allowed_tracks: HashSet<String>,
    /// 运行时ID
    pub runtime_id: usize,
    /// 运行时句柄
    pub runtime_handle: RuntimeHandle,
    /// Attach回调，用于与MediaSource的RingBuffer交互
    pub attach_callback: Option<Arc<AttachCallback>>,
    /// TCP写入器
    pub tcp_writer: Option<SharedTcpWriter>,
    /// Track到RTP通道的映射
    track_channels: Arc<FxHashMap<String, u8>>,
    /// Track到UDP发送目标的映射
    udp_targets: Arc<FxHashMap<String, UdpSendTarget>>,
    /// TCP 批次缓存
    tcp_batch_cache: Rc<RefCell<Vec<(u8, Arc<RtpPacket>)>>>,
    /// UDP 批次缓存
    udp_batch_cache:
        Rc<RefCell<FxHashMap<usize, (Arc<UdpWriter>, Vec<(SocketAddr, Arc<RtpPacket>)>)>>>,
    /// 发送缓冲区A
    buffer_a: Rc<RefCell<VecDeque<FrameRef>>>,
    /// 发送缓冲区B
    buffer_b: Rc<RefCell<VecDeque<FrameRef>>>,
    /// 当前活跃的缓冲区标志（true表示buffer_a，false表示buffer_b）
    active_is_a: Rc<Cell<bool>>,
    /// 是否有待处理的数据排队
    drain_pending: Rc<Cell<bool>>,
    /// 是否已终止（例如对端关闭）
    closed: Rc<Cell<bool>>,
}

impl PullSession {
    fn cleanup_closed(stream_key: &str, runtime_id: usize) {
        Dispatcher::remove(stream_key);
        if let Some(cb) = MediaSource::find_attach_callback(stream_key) {
            cb(AttachCommand::Detach { runtime_id });
        }
    }
    pub fn new(session_id: String, stream_key: String, runtime_handle: RuntimeHandle) -> Self {
        let runtime_id = runtime_handle.idx();
        let pull_cfg = &config::get().session.pull;
        Self {
            session_id,
            stream_key,
            media_source_info: None,
            reader_id: None,
            allowed_tracks: HashSet::new(),
            runtime_id,
            runtime_handle,
            attach_callback: None,
            tcp_writer: None,
            track_channels: Arc::new(FxHashMap::default()),
            udp_targets: Arc::new(FxHashMap::default()),
            tcp_batch_cache: Rc::new(RefCell::new(Vec::with_capacity(
                pull_cfg.tcp_batch_size.saturating_mul(2),
            ))),
            udp_batch_cache: Rc::new(RefCell::new(FxHashMap::default())),
            buffer_a: Rc::new(RefCell::new(VecDeque::with_capacity(
                pull_cfg.send_queue_capacity,
            ))),
            buffer_b: Rc::new(RefCell::new(VecDeque::with_capacity(
                pull_cfg.send_queue_capacity,
            ))),
            active_is_a: Rc::new(Cell::new(true)),
            drain_pending: Rc::new(Cell::new(false)),
            closed: Rc::new(Cell::new(false)),
        }
    }

    /// DESCRIBE - 查找MediaSource，返回SDP
    pub fn load_media_source(&mut self) -> RtspResult<String> {
        // 查找MediaSource信息
        let info = crate::media::MediaSource::find_info(&self.stream_key)
            .ok_or(RtspError::StreamNotFound)?;

        // 获取SDP信息
        let sdp_info = info
            .sdp_info
            .as_ref()
            .cloned()
            .ok_or(RtspError::SdpNotReady)?;

        self.media_source_info = Some(info);

        tracing::info!(
            "PullSession {} DESCRIBE: 找到MediaSource for {}",
            self.session_id,
            self.stream_key
        );

        Ok(sdp_info)
    }

    /// SETUP - 配置transport，记录允许的track
    pub fn allow_track(&mut self, track_id: String) -> RtspResult<()> {
        self.allowed_tracks.insert(track_id.clone());

        tracing::info!(
            "PullSession {} SETUP: 添加track {}",
            self.session_id,
            track_id
        );
        Ok(())
    }

    pub fn set_track_channels(&mut self, mapping: HashMap<String, u8>) {
        self.track_channels = Arc::new(mapping.into_iter().collect());
    }

    pub fn set_udp_targets(&mut self, targets: Vec<(String, Arc<UdpSocket>, SocketAddr)>) {
        if targets.is_empty() {
            self.udp_targets = Arc::new(FxHashMap::default());
            return;
        }

        let mut map = FxHashMap::default();
        for (track, socket, destination) in targets {
            let writer = Arc::new(UdpWriter::new(socket));
            map.insert(
                track,
                UdpSendTarget {
                    writer,
                    destination,
                },
            );
        }
        self.udp_targets = Arc::new(map);
    }

    /// 创建Reader，注册到Dispatcher
    pub fn initialize_playback(&mut self) -> Result<()> {
        self.media_source_info
            .as_ref()
            .context("MediaSource not found")?;

        let has_tcp = self.tcp_writer.is_some();
        let has_udp = !self.udp_targets.is_empty();
        if !has_tcp && !has_udp {
            bail!("No transport configured for pull session");
        }

        // 创建Reader并设置回调
        let reader_id = format!("reader_{}", self.session_id);
        let allowed_tracks = self.allowed_tracks.clone();
        let session_id = self.session_id.clone();
        let stream_key = self.stream_key.clone();
        let runtime_handle = self.runtime_handle.clone();
        let tcp_writer = self.tcp_writer.clone();
        let track_channels = self.track_channels.clone();
        let udp_targets = self.udp_targets.clone();

        let buffer_a = self.buffer_a.clone();
        let buffer_b = self.buffer_b.clone();
        let active_flag = self.active_is_a.clone();
        let pending_flag = self.drain_pending.clone();
        let closed_flag = self.closed.clone();
        let tcp_batch_cache = self.tcp_batch_cache.clone();
        let udp_batch_cache = self.udp_batch_cache.clone();

        let reader = Reader::new(
            reader_id.clone(),
            allowed_tracks,
            move |frames: FrameRef| {
                if frames.is_empty() || closed_flag.get() {
                    return;
                }
                let use_a = active_flag.get();
                let buffer = if use_a {
                    buffer_a.clone()
                } else {
                    buffer_b.clone()
                };

                {
                    let mut slot = buffer.borrow_mut();
                    slot.push_back(frames);
                }

                PullSession::enforce_queue_capacity(&buffer_a, &buffer_b);

                PullSession::schedule_drain(
                    buffer_a.clone(),
                    buffer_b.clone(),
                    active_flag.clone(),
                    pending_flag.clone(),
                    tcp_writer.clone(),
                    tcp_batch_cache.clone(),
                    udp_batch_cache.clone(),
                    runtime_handle.clone(),
                    session_id.clone(),
                    stream_key.clone(),
                    closed_flag.clone(),
                    track_channels.clone(),
                    udp_targets.clone(),
                );
            },
        );

        // 确保当前运行时的分片路由已初始化
        Dispatcher::init_runtime(self.runtime_handle.clone());

        let storage = Arc::new(StorageSnapshot::empty());
        let dispatcher =
            Dispatcher::get_or_create(&self.stream_key, self.runtime_id, storage.clone());

        dispatcher.borrow_mut().register_reader(reader)?;

        // Attach到RingBuffer，确保缓存从RingBuffer推送到已存在的Dispatcher/Reader
        let attach_cb = MediaSource::find_attach_callback(&self.stream_key)
            .context("Attach callback not found for stream")?;
        attach_cb(AttachCommand::Attach {
            handle: self.runtime_handle.clone(),
            use_cache: true,
        });
        self.attach_callback = Some(attach_cb.clone());

        self.reader_id = Some(reader_id);

        tracing::info!(
            "PullSession {} PLAY: 已注册到Dispatcher runtime={}",
            self.session_id,
            self.runtime_id
        );

        Ok(())
    }

    /// PAUSE - 暂停播放（保留Reader注册）
    pub fn pause(&mut self) -> RtspResult<()> {
        tracing::info!("PullSession {} PAUSE", self.session_id);
        Ok(())
    }

    /// TEARDOWN - 从Dispatcher注销Reader，清理资源
    pub fn shutdown(&mut self) -> RtspResult<()> {
        if let Some(ref reader_id) = self.reader_id {
            // 从thread_local的Dispatcher中注销
            Dispatcher::remove(&self.stream_key);
            tracing::info!(
                "PullSession {} TEARDOWN: 注销Reader {}",
                self.session_id,
                reader_id
            );
        }

        if let Some(cb) = self.attach_callback.take() {
            cb(AttachCommand::Detach {
                runtime_id: self.runtime_id,
            });
        }

        self.reader_id = None;
        self.media_source_info = None;
        self.tcp_writer = None;
        self.track_channels = Arc::new(FxHashMap::default());
        self.udp_targets = Arc::new(FxHashMap::default());
        self.buffer_a.borrow_mut().clear();
        self.buffer_b.borrow_mut().clear();
        self.active_is_a.set(true);
        self.drain_pending.set(false);

        Ok(())
    }

    pub fn set_tcp_writer(&mut self, writer: SharedTcpWriter) {
        self.tcp_writer = Some(writer);
    }

    fn total_queue_len(
        buffer_a: &Rc<RefCell<VecDeque<FrameRef>>>,
        buffer_b: &Rc<RefCell<VecDeque<FrameRef>>>,
    ) -> usize {
        buffer_a.borrow().iter().map(FrameRef::len).sum::<usize>()
            + buffer_b.borrow().iter().map(FrameRef::len).sum::<usize>()
    }

    /// 强制限制发送队列长度，超过上限则丢弃最早的数据
    fn enforce_queue_capacity(
        buffer_a: &Rc<RefCell<VecDeque<FrameRef>>>,
        buffer_b: &Rc<RefCell<VecDeque<FrameRef>>>,
    ) {
        let max_pending = config::get().session.pull.max_pending_packets;
        let mut total = Self::total_queue_len(buffer_a, buffer_b);
        if total <= max_pending {
            return;
        }

        let mut drop_count = total - max_pending;
        while drop_count > 0 && total > 0 {
            let len_a = buffer_a.borrow().front().map(FrameRef::len).unwrap_or(0);
            let len_b = buffer_b.borrow().front().map(FrameRef::len).unwrap_or(0);
            if len_a >= len_b {
                buffer_a.borrow_mut().pop_front();
            } else {
                buffer_b.borrow_mut().pop_front();
            }
            drop_count = drop_count.saturating_sub(len_a.max(len_b));
            total = total.saturating_sub(len_a.max(len_b));
        }
    }

    /// 调度发送队列的清空任务
    fn schedule_drain(
        buffer_a: Rc<RefCell<VecDeque<FrameRef>>>,
        buffer_b: Rc<RefCell<VecDeque<FrameRef>>>,
        active_flag: Rc<Cell<bool>>,
        pending: Rc<Cell<bool>>,
        writer: Option<SharedTcpWriter>,
        tcp_batch_cache: Rc<RefCell<Vec<(u8, Arc<RtpPacket>)>>>,
        udp_batch_cache: Rc<
            RefCell<FxHashMap<usize, (Arc<UdpWriter>, Vec<(SocketAddr, Arc<RtpPacket>)>)>>,
        >,
        runtime: RuntimeHandle,
        session_id: String,
        stream_key: String,
        closed: Rc<Cell<bool>>,
        track_channels: Arc<FxHashMap<String, u8>>,
        udp_targets: Arc<FxHashMap<String, UdpSendTarget>>,
    ) {
        if closed.get() {
            return;
        }
        if pending.replace(true) {
            return;
        }

        if !runtime.is_current() {
            tracing::error!(
                "PullSession {} schedule_drain called outside runtime-{} thread",
                session_id,
                runtime.idx()
            );
            pending.set(false);
            return;
        }

        let runtime_clone = runtime.clone();
        runtime_clone.clone().spawn_local_current(async move {
            PullSession::drain_send_queue(
                buffer_a,
                buffer_b,
                active_flag,
                writer,
                pending,
                tcp_batch_cache,
                udp_batch_cache,
                runtime_clone,
                session_id,
                stream_key,
                closed,
                track_channels,
                udp_targets,
            )
            .await;
        });
    }

    /// 清空发送队列
    #[instrument(
        level = "trace",
        name = "drain_send_queue",
        skip(buffer_a, buffer_b, writer, pending, track_channels, udp_targets, tcp_batch_cache, udp_batch_cache),
        fields(session = %session_id, stream = %stream_key)
    )]
    async fn drain_send_queue(
        buffer_a: Rc<RefCell<VecDeque<FrameRef>>>,
        buffer_b: Rc<RefCell<VecDeque<FrameRef>>>,
        active_flag: Rc<Cell<bool>>,
        writer: Option<SharedTcpWriter>,
        pending: Rc<Cell<bool>>,
        tcp_batch_cache: Rc<RefCell<Vec<(u8, Arc<RtpPacket>)>>>,
        udp_batch_cache: Rc<
            RefCell<FxHashMap<usize, (Arc<UdpWriter>, Vec<(SocketAddr, Arc<RtpPacket>)>)>>,
        >,
        runtime: RuntimeHandle,
        session_id: String,
        stream_key: String,
        closed: Rc<Cell<bool>>,
        track_channels: Arc<FxHashMap<String, u8>>,
        udp_targets: Arc<FxHashMap<String, UdpSendTarget>>,
    ) {
        let mut tcp_batch_storage = tcp_batch_cache.borrow_mut();
        let mut udp_batch_storage = udp_batch_cache.borrow_mut();
        let mut tcp_batch = std::mem::take(&mut *tcp_batch_storage);
        let mut udp_batches = std::mem::take(&mut *udp_batch_storage);

        loop {
            // 切换活跃缓冲区
            let using_a = active_flag.get();
            active_flag.set(!using_a);
            // 从旧的活跃缓冲区获取数据
            let buffer = if using_a {
                buffer_a.clone()
            } else {
                buffer_b.clone()
            };

            let frames: Vec<FrameRef> = {
                let mut slot = buffer.borrow_mut();
                if slot.is_empty() {
                    Vec::new()
                } else {
                    slot.drain(..).collect()
                }
            };

            // 如果没有数据,二次检查是否存在数据,避免频繁创建任务
            if frames.is_empty() {
                pending.set(false);
                let remaining_a = buffer_a.borrow().len();
                let remaining_b = buffer_b.borrow().len();
                if (remaining_a + remaining_b) > 0 {
                    pending.set(true);
                    continue;
                }
                break;
            }

            if let Err(err) = Self::flush_outgoing_batch(
                frames,
                &writer,
                &mut tcp_batch,
                &session_id,
                &stream_key,
                &track_channels,
                &udp_targets,
                &mut udp_batches,
            )
            .await
            {
                pending.set(false);
                match err {
                    FlushError::BrokenPipe => {
                        closed.set(true);
                        buffer_a.borrow_mut().clear();
                        buffer_b.borrow_mut().clear();
                        PullSession::cleanup_closed(&stream_key, runtime.idx());
                        tracing::warn!(
                            stage = "pull.queue.flush_error",
                            session = %session_id,
                            stream = %stream_key,
                            "tcp writer broken pipe, stop draining queue and mark session closed"
                        );
                    }
                    FlushError::Io(e) => {
                        tracing::error!(
                            stage = "pull.queue.flush_error",
                            session = %session_id,
                            stream = %stream_key,
                            error = %e,
                            "drain send queue failed"
                        );
                    }
                }
                return;
            }
        }

        // 归还复用缓存
        tcp_batch.clear();
        udp_batches
            .values_mut()
            .for_each(|(_, batch)| batch.clear());
        udp_batches.clear();
        *tcp_batch_storage = tcp_batch;
        *udp_batch_storage = udp_batches;
    }

    /// 发送一批FrameData
    #[instrument(
        level = "trace",
        name = "flush_outgoing_batch",
        skip(frames, writer, tcp_batch, track_channels, udp_targets, udp_batches),
        fields(batch = frames.len(), session = %session_id, stream = %stream_key)
    )]
    async fn flush_outgoing_batch(
        frames: Vec<FrameRef>,
        writer: &Option<SharedTcpWriter>,
        tcp_batch: &mut Vec<(u8, Arc<RtpPacket>)>,
        session_id: &str,
        stream_key: &str,
        track_channels: &FxHashMap<String, u8>,
        udp_targets: &FxHashMap<String, UdpSendTarget>,
        udp_batches: &mut FxHashMap<usize, (Arc<UdpWriter>, Vec<(SocketAddr, Arc<RtpPacket>)>)>,
    ) -> Result<(), FlushError> {
        let tcp_batch_limit = config::get().session.pull.tcp_batch_size;
        for frame in frames {
            for packet in frame.as_slice().iter().flat_map(|f| f.packets.iter()) {
                if let Some(&channel) = track_channels.get(&packet.track_index) {
                    tcp_batch.push((channel, Arc::clone(packet)));
                    if tcp_batch.len() >= tcp_batch_limit {
                        Self::flush_tcp_batch(writer, tcp_batch, session_id, stream_key).await?;
                    }
                } else if let Some(target) = udp_targets.get(&packet.track_index) {
                    let key = Arc::as_ptr(&target.writer) as usize;
                    let entry = udp_batches
                        .entry(key)
                        .or_insert_with(|| (target.writer.clone(), Vec::new()));
                    entry.1.push((target.destination, Arc::clone(packet)));
                } else {
                    tracing::warn!(
                        "PullSession {} 找不到track {} 的channel",
                        session_id,
                        packet.track_index
                    );
                }
            }
        }
        Self::flush_tcp_batch(writer, tcp_batch, session_id, stream_key).await?;

        for (_, (udp_writer, batch)) in udp_batches.iter_mut() {
            if batch.is_empty() {
                continue;
            }
            let packet_batch = batch.len();
            let mut taken = std::mem::take(batch);
            if let Err(err) = udp_writer.send_batch(&taken).await {
                tracing::error!(
                    stage = "pull.udp_send.error",
                    session = %session_id,
                    stream = %stream_key,
                    error = %err,
                    packet_batch = packet_batch,
                    "udp batch send failed"
                );
            }
            taken.clear();
        }

        Ok(())
    }

    /// 发送TCP批量数据
    async fn flush_tcp_batch(
        writer: &Option<SharedTcpWriter>,
        batch: &mut Vec<(u8, Arc<RtpPacket>)>,
        session_id: &str,
        _stream_key: &str,
    ) -> Result<(), FlushError> {
        if batch.is_empty() {
            return Ok(());
        }

        let shared = match writer {
            Some(w) => w.clone(),
            None => {
                tracing::warn!(
                    "PullSession {} 没有可用的TCP writer，丢弃{}个packet",
                    session_id,
                    batch.len()
                );
                batch.clear();
                return Ok(());
            }
        };

        let mut guard = shared.acquire().await;
        let result = guard.write_rtp_packets(batch.as_slice()).await;
        batch.clear();
        match result {
            Ok(_) => Ok(()),
            Err(err) => {
                if err.kind() == std::io::ErrorKind::BrokenPipe {
                    tracing::warn!(
                        "PullSession {} tcp writer broken pipe, dropping {} packet(s)",
                        session_id,
                        batch.len()
                    );
                    return Err(FlushError::BrokenPipe);
                }
                Err(FlushError::Io(err))
            }
        }
    }
}
