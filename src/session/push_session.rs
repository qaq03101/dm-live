use anyhow::{Context, Result};
use bytes::Bytes;
use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use crate::{
    event::RuntimeHandle,
    media::{AttachCommand, MediaSource, MediaSourceInfo},
    rtp::RtpReceiver,
    rtsp::SdpTrack,
    session::state::{RtspError, RtspResult},
};

/// PushSession - 推流会话
///
/// 用于管理单个RTSP推流会话的状态和操作。
pub struct PushSession {
    /// 会话ID
    pub session_id: String,
    /// 流键
    pub stream_key: String,
    /// 运行时ID
    runtime_id: usize,
    /// 媒体源实例
    media_source: Option<Rc<RefCell<MediaSource>>>,
    /// 媒体信息
    media_source_info: Option<Arc<MediaSourceInfo>>,
    /// RTP接收器
    pub rtp_receiver: Option<RtpReceiver>,
    /// 已设置的tracks
    pub setup_tracks: HashMap<String, SdpTrack>,
    /// 是否已注册
    pub registered: bool,
}

/// 附加到 MediaSource 的命令分发函数
fn dispatch_attach_command(
    runtime_handle: RuntimeHandle,
    stream_key: String,
    command: AttachCommand,
) {
    match command {
        AttachCommand::Attach {
            handle: target,
            use_cache,
        } => {
            runtime_handle.spawn_local(async move {
                MediaSource::with_local_mut(&stream_key, |inner| {
                    inner.ring_buffer_mut().attach(target, use_cache);
                });
            });
        }
        AttachCommand::Detach { runtime_id } => {
            runtime_handle.spawn_local(async move {
                MediaSource::with_local_mut(&stream_key, |inner| {
                    inner.ring_buffer_mut().detach(runtime_id);
                });
            });
        }
    }
}

impl PushSession {
    /// 创建新的 PushSession 实例
    pub fn new(session_id: String, stream_key: String, runtime_id: usize) -> Self {
        Self {
            session_id,
            stream_key,
            runtime_id,
            media_source: None,
            media_source_info: None,
            rtp_receiver: None,
            setup_tracks: HashMap::new(),
            registered: false,
        }
    }

    ///  解析 SDP 并构建 MediaSource。
    pub fn configure_media_source(
        &mut self,
        sdp_text: String,
        sdp_tracks: Vec<Arc<SdpTrack>>,
    ) -> RtspResult<()> {
        let (mut media_source, _info) = MediaSource::new(self.stream_key.clone(), self.runtime_id);
        media_source.update_info(Some(sdp_text), sdp_tracks);
        let updated_info = media_source.info().clone();

        let source_rc = Rc::new(RefCell::new(media_source));
        MediaSource::register_local(&self.stream_key, source_rc.clone());

        self.media_source = Some(source_rc);
        self.media_source_info = Some(updated_info);
        self.registered = false;

        tracing::info!(
            "PushSession {} ANNOUNCE: 创建MediaSource for {}",
            self.session_id,
            self.stream_key
        );
        Ok(())
    }

    /// 标记 track 已 SETUP
    pub fn register_track(&mut self, track: SdpTrack) -> RtspResult<()> {
        let track_id = track.control_url.clone();
        self.setup_tracks.insert(track_id.clone(), track);

        tracing::info!(
            "PushSession {} SETUP: 注册track {}",
            self.session_id,
            track_id
        );
        Ok(())
    }

    /// 注册媒体到全局媒体表
    pub fn start_recording(&mut self, runtime_handle: RuntimeHandle) -> RtspResult<()> {
        let _media_source_handle = self
            .media_source
            .as_ref()
            .cloned()
            .ok_or(RtspError::PushSessionNotInitialized)?;

        let info = self
            .media_source_info
            .as_ref()
            .cloned()
            .ok_or(RtspError::PushSessionNotInitialized)?;

        let tracks: Vec<SdpTrack> = self.setup_tracks.values().cloned().collect();

        let receiver = RtpReceiver::new(&tracks);

        self.rtp_receiver = Some(receiver);

        if !self.registered {
            let stream_key = self.stream_key.clone();
            MediaSource::register(
                self.runtime_id,
                info.clone(),
                Box::new(move |command| {
                    dispatch_attach_command(runtime_handle.clone(), stream_key.clone(), command);
                }),
            );

            self.registered = true;
        }

        tracing::info!(
            "PushSession {} RECORD: 启动推流，配置 {} 个track",
            self.session_id,
            tracks.len()
        );
        Ok(())
    }

    /// 处理输入的 RTP 数据。
    ///
    /// # Arguments
    /// * `track_id` - 轨道标识符
    /// * `data` - RTP 数据包
    pub fn input_rtp(&mut self, track_id: &str, data: Bytes) -> Result<()> {
        let receiver = self
            .rtp_receiver
            .as_mut()
            .context("RTP Receiver not initialized")?;

        let sorted_packets = receiver.input_rtp(track_id.to_string(), data)?;

        if let Some(media_source) = &self.media_source {
            let mut source = media_source.borrow_mut();
            for packet in sorted_packets {
                source.push_to_cache(packet);
            }
        }

        Ok(())
    }

    /// TEARDOWN - 清理 Session 与 MediaSource 状态。
    pub fn shutdown(&mut self) -> RtspResult<()> {
        self.rtp_receiver = None;
        self.media_source = None;
        self.media_source_info = None;
        self.registered = false;

        tracing::info!("PushSession {} TEARDOWN: 清理资源", self.session_id);
        Ok(())
    }
}
