use crate::event::RuntimeHandle;
use crate::media::dispatcher::{Dispatcher, DispatcherEvent};
use crate::rtp::FrameData;
use crate::rtsp::TrackType;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, instrument, trace};

/// 一组完整的视频帧序列
#[derive(Clone)]
pub struct GopData {
    /// 起始时间戳
    pub start_timestamp: u64,
    /// 起始序列号
    pub start_seq: u64,
    /// 帧列表
    pub frames: Vec<Arc<FrameData>>,
    /// 是否包含关键帧
    pub has_key: bool,
}

impl GopData {
    pub fn new(start_timestamp: u64, start_seq: u64, has_key: bool) -> Self {
        Self {
            start_timestamp,
            start_seq,
            frames: Vec::new(),
            has_key,
        }
    }

    pub fn add_frame(&mut self, frame: Arc<FrameData>) {
        self.frames.push(frame);
    }

    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }
}

/// GOP的快照
///
/// - 写入端（RingBuffer）独占,创建新快照
/// - 读取端（Dispatcher）通过Arc共享,只读不写
/// - start_seq/end_seq用于Reader游标对齐,防止旧数据被淘汰后读不到
#[derive(Clone)]
pub struct StorageSnapshot {
    pub gops: Arc<[Arc<GopData>]>,
    pub start_seq: u64,
    pub end_seq: u64,
    pub frames: Arc<[Arc<FrameData>]>,
}

impl StorageSnapshot {
    pub fn empty() -> Self {
        Self {
            gops: Arc::new([]),
            start_seq: 0,
            end_seq: 0,
            frames: Arc::new([]),
        }
    }

    pub fn len(&self) -> usize {
        self.gops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.gops.is_empty()
    }
}

/// RingBuffer 存储GOP的环形缓冲区
pub struct GopRingBuffer {
    /// 流标识符
    stream_key: String,
    /// 当前存储的快照
    current_storage: Arc<StorageSnapshot>,
    /// 最大GOP数量
    max_gop_count: usize,
    /// 已构建的GOP队列
    internal_gops: VecDeque<Arc<GopData>>,
    /// 当前临时GOP
    current_gop: Option<GopData>,
    /// 当前GOP是否包含关键帧
    current_gop_has_key: bool,
    /// 当前GOP是否仅包含音频帧
    current_gop_is_audio_only: bool,
    /// 每个GOP的最大帧数
    max_frames_per_gop: usize,
    /// 非关键帧强制刷新阈值
    force_flush_non_key_frames: usize,
    /// 仅音频帧强制刷新阈值
    force_flush_audio_frames: usize,
    /// 运行时上分发器映射
    runtime_handles: HashMap<usize, RuntimeDispatchEntry>,
    /// 帧计数器
    frame_count: u64,
}

/// 每个运行时的分发条目
struct RuntimeDispatchEntry {
    /// 引用计数,当值为1时, 才能移除改运行时上的分发器
    ref_count: usize,

    /// 分片发送器集合
    shard_senders: Arc<Vec<UnboundedSender<DispatcherEvent>>>,
}

impl GopRingBuffer {
    /// 创建新的 GopRingBuffer 实例
    pub fn new(
        stream_key: String,
        max_gop_count: usize,
        max_frames_per_gop: usize,
        force_flush_non_key_frames: usize,
        force_flush_audio_frames: usize,
    ) -> Self {
        Self {
            stream_key,
            current_storage: Arc::new(StorageSnapshot::empty()),
            max_gop_count,
            internal_gops: VecDeque::new(),
            current_gop: None,
            current_gop_has_key: false,
            current_gop_is_audio_only: true,
            max_frames_per_gop,
            force_flush_non_key_frames,
            force_flush_audio_frames,
            runtime_handles: HashMap::new(),
            frame_count: 0,
        }
    }

    /// 写入一帧数据
    #[instrument(
        level = "trace",
        name = "ringbuffer_write_frame",
        skip(self, frame),
        fields(stream = %self.stream_key)
    )]
    pub fn write_frame(&mut self, frame: FrameData) {
        let seq = self.frame_count;
        self.frame_count = self.frame_count.wrapping_add(1);
        let frame_arc = Arc::new(frame);

        // 遇到关键帧，开始新GOP
        if frame_arc.is_key {
            if let Some(gop) = self.current_gop.take() {
                self.flush_gop(gop);
                self.current_gop_has_key = false;
                self.current_gop_is_audio_only = true;
            }
            let mut gop = GopData::new(frame_arc.timestamp_ms, seq, true);
            gop.add_frame(frame_arc);
            self.current_gop_has_key = true;
            self.current_gop_is_audio_only = frame_is_audio_only(&gop);
            self.current_gop = Some(gop);
            return;
        }

        // 添加到当前GOP
        if let Some(ref mut gop) = self.current_gop {
            if frame_contains_video(&frame_arc) {
                self.current_gop_is_audio_only = false;
            }
            gop.add_frame(frame_arc);
        } else {
            // 还没遇到关键帧，创建临时GOP
            let mut gop = GopData::new(frame_arc.timestamp_ms, seq, self.current_gop_has_key);
            gop.add_frame(frame_arc);
            self.current_gop_has_key = false;
            self.current_gop_is_audio_only = frame_is_audio_only(&gop);
            self.current_gop = Some(gop);
        }

        self.maybe_force_flush();
    }

    /// Flush GOP到storage - 创建新的不可变快照
    #[instrument(
        level = "trace",
        name = "gop_flush",
        skip(self, gop),
        fields(stream = %self.stream_key)
    )]
    fn flush_gop(&mut self, gop: GopData) {
        let gop_start_seq = gop.start_seq;
        let frame_count = gop.frame_count();
        let start_ts = gop.start_timestamp;

        self.internal_gops.push_back(Arc::new(gop));
        // 至少保留最近的IDR GOP，不要把最后的关键帧GOP挤掉
        let mut last_idr_idx = self.internal_gops.iter().rposition(|g| g.has_key);

        while self.internal_gops.len() > self.max_gop_count {
            if let Some(idx) = last_idr_idx {
                if idx == 0 {
                    // 队首就是最新的IDR，不能再丢
                    break;
                }
                // 弹出前部非IDR，保持最后一个IDR仍在窗口内
                self.internal_gops.pop_front();
                last_idr_idx = last_idr_idx.map(|i| i.saturating_sub(1));
            } else {
                // 没有IDR，正常淘汰
                self.internal_gops.pop_front();
            }
        }

        let snapshot_gops: Arc<[Arc<GopData>]> = self
            .internal_gops
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .into();

        let start_seq = snapshot_gops
            .first()
            .map(|g| g.start_seq)
            .unwrap_or(self.frame_count);
        let end_seq = snapshot_gops
            .last()
            .map(|g| g.start_seq + g.frame_count() as u64)
            .unwrap_or(start_seq);

        let total_frames: usize = snapshot_gops.iter().map(|g| g.frame_count()).sum();
        let mut flat_frames = Vec::with_capacity(total_frames);
        for gop in snapshot_gops.iter() {
            flat_frames.extend(gop.frames.iter().cloned());
        }
        let flat_frames: Arc<[Arc<FrameData>]> = flat_frames.into();
        let new_storage = Arc::new(StorageSnapshot {
            gops: snapshot_gops,
            start_seq,
            end_seq,
            frames: flat_frames,
        });
        self.current_storage = new_storage.clone();

        info!(
            stage = "ringbuffer.flush",
            stream = %self.stream_key,
            gop_start_seq = gop_start_seq,
            frame_count = frame_count,
            start_timestamp = start_ts,
            "ringbuffer wrote GOP"
        );

        // 通知所有Dispatcher，传递新的Storage
        self.notify_dispatchers(new_storage);
    }

    /// 通知所有已注册的Dispatcher
    #[instrument(
        level = "trace",
        name = "ringbuffer_notify",
        skip(self, new_storage),
        fields(stream = %self.stream_key, runtime_count = self.runtime_handles.len())
    )]
    fn notify_dispatchers(&mut self, new_storage: Arc<StorageSnapshot>) {
        let stream_key = Arc::new(self.stream_key.clone());
        let shard_idx = Dispatcher::shard_index(&self.stream_key);

        for (runtime_id, entry) in &self.runtime_handles {
            if let Some(tx) = entry.shard_senders.get(shard_idx) {
                trace!(
                    stage = "ringbuffer.notify",
                    stream = %stream_key,
                    runtime = runtime_id,
                    gop_end_seq = new_storage.end_seq,
                    shard = shard_idx,
                    "ringbuffer notified dispatcher"
                );
                let _ = tx.send(DispatcherEvent::Update {
                    stream_key: stream_key.clone(),
                    storage: new_storage.clone(),
                });
            } else {
                trace!(
                    stage = "ringbuffer.notify.skip",
                    stream = %stream_key,
                    runtime = runtime_id,
                    shard = shard_idx,
                    "shard sender missing"
                );
            }
        }
    }

    /// Attach到指定runtime的Dispatcher
    ///
    /// # Arguments
    /// * `runtime_handle`: 拉流运行时的句柄
    /// * `use_cache`: 是否通过Dispatcher缓存数据
    #[instrument(
        level = "trace",
        name = "ringbuffer_attach",
        skip(self, runtime_handle),
        fields(stream = %self.stream_key, use_cache = use_cache)
    )]
    pub fn attach(&mut self, runtime_handle: RuntimeHandle, use_cache: bool) {
        let runtime_id = runtime_handle.idx();
        let stream_key = self.stream_key.clone();
        let initial_storage = self.current_storage.clone();

        if let Some(entry) = self.runtime_handles.get_mut(&runtime_id) {
            entry.ref_count += 1;
            if use_cache {
                let shard_idx = shard_idx_for_stream(&stream_key);
                if let Some(tx) = entry.shard_senders.get(shard_idx) {
                    let _ = tx.send(DispatcherEvent::Update {
                        stream_key: Arc::new(stream_key.clone()),
                        storage: initial_storage.clone(),
                    });
                }
            }
            tracing::info!(
                "RingBuffer reuse runtime attachment: stream={}, runtime={}, ref_count={}",
                self.stream_key,
                runtime_id,
                entry.ref_count
            );
            // 分发器已经被创建了, 只增加引用计数器.
            return;
        }

        let shard_senders = Dispatcher::shard_senders(runtime_id)
            .unwrap_or_else(|| panic!("shard senders missing for runtime {}", runtime_id));

        let shard_idx = shard_idx_for_stream(&stream_key);
        let entry = RuntimeDispatchEntry {
            ref_count: 1,
            shard_senders: shard_senders.clone(),
        };

        if use_cache {
            if let Some(tx) = entry.shard_senders.get(shard_idx) {
                let _ = tx.send(DispatcherEvent::Update {
                    stream_key: Arc::new(stream_key.clone()),
                    storage: initial_storage.clone(),
                });
            }
        }

        self.runtime_handles.insert(runtime_id, entry);

        tracing::info!(
            "RingBuffer attach runtime: stream={}, runtime={}, use_cache={}, ref_count=1",
            self.stream_key,
            runtime_id,
            use_cache,
        );
    }

    /// Detach指定runtime的Dispatcher
    pub fn detach(&mut self, runtime_id: usize) {
        if let Some(entry) = self.runtime_handles.get_mut(&runtime_id) {
            if entry.ref_count > 1 {
                entry.ref_count -= 1;
                tracing::info!(
                    "RingBuffer decrement runtime attachment: stream={}, runtime={}, ref_count={}",
                    self.stream_key,
                    runtime_id,
                    entry.ref_count
                );
                return;
            }
        }

        if self.runtime_handles.remove(&runtime_id).is_some() {
            tracing::info!(
                "RingBuffer detach runtime: stream={}, runtime={}",
                self.stream_key,
                runtime_id
            );
        }
    }

    /// 获取当前storage的clone
    pub fn get_storage(&self) -> Arc<StorageSnapshot> {
        self.current_storage.clone()
    }

    /// 获取当前写入序号
    pub fn write_seq(&self) -> u64 {
        self.frame_count
    }

    /// 根据当前GOP状态决定是否强制flush
    /// 帧数超过阈值则flush
    /// 仅音频帧超过阈值则flush
    fn maybe_force_flush(&mut self) {
        let should_flush = if let Some(ref gop) = self.current_gop {
            if self.current_gop_has_key {
                gop.frame_count() >= self.max_frames_per_gop
            } else {
                let threshold = if self.current_gop_is_audio_only {
                    self.force_flush_audio_frames.max(1)
                } else {
                    self.force_flush_non_key_frames.max(1)
                };
                gop.frame_count() >= threshold
            }
        } else {
            false
        };

        if should_flush {
            if let Some(gop) = self.current_gop.take() {
                self.flush_gop(gop);
            }
            self.current_gop_has_key = false;
            self.current_gop_is_audio_only = true;
        }
    }
}

fn frame_contains_video(frame: &FrameData) -> bool {
    frame
        .packets
        .iter()
        .any(|pkt| matches!(pkt.track_type, TrackType::Video))
}

fn frame_is_audio_only(frame: &GopData) -> bool {
    frame.frames.iter().all(|f| {
        f.packets
            .iter()
            .all(|pkt| matches!(pkt.track_type, TrackType::Audio))
    })
}

fn shard_idx_for_stream(stream_key: &str) -> usize {
    Dispatcher::shard_index(stream_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        event::RuntimePool,
        media::dispatcher::Dispatcher,
        rtp::{RtpHeader, RtpPacket},
    };
    use bytes::Bytes;
    use std::time::Duration;
    use tokio::time::sleep;

    fn build_packet(timestamp_ms: u64, track_type: TrackType, is_key: bool) -> Arc<RtpPacket> {
        let nal = if is_key { 0x65 } else { 0x41 };
        let header = RtpHeader {
            version: 2,
            padding: false,
            extension: false,
            csrc_count: 0,
            marker: true,
            payload_type: 96,
            sequence: (timestamp_ms % u16::MAX as u64) as u16,
            timestamp: timestamp_ms as u32,
            ssrc: 1,
        };
        let payload = Bytes::from(vec![nal, 0x00, 0x01]);
        let mut raw_buf = header.serialize().to_vec();
        raw_buf.extend_from_slice(&payload);
        let raw_packet = Bytes::from(raw_buf);

        Arc::new(RtpPacket::new(
            raw_packet,
            header,
            payload,
            track_type,
            match track_type {
                TrackType::Audio => "audio".to_string(),
                _ => "video".to_string(),
            },
            tokio::time::Instant::now(),
        ))
    }

    fn make_frame(timestamp_ms: u64, is_key: bool, track_type: TrackType) -> FrameData {
        FrameData {
            timestamp_ms,
            is_key,
            packets: vec![build_packet(timestamp_ms, track_type, is_key)],
        }
    }

    #[test]
    fn gop_flushes_on_new_key_frame() {
        let mut buffer = GopRingBuffer::new("test".to_string(), 5, 30, 60, 100);

        buffer.write_frame(make_frame(0, true, TrackType::Video));
        for i in 1..=10 {
            buffer.write_frame(make_frame(i * 40, false, TrackType::Video));
        }
        assert_eq!(buffer.get_storage().gops.len(), 0);

        buffer.write_frame(make_frame(500, true, TrackType::Video));
        let storage = buffer.get_storage();
        assert_eq!(storage.gops.len(), 1);
        assert_eq!(storage.gops[0].frame_count(), 11);
    }

    #[test]
    fn retains_recent_idr_gops() {
        let mut buffer = GopRingBuffer::new("idr_retention".to_string(), 3, 20, 20, 50);

        for gop_idx in 0..5 {
            let mut i = 20;
            if gop_idx == 0 {
                buffer.write_frame(make_frame(gop_idx * 1000, true, TrackType::Video));
                i = 5;
            }

            for frame_idx in 1..=i {
                buffer.write_frame(make_frame(
                    gop_idx * 1000 + frame_idx * 40,
                    false,
                    TrackType::Video,
                ));
            }
        }

        let storage = buffer.get_storage();
        assert_eq!(storage.gops.len(), 4);
        assert!(storage.gops[0].has_key);
    }

    #[test]
    fn flushes_non_key_gop_when_threshold_reached() {
        let mut buffer = GopRingBuffer::new("non_key_flush".to_string(), 5, 10, 5, 10);
        for idx in 0..5 {
            buffer.write_frame(make_frame(idx * 40, false, TrackType::Video));
        }

        let storage = buffer.get_storage();
        assert_eq!(storage.gops.len(), 1);
        assert_eq!(storage.gops[0].frame_count(), 5);
        assert!(!storage.gops[0].has_key);
    }

    #[test]
    fn flushes_audio_only_gop_when_threshold_reached() {
        let mut buffer = GopRingBuffer::new("audio_only".to_string(), 5, 10, 10, 3);
        for idx in 0..3 {
            buffer.write_frame(make_frame(idx * 20, false, TrackType::Audio));
        }

        let storage = buffer.get_storage();
        assert_eq!(storage.gops.len(), 1);
        assert_eq!(storage.gops[0].frame_count(), 3);
        assert!(storage.gops[0].frames.iter().all(|f| {
            f.packets
                .iter()
                .all(|pkt| matches!(pkt.track_type, TrackType::Audio))
        }));
    }

    #[tokio::test]
    async fn attach_and_detach_runtime_entries() {
        let pool = RuntimePool::new(Some(2));
        let handles = pool.handles();

        for handle in &handles {
            Dispatcher::init_runtime(handle.clone());
        }

        for handle in &handles {
            let runtime_id = handle.idx();
            let mut attempts = 0;
            while Dispatcher::shard_senders(runtime_id).is_none() && attempts < 20 {
                sleep(Duration::from_millis(10)).await;
                attempts += 1;
            }
            assert!(
                Dispatcher::shard_senders(runtime_id).is_some(),
                "shard senders not initialized for runtime {runtime_id}"
            );
        }

        let mut buffer = GopRingBuffer::new("stream_attach".to_string(), 4, 10, 5, 5);
        for handle in &handles {
            buffer.attach(handle.clone(), true);
        }
        assert_eq!(buffer.runtime_handles.len(), handles.len());

        for handle in &handles {
            buffer.detach(handle.idx());
        }
        assert_eq!(buffer.runtime_handles.len(), 0);
    }
}
