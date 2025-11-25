use crate::config;
use crate::rtp::{PacketCache, RtpPacket};
use crate::rtsp::SdpTrack;
use crate::{event::RuntimeHandle, media::GopRingBuffer};
use dashmap::DashMap;
use once_cell::sync::OnceCell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

pub type SdpInfo = String;

/// AttachCommand - 控制 RingBuffer 在不同 runtime 上的附着/解绑。
///
/// * `Attach`: 将 Reader 接入指定 runtime 的 RingBuffer，可选择是否复用缓存。
/// * `Detach`: 根据 runtime_id 主动解除附着。
pub enum AttachCommand {
    Attach {
        handle: RuntimeHandle,
        use_cache: bool,
    },
    Detach {
        runtime_id: usize,
    },
}

/// 拉流 Session 与 RingBuffer 的桥梁回调。
pub type AttachCallback = Box<dyn Fn(AttachCommand) + Send + Sync>;

#[derive(Clone)]
struct RegistryEntry {
    info: Weak<MediaSourceInfo>,
    callback: Arc<AttachCallback>,
}

type RegistrySlot = DashMap<String, RegistryEntry>;

static MEDIA_SOURCE_REGISTRY: OnceCell<Vec<RegistrySlot>> = OnceCell::new();

// 本地线程媒体源映射表
thread_local! {
    static LOCAL_MEDIA_SOURCES: RefCell<HashMap<String, Rc<RefCell<MediaSource>>>> =
        RefCell::new(HashMap::new());
}

/// 管理全局所有线程上的 MediaSource 注册操作
///
/// # 用途
/// - 推流 Session 将源写入所属 runtime 的 slot
/// - 拉流 Session 遍历 slot，实现跨 runtime 查询
pub struct MediaSourceRegistry;

impl MediaSourceRegistry {
    /// 初始化全局注册表
    pub fn init(runtime_count: usize) {
        let _ = MEDIA_SOURCE_REGISTRY.set(Self::build(runtime_count));
    }

    /// 构建注册表槽位
    fn build(runtime_count: usize) -> Vec<RegistrySlot> {
        let count = runtime_count.max(1);
        (0..count).map(|_| DashMap::new()).collect()
    }

    /// 获取所有注册表槽位
    fn slots() -> &'static [RegistrySlot] {
        MEDIA_SOURCE_REGISTRY
            .get()
            .expect("MediaSourceRegistry not initialized")
    }

    /// 获取指定运行时的注册表槽位
    fn slot(runtime_id: usize) -> &'static RegistrySlot {
        let slots = Self::slots();
        let idx = runtime_id.min(slots.len() - 1);
        &slots[idx]
    }

    /// 查找注册表条目
    fn find_entry(stream_key: &str) -> Option<RegistryEntry> {
        for slot in Self::slots() {
            if let Some(entry) = slot.get(stream_key) {
                return Some(entry.clone());
            }
        }
        None
    }

    /// 注册条目
    fn register(runtime_id: usize, stream_key: String, entry: RegistryEntry) {
        Self::slot(runtime_id).insert(stream_key, entry);
    }

    /// 注销条目
    fn unregister(runtime_id: usize, stream_key: &str) {
        Self::slot(runtime_id).remove(stream_key);
    }

    /// 列出所有注册的流键
    fn list_keys() -> Vec<String> {
        let mut keys = Vec::new();
        for slot in Self::slots() {
            for entry in slot.iter() {
                keys.push(entry.key().clone());
            }
        }
        keys
    }

    /// 获取当前活跃的媒体源数量
    fn active_count() -> usize {
        Self::slots().iter().map(|slot| slot.len()).sum()
    }
}

/// MediaSourceInfo 媒体只读信息
#[derive(Clone)]
pub struct MediaSourceInfo {
    /// 流标识符
    pub stream_key: String,
    /// SDP 文本
    pub sdp_info: Option<SdpInfo>,
    /// SDP Track 列表
    pub tracks: Vec<Arc<SdpTrack>>,
    /// 创建时间
    pub created_at: u64,
}

impl MediaSourceInfo {
    pub fn new(stream_key: String) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            stream_key,
            sdp_info: None,
            tracks: Vec::new(),
            created_at,
        }
    }

    pub fn with_sdp(mut self, sdp_info: SdpInfo) -> Self {
        self.sdp_info = Some(sdp_info);
        self
    }

    pub fn with_tracks(mut self, tracks: Vec<Arc<SdpTrack>>) -> Self {
        self.tracks = tracks;
        self
    }
}

/// MediaSource - 媒体源的可写实现，负责接收 RTP 包并存储到 GOP 环形缓冲区。
pub struct MediaSource {
    /// 只读媒体信息
    info: Arc<MediaSourceInfo>,
    /// GOP 环形缓冲区
    ring_buffer: GopRingBuffer,
    /// RTP 包缓存
    packet_cache: PacketCache,
    /// 注册的运行时 ID
    registry_runtime_id: usize,
}

impl MediaSource {
    /// 创建新的 MediaSource。
    ///
    /// # Returns
    /// * `(MediaSource, Arc<MediaSourceInfo>)` - 可写实现与只读视图。
    pub fn new(stream_key: String, runtime_id: usize) -> (Self, Arc<MediaSourceInfo>) {
        Self::new_with_override(stream_key, runtime_id, None)
    }

    pub fn new_with_override(
        stream_key: String,
        runtime_id: usize,
        max_gop_count: Option<usize>,
    ) -> (Self, Arc<MediaSourceInfo>) {
        let settings = config::get();
        let gop_cfg = &settings.media.ring_buffer;
        let cache_cfg = &settings.media.packet_cache;

        let gop_count = max_gop_count.unwrap_or(gop_cfg.max_gop_count);
        let info = Arc::new(MediaSourceInfo::new(stream_key.clone()));
        let ring_buffer = GopRingBuffer::new(
            stream_key,
            gop_count,
            gop_cfg.max_frames_per_gop,
            gop_cfg.force_flush_non_key_frames,
            gop_cfg.force_flush_audio_frames,
        );
        let packet_cache = PacketCache::with_max_packets(cache_cfg.max_packets_per_frame);

        let source = Self {
            info: info.clone(),
            ring_buffer,
            packet_cache,
            registry_runtime_id: runtime_id,
        };

        tracing::info!("创建MediaSource: {}", info.stream_key);
        (source, info)
    }

    /// 更新媒体信息
    pub fn update_info(&mut self, sdp_info: Option<SdpInfo>, tracks: Vec<Arc<SdpTrack>>) {
        let mut new_info = (*self.info).clone();
        new_info.sdp_info = sdp_info;
        new_info.tracks = tracks;
        self.info = Arc::new(new_info);
    }

    /// 获取只读信息
    pub fn info(&self) -> &Arc<MediaSourceInfo> {
        &self.info
    }

    /// 获取RingBuffer的可变引用
    pub fn ring_buffer_mut(&mut self) -> &mut GopRingBuffer {
        &mut self.ring_buffer
    }

    /// 注册到当前线程的本地表
    pub fn register_local(stream_key: &str, source: Rc<RefCell<Self>>) {
        LOCAL_MEDIA_SOURCES.with(|map| {
            let old = {
                let mut guard = map.borrow_mut();
                guard.insert(stream_key.to_string(), source)
            };
            drop(old);
        });
    }

    /// 从本地表移除
    pub fn remove_local(stream_key: &str) {
        LOCAL_MEDIA_SOURCES.with(|map| {
            let removed = { map.borrow_mut().remove(stream_key) };
            drop(removed);
        });
    }

    /// 查找媒体源,在媒体源的ringbuffer中执行attach
    ///
    /// # Arguments
    /// * `stream_key`: 媒体源标识符
    /// * `f`: 闭包，在媒体源的ringbuffer中执行attach
    pub fn with_local_mut<F, R>(stream_key: &str, f: F) -> Option<R>
    where
        F: FnOnce(&mut MediaSource) -> R,
    {
        LOCAL_MEDIA_SOURCES
            .with(|map| map.borrow().get(stream_key).cloned())
            .map(|rc| {
                let mut borrow = rc.borrow_mut();
                f(&mut *borrow)
            })
    }

    /// 推入 RTP 包到 PacketCache。
    ///
    /// # Arguments
    /// * `packet`: 待推入的 RTP 包
    pub fn push_to_cache(&mut self, packet: RtpPacket) {
        if let Some(frame_data) = self.packet_cache.push(Arc::new(packet)) {
            let ts = frame_data.timestamp_ms;
            let pkt_cnt = frame_data.packets.len();
            let is_key = frame_data.is_key;
            debug!(
                target: "cache.flush",
                stream = %self.info.stream_key,
                timestamp_ms = ts,
                packet_count = pkt_cnt,
                is_key = is_key,
                "cache flushed frame"
            );
            self.ring_buffer.write_frame(frame_data);
        }
    }

    /// 强制flush PacketCache到RingBuffer
    pub fn flush_cache(&mut self) {
        if let Some(frame_data) = self.packet_cache.flush() {
            self.ring_buffer.write_frame(frame_data);
        }
    }

    /// 注册到全局媒体表
    ///
    /// # Arguments
    /// - runtime_id: 所属runtime
    /// - info: 媒体信息
    /// - attach_callback: 注册到ringbuffer的回调
    pub fn register(
        runtime_id: usize,
        info: Arc<MediaSourceInfo>,
        attach_callback: AttachCallback,
    ) {
        let stream_key = info.stream_key.clone();
        let entry = RegistryEntry {
            info: Arc::downgrade(&info),
            callback: Arc::new(attach_callback),
        };
        MediaSourceRegistry::register(runtime_id, stream_key.clone(), entry);
        tracing::info!("注册MediaSource到runtime-{}: {}", runtime_id, stream_key);
    }

    /// 从全局表注销
    pub fn unregister(runtime_id: usize, stream_key: &str) {
        MediaSourceRegistry::unregister(runtime_id, stream_key);
        tracing::info!("从runtime-{}注销MediaSource: {}", runtime_id, stream_key);
    }
}

/// 全局查找接口
impl MediaSource {
    /// 从全局表查找MediaSource信息
    pub fn find_info(stream_key: &str) -> Option<Arc<MediaSourceInfo>> {
        MediaSourceRegistry::find_entry(stream_key).and_then(|entry| entry.info.upgrade())
    }

    /// 从全局表获取attach回调
    pub fn find_attach_callback(stream_key: &str) -> Option<Arc<AttachCallback>> {
        MediaSourceRegistry::find_entry(stream_key).map(|entry| entry.callback.clone())
    }

    /// 检查MediaSource是否存在
    pub fn exists(stream_key: &str) -> bool {
        MediaSourceRegistry::find_entry(stream_key).is_some()
    }

    /// 获取当前活跃的媒体源数量
    pub fn active_count() -> usize {
        MediaSourceRegistry::active_count()
    }

    /// 列出所有活跃的stream keys
    pub fn list_active_streams() -> Vec<String> {
        MediaSourceRegistry::list_keys()
    }
}

impl Drop for MediaSource {
    fn drop(&mut self) {
        // 从全局表移除
        Self::unregister(self.registry_runtime_id, &self.info.stream_key);
        Self::remove_local(&self.info.stream_key);
        tracing::info!("销毁MediaSource: {}", self.info.stream_key);
    }
}
