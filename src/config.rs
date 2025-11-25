use anyhow::Result;
use once_cell::sync::OnceCell;
use serde::Deserialize;

static SETTINGS: OnceCell<AppConfig> = OnceCell::new();

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub runtime: RuntimeConfig,
    pub dispatcher: DispatcherConfig,
    pub media: MediaConfig,
    pub rtp: RtpConfig,
    pub io: IoConfig,
    pub session: SessionConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            runtime: RuntimeConfig::default(),
            dispatcher: DispatcherConfig::default(),
            media: MediaConfig::default(),
            rtp: RtpConfig::default(),
            io: IoConfig::default(),
            session: SessionConfig::default(),
        }
    }
}

impl AppConfig {
    fn normalize(mut self) -> Self {
        self.dispatcher.shard_count = self.dispatcher.shard_count.max(1);
        self.dispatcher.frame_batch_size = self.dispatcher.frame_batch_size.max(1);
        self.media.ring_buffer.max_gop_count = self.media.ring_buffer.max_gop_count.max(1);
        self.media.ring_buffer.max_frames_per_gop =
            self.media.ring_buffer.max_frames_per_gop.max(1);
        self.media.ring_buffer.force_flush_non_key_frames =
            self.media.ring_buffer.force_flush_non_key_frames.max(1);
        self.media.ring_buffer.force_flush_audio_frames =
            self.media.ring_buffer.force_flush_audio_frames.max(1);
        self.media.packet_cache.max_packets_per_frame =
            self.media.packet_cache.max_packets_per_frame.max(1);
        self.rtp.sorter.max_buffer_size = self.rtp.sorter.max_buffer_size.max(1);
        self.rtp.sorter.max_buffer_ms = self.rtp.sorter.max_buffer_ms.max(1);
        self.rtp.sorter.max_distance = self.rtp.sorter.max_distance.max(1);
        self.rtp.udp.linux_max_batch = self.rtp.udp.linux_max_batch.max(1);
        self.session.pull.max_pending_packets = self.session.pull.max_pending_packets.max(1);
        self.session.pull.send_queue_capacity = self.session.pull.send_queue_capacity.max(1);
        self.session.pull.tcp_batch_size = self.session.pull.tcp_batch_size.max(1);
        self.session.pull.merge_window_ms = self.session.pull.merge_window_ms.max(0);
        self.session.rtsp.max_pending_udp = self.session.rtsp.max_pending_udp.max(1);
        self.session.rtsp.udp_queue_capacity = self.session.rtsp.udp_queue_capacity.max(1);
        self.session.rtsp.udp_recv_buffer = self.session.rtsp.udp_recv_buffer.max(1);
        self
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct IoConfig {
    pub mock_socket_io: bool,
}

impl Default for IoConfig {
    fn default() -> Self {
        Self {
            mock_socket_io: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind_ip: String,
    pub port: u16,
    pub max_connections: usize,
    pub workers: Option<usize>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_ip: "0.0.0.0".to_string(),
            port: 9090,
            max_connections: 50_000,
            workers: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    pub fallback_worker_count: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            fallback_worker_count: 4,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DispatcherConfig {
    pub shard_count: usize,
    pub frame_batch_size: usize,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            shard_count: 32,
            frame_batch_size: 64,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RingBufferConfig {
    pub max_gop_count: usize,
    pub max_frames_per_gop: usize,
    pub force_flush_non_key_frames: usize,
    pub force_flush_audio_frames: usize,
}

impl Default for RingBufferConfig {
    fn default() -> Self {
        Self {
            max_gop_count: 3,
            max_frames_per_gop: 60,
            force_flush_non_key_frames: 16,
            force_flush_audio_frames: 8,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PacketCacheConfig {
    pub max_packets_per_frame: usize,
}

impl Default for PacketCacheConfig {
    fn default() -> Self {
        Self {
            max_packets_per_frame: 200,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MediaConfig {
    pub ring_buffer: RingBufferConfig,
    pub packet_cache: PacketCacheConfig,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            ring_buffer: RingBufferConfig::default(),
            packet_cache: PacketCacheConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PacketSorterConfig {
    pub max_buffer_size: usize,
    pub max_buffer_ms: u64,
    pub max_distance: u16,
}

impl Default for PacketSorterConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 2024,
            max_buffer_ms: 2000,
            max_distance: 500,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct UdpConfig {
    pub linux_max_batch: usize,
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self {
            linux_max_batch: 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RtpConfig {
    pub sorter: PacketSorterConfig,
    pub udp: UdpConfig,
}

impl Default for RtpConfig {
    fn default() -> Self {
        Self {
            sorter: PacketSorterConfig::default(),
            udp: UdpConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DefaultMediaInfo {
    pub schema: String,
    pub host: String,
    pub port: u16,
    pub app: String,
    pub stream: String,
    pub protocol: String,
}

impl Default for DefaultMediaInfo {
    fn default() -> Self {
        Self {
            schema: "rtsp".to_string(),
            host: "localhost".to_string(),
            port: 554,
            app: "live".to_string(),
            stream: "test".to_string(),
            protocol: "rtsp".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RtspSessionConfig {
    pub session_timeout_secs: u64,
    pub max_pending_udp: usize,
    pub udp_queue_capacity: usize,
    pub udp_recv_buffer: usize,
    pub server_header: String,
    pub default_media: DefaultMediaInfo,
}

impl Default for RtspSessionConfig {
    fn default() -> Self {
        Self {
            session_timeout_secs: 60,
            max_pending_udp: 2048,
            udp_queue_capacity: 256,
            udp_recv_buffer: 2048,
            server_header: "RustRtspServer/1.0".to_string(),
            default_media: DefaultMediaInfo::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PullSessionConfig {
    pub max_pending_packets: usize,
    pub send_queue_capacity: usize,
    pub tcp_batch_size: usize,
    /// 合并写时间窗口，毫秒，0 表示关闭
    pub merge_window_ms: u64,
}

impl Default for PullSessionConfig {
    fn default() -> Self {
        Self {
            max_pending_packets: 1024,
            send_queue_capacity: 512,
            tcp_batch_size: 64,
            merge_window_ms: 2,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionConfig {
    pub rtsp: RtspSessionConfig,
    pub pull: PullSessionConfig,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            rtsp: RtspSessionConfig::default(),
            pull: PullSessionConfig::default(),
        }
    }
}

pub fn init(config_path: Option<&str>) -> Result<&'static AppConfig> {
    SETTINGS.get_or_try_init(|| load_config(config_path))
}

pub fn get() -> &'static AppConfig {
    SETTINGS.get_or_init(AppConfig::default)
}

fn load_config(config_path: Option<&str>) -> Result<AppConfig> {
    let mut builder = ::config::Config::builder()
        .add_source(::config::Environment::with_prefix("DM_LIVE").separator("__"));

    if let Some(path) = config_path {
        builder = builder.add_source(::config::File::with_name(path).required(false));
    } else {
        builder = builder.add_source(::config::File::with_name("config").required(false));
    }

    let cfg = builder.build()?;
    let settings: AppConfig = cfg.try_deserialize().unwrap_or_default();
    Ok(settings.normalize())
}
