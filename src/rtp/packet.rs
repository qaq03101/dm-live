use anyhow::Result;
use bytes::Bytes;
use tokio::time::Instant;

use crate::rtsp::sdp::TrackType;

/// RTP传输类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtpType {
    Invalid = -1,
    Tcp = 0,
    Udp = 1,
    Multicast = 2,
}

/// RTP包头
#[derive(Debug, Clone)]
pub struct RtpHeader {
    pub version: u8,
    pub padding: bool,
    pub extension: bool,
    pub csrc_count: u8,
    pub marker: bool,
    pub payload_type: u8,
    pub sequence: u16,
    pub timestamp: u32,
    pub ssrc: u32,
}

impl RtpHeader {
    pub const SIZE: usize = 12;

    /// 解析RTP包头
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < Self::SIZE {
            anyhow::bail!("RTP header too short: {} bytes", data.len());
        }

        let first_byte = data[0];
        let version = (first_byte >> 6) & 0x03;
        if version != 2 {
            anyhow::bail!("Invalid RTP version: {}", version);
        }

        let padding = (first_byte & 0x20) != 0;
        let extension = (first_byte & 0x10) != 0;
        let csrc_count = first_byte & 0x0F;

        let second_byte = data[1];
        let marker = (second_byte & 0x80) != 0;
        let payload_type = second_byte & 0x7F;

        let sequence = u16::from_be_bytes([data[2], data[3]]);
        let timestamp = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        let ssrc = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);

        Ok(RtpHeader {
            version,
            padding,
            extension,
            csrc_count,
            marker,
            payload_type,
            sequence,
            timestamp,
            ssrc,
        })
    }

    /// 序列化RTP包头
    pub fn serialize(&self) -> [u8; Self::SIZE] {
        let mut data = [0u8; Self::SIZE];

        data[0] = (self.version << 6)
            | (if self.padding { 0x20 } else { 0 })
            | (if self.extension { 0x10 } else { 0 })
            | (self.csrc_count & 0x0F);

        data[1] = (if self.marker { 0x80 } else { 0 }) | (self.payload_type & 0x7F);

        data[2..4].copy_from_slice(&self.sequence.to_be_bytes());
        data[4..8].copy_from_slice(&self.timestamp.to_be_bytes());
        data[8..12].copy_from_slice(&self.ssrc.to_be_bytes());

        data
    }
}

/// RTP包
#[derive(Debug)]
pub struct RtpPacket {
    /// RTP包头
    pub header: RtpHeader,
    /// RTP包负载
    pub payload: Bytes,
    /// 媒体类型
    pub track_type: TrackType,
    /// 采样率
    pub sample_rate: u32,
    /// NTP时间戳
    pub ntp_stamp: u64,
    /// 轨道索引
    pub track_index: String,
    /// 被服务器接收的时间
    pub received_at: Instant,
    /// 原始RTP包数据
    raw_packet: Bytes,
}

impl RtpPacket {
    pub fn new(
        raw_packet: Bytes,
        header: RtpHeader,
        payload: Bytes,
        track_type: TrackType,
        track_idx: String,
        received_at: Instant,
    ) -> Self {
        Self {
            header,
            payload,
            track_type,
            sample_rate: 0,
            ntp_stamp: 0,
            track_index: track_idx,
            received_at,
            raw_packet,
        }
    }

    pub fn size(&self) -> usize {
        self.raw_packet.len()
    }

    /// 获取时间戳（毫秒）
    ///
    /// # Arguments
    /// * `use_ntp`: 是否优先使用NTP时间戳
    ///
    /// # Returns
    /// * `u64`: 时间戳（毫秒）, 不使用NTP时返回基于RTP包头时间戳计算的值
    pub fn timestamp_ms(&self, use_ntp: bool) -> u64 {
        if use_ntp && self.ntp_stamp > 0 {
            return self.ntp_stamp;
        }

        if self.sample_rate == 0 {
            return 0;
        }

        (self.header.timestamp as u64 * 1000) / self.sample_rate as u64
    }

    pub fn rtp_bytes(&self) -> Bytes {
        self.raw_packet.clone()
    }

    /// 获取原始RTP数据的只读引用，避免额外clone
    pub fn raw_packet(&self) -> &Bytes {
        &self.raw_packet
    }
}

impl Clone for RtpPacket {
    fn clone(&self) -> Self {
        Self {
            header: self.header.clone(),
            payload: self.payload.clone(),
            track_type: self.track_type,
            sample_rate: self.sample_rate,
            ntp_stamp: self.ntp_stamp,
            track_index: self.track_index.clone(),
            received_at: self.received_at,
            raw_packet: self.raw_packet.clone(),
        }
    }
}
