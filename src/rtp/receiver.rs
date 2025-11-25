use anyhow::{Context, Result};
use bytes::Bytes;
use std::collections::HashMap;
use tokio::time::Instant;
use tracing::instrument;

use crate::rtp::{PacketSorter, RtpHeader, RtpPacket};
use crate::rtsp::{SdpTrack, TrackType};

/// 单个RTP Track的状态
#[derive(Debug, Clone)]
pub struct RtpTrack {
    /// SSRC
    pub ssrc: u32,
    /// 负载类型
    pub payload_type: u8,
    /// 采样率
    pub sample_rate: u32,
    /// Track类型
    pub track_type: TrackType,
    /// 序列号
    seq: u16,
    /// 时间戳
    timestamp: u32,
}

impl RtpTrack {
    pub fn new(payload_type: u8, sample_rate: u32, track_type: TrackType) -> Self {
        Self {
            ssrc: 0,
            payload_type,
            sample_rate,
            track_type,
            seq: 0,
            timestamp: 0,
        }
    }

    /// 将收到的RTP数据解析为RtpPacket
    #[instrument(
        level = "trace",
        name = "rtp_process",
        skip(self, data),
        fields(track = %track_idx, len = data.len())
    )]
    pub fn process_rtp(
        &mut self,
        data: Bytes,
        track_idx: String,
        received_at: Instant,
    ) -> Result<RtpPacket> {
        if data.len() < RtpHeader::SIZE {
            anyhow::bail!("RTP packet too short: {} bytes", data.len());
        }

        let header = RtpHeader::parse(data.as_ref()).context("Failed to parse RTP header")?;

        if self.payload_type != 0xFF && header.payload_type != self.payload_type {
            anyhow::bail!(
                "Payload type mismatch: expected {}, got {}",
                self.payload_type,
                header.payload_type
            );
        }

        // SSRC处理
        if self.ssrc == 0 {
            self.ssrc = header.ssrc;
            tracing::info!(
                "SSRC set to {:08X} for {:?} track",
                header.ssrc,
                self.track_type
            );
        } else if self.ssrc != header.ssrc {
            tracing::warn!("SSRC changed: {:08X} -> {:08X}", self.ssrc, header.ssrc);
            self.ssrc = header.ssrc;
        }

        // 计算payload范围
        let payload_start = RtpHeader::SIZE
            + self.get_csrc_size(&header)
            + self.get_extension_size(&header, data.as_ref())?;
        let payload_end = data.len() - self.get_padding_size(&header, data.as_ref())?;

        if payload_start >= payload_end {
            anyhow::bail!(
                "Invalid payload size: start={}, end={}",
                payload_start,
                payload_end
            );
        }

        let payload = data.slice(payload_start..payload_end);

        self.seq = header.sequence;
        self.timestamp = header.timestamp;

        let mut packet = RtpPacket::new(
            data,
            header,
            payload,
            self.track_type,
            track_idx,
            received_at,
        );
        packet.sample_rate = self.sample_rate;

        Ok(packet)
    }

    fn get_csrc_size(&self, header: &RtpHeader) -> usize {
        header.csrc_count as usize * 4
    }

    fn get_extension_size(&self, header: &RtpHeader, data: &[u8]) -> Result<usize> {
        if !header.extension {
            return Ok(0);
        }

        let csrc_size = self.get_csrc_size(header);
        let ext_offset = RtpHeader::SIZE + csrc_size;

        if data.len() < ext_offset + 4 {
            anyhow::bail!("Extension header too short");
        }

        let ext_len = u16::from_be_bytes([data[ext_offset + 2], data[ext_offset + 3]]) as usize;
        Ok(4 + ext_len * 4)
    }

    fn get_padding_size(&self, header: &RtpHeader, data: &[u8]) -> Result<usize> {
        if !header.padding {
            return Ok(0);
        }

        if data.is_empty() {
            anyhow::bail!("Cannot get padding from empty packet");
        }

        Ok(data[data.len() - 1] as usize)
    }
}

/// RTP接收器
pub struct RtpReceiver {
    /// Track列表
    tracks: HashMap<String, RtpTrack>,
    /// 每个Track对应的PacketSorter
    sorters: HashMap<String, PacketSorter<RtpPacket>>,
}

impl RtpReceiver {
    /// 创建RtpReceiver
    ///
    /// # Arguments
    /// - sdp_tracks: SDP中的track信息
    pub fn new(sdp_tracks: &[SdpTrack]) -> Self {
        let mut tracks = HashMap::new();
        let mut sorters = HashMap::new();

        for sdp_track in sdp_tracks {
            let track_type = sdp_track.track_type;
            let payload_type = sdp_track.payload_type;
            let sample_rate = sdp_track.sample_rate;

            let track = RtpTrack::new(payload_type, sample_rate, track_type);
            let sorter = PacketSorter::new();

            let track_id = sdp_track.control_url.clone();

            sorters.insert(track_id.clone(), sorter);
            tracks.insert(track_id, track);
        }

        Self {
            tracks: tracks,
            sorters: sorters,
        }
    }

    /// 输入RTP数据后, 返回排序后的RTP包列表
    #[instrument(
        level = "trace",
        name = "rtp_input",
        skip(self, data),
        fields(track = %track_idx, size = data.len())
    )]
    pub fn input_rtp(&mut self, track_idx: String, data: Bytes) -> Result<Vec<RtpPacket>> {
        let received_at = Instant::now();

        let packet = {
            let track = self
                .tracks
                .get_mut(&track_idx)
                .with_context(|| format!("Track {} not found", track_idx))?;
            track
                .process_rtp(data, track_idx.clone(), received_at)
                .with_context(|| format!("Failed to process RTP for track {}", track_idx))?
        };

        let mut output = Vec::new();
        {
            let sorter = self
                .sorters
                .get_mut(&track_idx)
                .with_context(|| format!("Sorter for track {} not found", track_idx))?;
            sorter.sort_packet_collect(packet.header.sequence, packet, &mut output);
        }

        Ok(output)
    }
}
