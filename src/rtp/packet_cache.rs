use crate::config;
use crate::rtp::{RtpPacket, parse_h264_nal_type};
use std::sync::Arc;

/// FrameData : 一帧的所有RTP包
#[derive(Clone)]
pub struct FrameData {
    /// 时间戳（毫秒）
    pub timestamp_ms: u64,
    /// 是否为关键帧
    pub is_key: bool,
    /// 包含的RTP包列表
    pub packets: Vec<Arc<RtpPacket>>,
}

/// RTP包缓存，用于组帧
pub struct PacketCache {
    /// 当前缓存的时间戳
    current_timestamp: Option<u64>,
    /// 缓存的RTP包
    packets: Vec<Arc<RtpPacket>>,
    /// 每帧最大包数
    max_packets_per_frame: usize,
}

impl PacketCache {
    pub fn new() -> Self {
        let max_packets = config::get().media.packet_cache.max_packets_per_frame;
        Self::with_max_packets(max_packets)
    }

    pub fn with_max_packets(max_packets_per_frame: usize) -> Self {
        Self {
            current_timestamp: None,
            packets: Vec::new(),
            max_packets_per_frame,
        }
    }

    /// 推入RTP包并尝试flush
    ///
    /// # Arguments
    /// * `packet`: RTP包
    ///
    /// # Returns
    /// * `Option<FrameData>`: 如果触发flush则返回FrameData，否则返回None
    pub fn push(&mut self, packet: Arc<RtpPacket>) -> Option<FrameData> {
        let mut pkt_timestamp_ms = packet.timestamp_ms(false);
        if pkt_timestamp_ms == 0 {
            pkt_timestamp_ms = packet.header.timestamp as u64;
        }

        // 检查是否需要flush
        let need_flush = if let Some(cur_ts) = self.current_timestamp {
            // 时间戳变化或包数超限
            cur_ts != pkt_timestamp_ms || self.packets.len() >= self.max_packets_per_frame
        } else {
            false
        };

        let mut result = None;
        if need_flush {
            result = self.flush();
        }

        // 更新当前时间戳
        if self.current_timestamp.is_none() {
            self.current_timestamp = Some(pkt_timestamp_ms);
        }

        // 检测关键帧
        let is_key = Self::is_key_frame(&packet);
        if is_key {
            // 遇到关键帧,先flush当前缓存
            if !self.packets.is_empty() {
                result = self.flush();
            }
            self.current_timestamp = Some(pkt_timestamp_ms);
        }

        self.packets.push(packet);
        result
    }

    /// Flush当前缓存的包，返回FrameData
    pub fn flush(&mut self) -> Option<FrameData> {
        if self.packets.is_empty() {
            return None;
        }

        let timestamp_ms = self.current_timestamp.unwrap_or(0);
        let is_key = self.packets.iter().any(|pkt| Self::is_key_frame(pkt));

        let frame = FrameData {
            timestamp_ms,
            is_key,
            packets: std::mem::take(&mut self.packets),
        };

        self.current_timestamp = None;
        Some(frame)
    }

    /// 检测是否为H.264关键帧
    fn is_key_frame(packet: &RtpPacket) -> bool {
        if let Some(nal_type) = parse_h264_nal_type(&packet.payload) {
            nal_type.is_key_frame()
        } else {
            false
        }
    }
}

impl Default for PacketCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtp::RtpHeader;
    use crate::rtsp::TrackType;
    use bytes::Bytes;
    use tokio::time::Instant;

    fn build_packet(seq: u16, timestamp_ms: u32, marker: bool, is_key: bool) -> Arc<RtpPacket> {
        let nal = if is_key { 0x65 } else { 0x41 };
        let header = RtpHeader {
            version: 2,
            padding: false,
            extension: false,
            csrc_count: 0,
            marker,
            payload_type: 96,
            sequence: seq,
            timestamp: timestamp_ms,
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
            TrackType::Video,
            "video".to_string(),
            Instant::now(),
        ))
    }

    #[test]
    fn single_packet_flushes_immediately() {
        let mut cache = PacketCache::with_max_packets(10);
        let packet = build_packet(1, 1000, true, true);

        cache.push(packet);
        let frame = cache.flush().expect("frame should flush");
        assert_eq!(frame.packets.len(), 1);
        assert!(frame.is_key);
        assert_eq!(frame.timestamp_ms, 1000);
    }

    #[test]
    fn fragmented_frame_flushes_on_timestamp_change() {
        let mut cache = PacketCache::with_max_packets(10);
        let ts = 1000;

        let pkt1 = build_packet(1, ts, false, true);
        let pkt2 = build_packet(2, ts, false, false);
        let pkt3 = build_packet(3, ts, true, false);

        assert!(cache.push(pkt1).is_none());
        assert!(cache.push(pkt2).is_none());
        assert!(cache.push(pkt3).is_none());

        let next = build_packet(4, ts + 1000, true, false);
        let frame = cache.push(next).expect("timestamp change should flush");
        assert_eq!(frame.packets.len(), 3);
        assert_eq!(frame.timestamp_ms, ts as u64);
    }

    #[test]
    fn detects_key_frame_flag() {
        let mut cache = PacketCache::with_max_packets(5);
        let idr = build_packet(1, 2000, true, true);
        cache.push(idr);
        let frame = cache.flush().expect("key frame flushes");
        assert!(frame.is_key);
    }

    #[test]
    fn flushes_when_packet_limit_exceeded() {
        let mut cache = PacketCache::with_max_packets(10);
        let ts = 3000;
        let mut flushed = None;

        for seq in 0..=10 {
            let pkt = build_packet(seq, ts, false, false);
            let result = cache.push(pkt);
            if seq < 10 {
                assert!(result.is_none());
            } else {
                flushed = result;
            }
        }

        let frame = flushed.expect("exceeding capacity should flush");
        assert_eq!(frame.packets.len(), 10);
        assert!(!frame.is_key);
        assert_eq!(frame.timestamp_ms, ts as u64);
    }

    #[test]
    fn key_frame_forces_previous_flush() {
        let mut cache = PacketCache::with_max_packets(10);
        let first = build_packet(1, 1000, true, false);
        assert!(cache.push(first).is_none());

        let idr = build_packet(2, 2000, true, true);
        let flushed = cache.push(idr).expect("key frame should flush old frame");
        assert!(!flushed.is_key);
        assert_eq!(flushed.timestamp_ms, 1000);
    }
}
