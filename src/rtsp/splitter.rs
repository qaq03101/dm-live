use anyhow::{Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use tracing::debug;

use crate::rtsp::{RtspParser, RtspRequest, RtspResponse};

/// RTSP消息类型
#[derive(Debug, Clone)]
pub enum RtspMessage {
    /// RTSP请求
    Request(RtspRequest),
    /// RTSP响应
    Response(RtspResponse),
    /// RTP over TCP封装包
    Rtp { interleaved: u8, data: Bytes },
}

/// RTP包验证trait
pub trait RtpValidator: Send + Sync {
    /// 验证RTP包头部
    ///
    /// # Arguments
    /// * `data`: RTP包原始数据
    ///
    /// # Returns
    /// * `Ok(())`: 验证通过
    /// * `Err(e)`: 验证失败
    fn validate(&self, data: &[u8]) -> Result<()>;

    /// 验证SSRC
    fn validate_ssrc(&self, _ssrc: u32) -> Result<()> {
        Ok(())
    }

    /// 验证Payload Type
    fn validate_payload_type(&self, _pt: u8) -> Result<()> {
        Ok(())
    }

    /// 验证RTP版本
    fn validate_version(&self, _version: u8) -> Result<()> {
        Ok(())
    }
}

/// 默认不进行验证
#[derive(Default)]
pub struct NoopRtpValidator;

impl RtpValidator for NoopRtpValidator {
    fn validate(&self, _data: &[u8]) -> Result<()> {
        Ok(())
    }
}

/// RTSP协议分割器
pub struct RtspSplitter {
    /// 是否启用RTP over TCP解析
    enable_rtp: bool,
    #[allow(dead_code)]
    buffer: BytesMut,
    /// RTP包验证器
    rtp_validator: Box<dyn RtpValidator>,
}

impl RtspSplitter {
    /// 创建新的分割器实例
    pub fn new() -> Self {
        Self {
            enable_rtp: false,
            buffer: BytesMut::new(),
            rtp_validator: Box::new(NoopRtpValidator),
        }
    }

    /// 设置自定义RTP验证器
    pub fn with_validator(mut self, validator: Box<dyn RtpValidator>) -> Self {
        self.rtp_validator = validator;
        self
    }

    /// 启用/禁用RTP over TCP解析
    pub fn enable_rtp(&mut self, enable: bool) {
        self.enable_rtp = enable;
        debug!(
            "RTP parsing {}",
            if enable { "enabled" } else { "disabled" }
        );
    }

    /// 从 src 解码一条RTSP消息或RTP包
    pub fn decode_message(&mut self, src: &mut BytesMut) -> Result<Option<RtspMessage>> {
        if src.is_empty() {
            return Ok(None);
        }

        // 检查是否是RTP包
        if self.enable_rtp && src.len() >= 4 && src[0] == b'$' {
            return self.decode_rtp_packet(src);
        }

        // 查找RTSP消息的结尾
        if let Some(header_end) = self.find_header_end(src) {
            // 解析头部
            let header_data = &src[..header_end];
            let header_str =
                std::str::from_utf8(header_data).context("Invalid UTF-8 in RTSP header")?;

            // 提取Content-Length
            let content_length = self.extract_content_length(header_str)?;

            let total_length = header_end + 4 + content_length; // +4 for \r\n\r\n

            if src.len() >= total_length {
                // 完整的消息
                let message_data = src.split_to(total_length);
                let message_str =
                    std::str::from_utf8(&message_data).context("Invalid UTF-8 in RTSP message")?;

                // 尝试解析为请求或响应
                if message_str.starts_with("RTSP/") {
                    let response = RtspParser::parse_response(message_str)
                        .context("Failed to parse RTSP response")?;
                    return Ok(Some(RtspMessage::Response(response)));
                } else {
                    let (request, _) = RtspParser::parse_message(message_str)
                        .context("Failed to parse RTSP request")?;
                    return Ok(Some(RtspMessage::Request(request)));
                }
            } else {
                // 等待更多数据
                return Ok(None);
            }
        }

        // 没有找到完整的头部，等待更多数据
        Ok(None)
    }

    /// 解码RTP over TCP包
    ///
    /// # 格式
    /// - Byte 0: '$' (0x24)
    /// - Byte 1: channel id (interleaved)
    /// - Byte 2-3: length (big-endian u16)
    /// - Byte 4+: RTP packet data
    fn decode_rtp_packet(&mut self, src: &mut BytesMut) -> Result<Option<RtspMessage>> {
        if src.len() < 4 {
            return Ok(None);
        }

        let interleaved = src[1];
        let length = u16::from_be_bytes([src[2], src[3]]) as usize;
        let total_length = 4 + length;

        if src.len() >= total_length {
            let packet_data = src.split_to(total_length).freeze();
            let rtp_data = packet_data.slice(4..);

            self.rtp_validator.validate(&rtp_data)?;

            Ok(Some(RtspMessage::Rtp {
                interleaved,
                data: rtp_data,
            }))
        } else {
            Ok(None)
        }
    }

    /// 查找RTSP头部结束位置(\r\n\r\n)
    fn find_header_end(&self, data: &[u8]) -> Option<usize> {
        // 查找 \r\n\r\n
        for i in 0..data.len().saturating_sub(3) {
            if data[i] == b'\r'
                && data[i + 1] == b'\n'
                && data[i + 2] == b'\r'
                && data[i + 3] == b'\n'
            {
                return Some(i);
            }
        }
        None
    }

    /// 从RTSP头部提取Content-Length
    fn extract_content_length(&self, header: &str) -> Result<usize> {
        for line in header.lines() {
            if line.to_lowercase().starts_with("content-length:") {
                let value = line
                    .split(':')
                    .nth(1)
                    .context("Invalid Content-Length header format")?
                    .trim();
                return value
                    .parse()
                    .with_context(|| format!("Invalid Content-Length value: {}", value));
            }
        }
        Ok(0)
    }
}

impl Default for RtspSplitter {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for RtspSplitter {
    type Item = RtspMessage;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        self.decode_message(src)
    }
}

impl Encoder<RtspMessage> for RtspSplitter {
    type Error = anyhow::Error;

    fn encode(&mut self, item: RtspMessage, dst: &mut BytesMut) -> Result<()> {
        match item {
            RtspMessage::Request(req) => {
                let data = RtspParser::build_request(
                    &req.method,
                    &req.uri,
                    &req.headers,
                    req.body.as_deref(),
                );
                dst.put(data.as_bytes());
            }
            RtspMessage::Response(resp) => {
                let data = RtspParser::build_response(
                    resp.status_code,
                    &resp.reason_phrase,
                    &resp.headers,
                    resp.body.as_deref(),
                );
                dst.put(data.as_bytes());
            }
            RtspMessage::Rtp { interleaved, data } => {
                let l = data.len();
                dst.put_u8(b'$');
                dst.put_u8(interleaved);
                dst.put_u16(l as u16);
                dst.put(data);
            }
        }
        Ok(())
    }
}
