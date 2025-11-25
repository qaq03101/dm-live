use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TrackType {
    Invalid = -1,
    Video = 0,
    Audio = 1,
    Max = 2,
}

impl fmt::Display for TrackType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrackType::Video => write!(f, "video"),
            TrackType::Audio => write!(f, "audio"),
            TrackType::Invalid => write!(f, "invalid"),
            TrackType::Max => write!(f, "max"),
        }
    }
}

/// 存储SDP解析后的信息
#[derive(Debug, Clone)]
pub struct SdpTrack {
    pub track_type: TrackType,
    pub payload_type: u8,
    pub codec: String,
    pub sample_rate: u32,
    pub channels: u16,
    pub control_url: String,
    pub fmtp: Option<String>,
    pub rtpmap: Option<String>,

    // 运行时状态
    pub ssrc: u32,
    pub sequence: u16,
    pub timestamp: u32,
    pub interleaved: u8,
    pub initialized: bool,
}

impl SdpTrack {
    pub fn new(track_type: TrackType) -> Self {
        Self {
            track_type,
            payload_type: 0,
            codec: String::new(),
            sample_rate: 0,
            channels: 0,
            control_url: String::new(),
            fmtp: None,
            rtpmap: None,
            ssrc: 0,
            sequence: 0,
            timestamp: 0,
            interleaved: track_type as u8 * 2,
            initialized: false,
        }
    }

    pub fn get_control_url(&self, base_url: &str) -> String {
        if self.control_url.starts_with("rtsp://") {
            self.control_url.clone()
        } else {
            format!(
                "{}/{}",
                base_url.trim_end_matches('/'),
                self.control_url.trim_start_matches('/')
            )
        }
    }
}

#[derive(Debug, Clone)]
pub struct SdpOrigin {
    pub username: String,
    pub session_id: String,
    pub session_version: String,
    pub network_type: String,
    pub address_type: String,
    pub unicast_address: String,
}

#[derive(Debug, Clone)]
pub struct SdpConnection {
    pub network_type: String,
    pub address_type: String,
    pub connection_address: String,
}

#[derive(Debug, Clone)]
pub struct SdpTiming {
    pub start_time: u64,
    pub stop_time: u64,
}

#[derive(Debug, Clone)]
pub struct SdpMedia {
    pub media_type: String,
    pub port: u16,
    pub port_count: Option<u16>,
    pub protocol: String,
    pub formats: Vec<String>,
    pub connection: Option<SdpConnection>,
    pub attributes: HashMap<String, String>,
    pub bandwidth: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SdpSession {
    pub version: u8,
    pub origin: SdpOrigin,
    pub session_name: String,
    pub connection: Option<SdpConnection>,
    pub timing: SdpTiming,
    pub attributes: HashMap<String, String>,
    pub media_descriptions: Vec<SdpMedia>,
}

pub struct SdpParser;

impl SdpParser {
    pub fn parse(sdp: &str) -> Result<SdpSession> {
        let mut session = SdpSession {
            version: 0,
            origin: SdpOrigin {
                username: String::new(),
                session_id: String::new(),
                session_version: String::new(),
                network_type: String::new(),
                address_type: String::new(),
                unicast_address: String::new(),
            },
            session_name: String::new(),
            connection: None,
            timing: SdpTiming {
                start_time: 0,
                stop_time: 0,
            },
            attributes: HashMap::new(),
            media_descriptions: Vec::new(),
        };

        let mut current_media: Option<SdpMedia> = None;

        let lines: Vec<&str> = sdp.lines().collect();

        for line in lines {
            let line = line.trim();
            if line.is_empty() || line.len() < 2 {
                continue;
            }

            let type_char = line.chars().nth(0).unwrap();

            if line.chars().nth(1) != Some('=') {
                continue;
            }

            let value = &line[2..];

            match type_char {
                'v' => {
                    session.version = value
                        .parse()
                        .with_context(|| format!("Invalid version: {}", value))?;
                }
                'o' => {
                    session.origin =
                        Self::parse_origin(value).with_context(|| "Failed to parse origin line")?;
                }
                's' => session.session_name = value.to_string(),
                'c' => {
                    let connection = Self::parse_connection(value)
                        .with_context(|| "Failed to parse connection line")?;
                    if current_media.is_some() {
                        current_media.as_mut().unwrap().connection = Some(connection);
                    } else {
                        session.connection = Some(connection);
                    }
                }
                't' => {
                    session.timing =
                        Self::parse_timing(value).with_context(|| "Failed to parse timing line")?;
                }
                'a' => {
                    let (attr_name, attr_value) = Self::parse_attribute(value);
                    if let Some(ref mut media) = current_media {
                        media.attributes.insert(attr_name, attr_value);
                    } else {
                        session.attributes.insert(attr_name, attr_value);
                    }
                }
                'm' => {
                    if let Some(media) = current_media.take() {
                        session.media_descriptions.push(media);
                    }
                    current_media = Some(
                        Self::parse_media(value).with_context(|| "Failed to parse media line")?,
                    );
                }
                'b' => {
                    if let Some(ref mut media) = current_media {
                        media.bandwidth = Some(value.to_string());
                    }
                }
                _ => {
                    tracing::debug!("Unknown SDP line type: {}", type_char);
                }
            }
        }

        if let Some(media) = current_media {
            session.media_descriptions.push(media);
        }

        Ok(session)
    }

    fn parse_origin(value: &str) -> Result<SdpOrigin> {
        let parts: Vec<&str> = value.split_whitespace().collect();
        if parts.len() != 6 {
            anyhow::bail!(
                "Invalid origin line format, expected 6 parts, got {}",
                parts.len()
            );
        }

        Ok(SdpOrigin {
            username: parts[0].to_string(),
            session_id: parts[1].to_string(),
            session_version: parts[2].to_string(),
            network_type: parts[3].to_string(),
            address_type: parts[4].to_string(),
            unicast_address: parts[5].to_string(),
        })
    }

    fn parse_connection(value: &str) -> Result<SdpConnection> {
        let parts: Vec<&str> = value.split_whitespace().collect();
        if parts.len() != 3 {
            anyhow::bail!(
                "Invalid connection line format, expected 3 parts, got {}",
                parts.len()
            );
        }

        Ok(SdpConnection {
            network_type: parts[0].to_string(),
            address_type: parts[1].to_string(),
            connection_address: parts[2].to_string(),
        })
    }

    fn parse_timing(value: &str) -> Result<SdpTiming> {
        let parts: Vec<&str> = value.split_whitespace().collect();
        if parts.len() != 2 {
            anyhow::bail!(
                "Invalid timing line format, expected 2 parts, got {}",
                parts.len()
            );
        }

        let start_time = parts[0]
            .parse()
            .with_context(|| format!("Invalid start time: {}", parts[0]))?;
        let stop_time = parts[1]
            .parse()
            .with_context(|| format!("Invalid stop time: {}", parts[1]))?;

        Ok(SdpTiming {
            start_time,
            stop_time,
        })
    }

    fn parse_attribute(value: &str) -> (String, String) {
        if let Some(pos) = value.find(':') {
            (value[..pos].to_string(), value[pos + 1..].to_string())
        } else {
            (value.to_string(), String::new())
        }
    }

    fn parse_media(value: &str) -> Result<SdpMedia> {
        let parts: Vec<&str> = value.split_whitespace().collect();
        if parts.len() < 4 {
            anyhow::bail!(
                "Invalid media line format, expected at least 4 parts, got {}",
                parts.len()
            );
        }

        let media_type = parts[0].to_string();

        let port_info = parts[1];
        let (port, port_count) = if port_info.contains('/') {
            let port_parts: Vec<&str> = port_info.split('/').collect();
            let port = port_parts[0]
                .parse()
                .with_context(|| format!("Invalid port: {}", port_parts[0]))?;
            let count = if port_parts.len() > 1 {
                Some(
                    port_parts[1]
                        .parse()
                        .with_context(|| format!("Invalid port count: {}", port_parts[1]))?,
                )
            } else {
                None
            };
            (port, count)
        } else {
            let port = port_info
                .parse()
                .with_context(|| format!("Invalid port: {}", port_info))?;
            (port, None)
        };

        let protocol = parts[2].to_string();
        let formats: Vec<String> = parts[3..].iter().map(|s| s.to_string()).collect();

        Ok(SdpMedia {
            media_type,
            port,
            port_count,
            protocol,
            formats,
            connection: None,
            attributes: HashMap::new(),
            bandwidth: None,
        })
    }

    pub fn extract_tracks(session: &SdpSession) -> Vec<SdpTrack> {
        let mut tracks = Vec::new();

        for media in &session.media_descriptions {
            if let Some(track) = Self::media_to_track(media) {
                tracks.push(track);
            }
        }

        tracks
    }

    fn media_to_track(media: &SdpMedia) -> Option<SdpTrack> {
        let track_type = match media.media_type.as_str() {
            "video" => TrackType::Video,
            "audio" => TrackType::Audio,
            _ => return None,
        };

        if media.formats.is_empty() {
            return None;
        }

        let payload_type: u8 = match media.formats[0].parse() {
            Ok(v) => v,
            Err(_) => return None,
        };
        let mut track = SdpTrack::new(track_type);
        track.payload_type = payload_type;

        // 解析rtpmap属性
        if let Some(rtpmap) = media.attributes.get("rtpmap") {
            if let Some((codec, sample_rate, channels)) = Self::parse_rtpmap(rtpmap) {
                track.codec = codec;
                track.sample_rate = sample_rate;
                track.channels = channels;
                track.rtpmap = Some(rtpmap.clone());
            }
        }

        // 解析fmtp属性
        if let Some(fmtp) = media.attributes.get("fmtp") {
            track.fmtp = Some(fmtp.clone());
        }

        // 解析control属性
        if let Some(control) = media.attributes.get("control") {
            track.control_url = control.clone();
        }

        Some(track)
    }

    fn parse_rtpmap(rtpmap: &str) -> Option<(String, u32, u16)> {
        // 格式: "96 H264/90000" 或 "0 PCMU/8000/1"
        let parts: Vec<&str> = rtpmap.split_whitespace().collect();
        if parts.len() < 2 {
            return None;
        }

        let codec_info = parts[1];
        let codec_parts: Vec<&str> = codec_info.split('/').collect();

        if codec_parts.len() < 2 {
            return None;
        }

        let codec = codec_parts[0].to_string();
        let sample_rate = match codec_parts[1].parse() {
            Ok(v) => v,
            Err(_) => return None,
        };
        let channels = if codec_parts.len() > 2 {
            match codec_parts[2].parse() {
                Ok(v) => v,
                Err(_) => return None,
            }
        } else {
            1
        };

        Some((codec, sample_rate, channels))
    }

    pub fn generate_sdp(session_name: &str, origin_address: &str, tracks: &[SdpTrack]) -> String {
        let session_id = crate::utils::current_timestamp_ms();

        let mut sdp = format!(
            "v=0\r\n\
             o=- {} {} IN IP4 {}\r\n\
             s={}\r\n\
             c=IN IP4 {}\r\n\
             t=0 0\r\n",
            session_id, session_id, origin_address, session_name, origin_address
        );

        for (index, track) in tracks.iter().enumerate() {
            let media_type = match track.track_type {
                TrackType::Video => "video",
                TrackType::Audio => "audio",
                _ => continue,
            };

            sdp.push_str(&format!(
                "m={} 0 RTP/AVP {}\r\n",
                media_type, track.payload_type
            ));

            if let Some(ref rtpmap) = track.rtpmap {
                sdp.push_str(&format!("a=rtpmap:{}\r\n", rtpmap));
            }

            if let Some(ref fmtp) = track.fmtp {
                sdp.push_str(&format!("a=fmtp:{}\r\n", fmtp));
            }

            sdp.push_str(&format!("a=control:trackID={}\r\n", index));
        }

        sdp
    }
}
