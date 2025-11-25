#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum AvCodec {
    H264 = 0,
    H265 = 1,
    Aac = 2,
    Unknown = 255,
}

impl From<u8> for AvCodec {
    fn from(value: u8) -> Self {
        match value {
            0 => AvCodec::H264,
            1 => AvCodec::H265,
            2 => AvCodec::Aac,
            _ => AvCodec::Unknown,
        }
    }
}

impl From<AvCodec> for u8 {
    fn from(codec: AvCodec) -> Self {
        codec as u8
    }
}
