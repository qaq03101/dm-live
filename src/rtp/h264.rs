/// H.264 NAL单元类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum H264NalType {
    Unspecified = 0,
    NonIdrSlice = 1,
    DataPartitionA = 2,
    DataPartitionB = 3,
    DataPartitionC = 4,
    IdrSlice = 5,
    Sei = 6,
    Sps = 7,
    Pps = 8,
    AccessUnitDelimiter = 9,
    EndOfSequence = 10,
    EndOfStream = 11,
    FillerData = 12,
    SpsExtension = 13,
    Prefix = 14,
    SubsetSps = 15,
    Reserved16 = 16,
    Reserved17 = 17,
    Reserved18 = 18,
    AuxiliarySlice = 19,
    Extension = 20,
    DepthExtension = 21,
    Reserved22 = 22,
    Reserved23 = 23,
    StapA = 24,
    StapB = 25,
    Mtap16 = 26,
    Mtap24 = 27,
    FuA = 28,
    FuB = 29,
    Unknown = 0xFF,
}

impl From<u8> for H264NalType {
    fn from(value: u8) -> Self {
        match value & 0x1F {
            0 => Self::Unspecified,
            1 => Self::NonIdrSlice,
            2 => Self::DataPartitionA,
            3 => Self::DataPartitionB,
            4 => Self::DataPartitionC,
            5 => Self::IdrSlice,
            6 => Self::Sei,
            7 => Self::Sps,
            8 => Self::Pps,
            9 => Self::AccessUnitDelimiter,
            10 => Self::EndOfSequence,
            11 => Self::EndOfStream,
            12 => Self::FillerData,
            13 => Self::SpsExtension,
            14 => Self::Prefix,
            15 => Self::SubsetSps,
            16 => Self::Reserved16,
            17 => Self::Reserved17,
            18 => Self::Reserved18,
            19 => Self::AuxiliarySlice,
            20 => Self::Extension,
            21 => Self::DepthExtension,
            22 => Self::Reserved22,
            23 => Self::Reserved23,
            24 => Self::StapA,
            25 => Self::StapB,
            26 => Self::Mtap16,
            27 => Self::Mtap24,
            28 => Self::FuA,
            29 => Self::FuB,
            _ => Self::Unknown,
        }
    }
}

impl H264NalType {
    /// 是否是关键帧类型
    pub fn is_key_frame(&self) -> bool {
        matches!(self, Self::IdrSlice | Self::Sps | Self::Pps)
    }
}

/// 解析H.264 NAL单元类型
pub fn parse_h264_nal_type(payload: &[u8]) -> Option<H264NalType> {
    if payload.is_empty() {
        return None;
    }

    let nal_header = payload[0];
    let nal_type = H264NalType::from(nal_header);

    // 处理FU-A分片
    if nal_type == H264NalType::FuA && payload.len() >= 2 {
        let fu_header = payload[1];
        let start = (fu_header & 0x80) != 0;
        if start {
            // FU-A的首个分片包含实际NAL类型
            let actual_nal = H264NalType::from(fu_header);
            return Some(actual_nal);
        }
    }

    Some(nal_type)
}
