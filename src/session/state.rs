/// 统一的 RTSP 错误码枚举（控制面使用）
#[derive(Debug, Clone, Copy)]
pub enum RtspError {
    MissingCSeq,
    InvalidCSeq,
    MissingTransport,
    InvalidTrack,
    InvalidUri,
    InvalidSdpOrTransport,
    MissingInterleaved,
    MissingClientPort,
    InvalidClientPort,
    SessionNotFound,
    MethodNotImplemented,
    StreamNotFound,
    SdpNotReady,
    MissingSdp,
    PushSessionNotInitialized,
    InternalServerError,
}

pub type RtspResult<T> = Result<T, RtspError>;

impl RtspError {
    pub fn into_response(self) -> (u16, &'static str) {
        match self {
            RtspError::MissingCSeq => (400, "Bad Request - Missing CSeq"),
            RtspError::InvalidCSeq => (400, "Bad Request - Invalid CSeq"),
            RtspError::MissingTransport => (400, "Bad Request - Missing Transport"),
            RtspError::InvalidTrack => (400, "Bad Request - Invalid Track"),
            RtspError::InvalidUri => (400, "Bad Request - Invalid URI"),
            RtspError::InvalidSdpOrTransport => (400, "Bad Request - Invalid SDP/Transport"),
            RtspError::MissingInterleaved => (400, "Bad Request - Missing interleaved channel"),
            RtspError::MissingClientPort => (400, "Bad Request - Missing client_port"),
            RtspError::InvalidClientPort => (400, "Bad Request - Invalid client_port"),
            RtspError::SessionNotFound => (454, "Session Not Found"),
            RtspError::MethodNotImplemented => (501, "Not Implemented"),
            RtspError::StreamNotFound => (404, "Stream Not Found"),
            RtspError::SdpNotReady => (503, "SDP Not Ready"),
            RtspError::MissingSdp => (400, "Bad Request - Missing SDP"),
            RtspError::PushSessionNotInitialized => (454, "Session Not Ready"),
            RtspError::InternalServerError => (500, "Internal Server Error"),
        }
    }
}

#[derive(Debug)]
pub enum FlushError {
    Io(std::io::Error),
    BrokenPipe,
}
