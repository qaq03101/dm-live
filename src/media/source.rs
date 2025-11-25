use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct MediaInfo {
    pub schema: String,
    pub host: String,
    pub port: u16,
    pub app: String,
    pub stream: String,
    pub full_url: String,
    pub protocol: String,
}

impl MediaInfo {
    pub fn short_url(&self) -> String {
        format!("{}/{}", self.app, self.stream)
    }

    pub fn parse(url_str: &str) -> Result<Self> {
        let url = url::Url::parse(url_str)
            .with_context(|| format!("Failed to parse URL: {}", url_str))?;

        let schema = url.scheme().to_string();
        let host = url.host_str().unwrap_or("localhost").to_string();
        let port = url
            .port()
            .unwrap_or(if schema == "rtsp" { 554 } else { 8554 });

        let path_segments: Vec<&str> = url.path().trim_start_matches('/').split('/').collect();
        let (app, stream) = match path_segments.len() {
            0 => ("live".to_string(), "test".to_string()),
            1 => ("live".to_string(), path_segments[0].to_string()),
            _ => (path_segments[0].to_string(), path_segments[1].to_string()),
        };

        Ok(MediaInfo {
            schema,
            host,
            port,
            app,
            stream,
            full_url: url.to_string(),
            protocol: "rtsp".to_string(),
        })
    }
}
