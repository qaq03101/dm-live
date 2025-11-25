use anyhow::Result;
use nom::{
    IResult,
    bytes::complete::{take_until, take_while1},
    character::complete::{char, digit1, line_ending, not_line_ending, space0, space1},
    combinator::map_res,
    sequence::tuple,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RtspRequest {
    pub method: String,
    pub uri: String,
    pub version: String,
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RtspResponse {
    pub version: String,
    pub status_code: u16,
    pub reason_phrase: String,
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
}

/// RTSP消息解析器
pub struct RtspParser;

impl RtspParser {
    /// 解析RTSP请求消息
    ///
    /// # Arguments
    ///
    /// * `input` - 输入的RTSP请求消息字符串
    ///
    /// # Returns
    /// * `Result<(RtspRequest, Option<String>)>` - 解析成功返回包含RtspRequest和剩余未解析字符串的Option, 失败返回错误信息
    pub fn parse_message(input: &str) -> Result<(RtspRequest, Option<String>)> {
        match Self::parse_request(input) {
            Ok((remaining, request)) => {
                let remainder = if remaining.is_empty() {
                    None
                } else {
                    Some(remaining.to_string())
                };
                Ok((request, remainder))
            }
            Err(e) => Err(anyhow::anyhow!("Failed to parse RTSP request: {}", e)),
        }
    }

    fn parse_request(input: &str) -> IResult<&str, RtspRequest> {
        let (input, (method, _, uri, _, version, _)) = tuple((
            take_while1(|c: char| c.is_alphabetic()),
            space1,
            take_while1(|c: char| !c.is_whitespace()),
            space1,
            take_while1(|c: char| !c.is_whitespace()),
            line_ending,
        ))(input)?;

        let (input, headers) = Self::parse_headers(input)?;

        let (input, _) = line_ending(input)?;

        let body = if input.is_empty() {
            None
        } else {
            Some(input.to_string())
        };

        Ok((
            "",
            RtspRequest {
                method: method.to_string(),
                uri: uri.to_string(),
                version: version.to_string(),
                headers,
                body,
            },
        ))
    }

    pub fn parse_response(input: &str) -> Result<RtspResponse> {
        match Self::parse_response_internal(input) {
            Ok((_, response)) => Ok(response),
            Err(e) => anyhow::bail!("Failed to parse RTSP response: {}", e),
        }
    }

    fn parse_response_internal(input: &str) -> IResult<&str, RtspResponse> {
        let (input, (version, _, status_code, _, reason_phrase, _)) = tuple((
            take_while1(|c: char| !c.is_whitespace()),
            space1,
            map_res(digit1, str::parse::<u16>),
            space1,
            not_line_ending,
            line_ending,
        ))(input)?;

        let (input, headers) = Self::parse_headers(input)?;
        let (input, _) = line_ending(input)?;

        let body = if input.is_empty() {
            None
        } else {
            Some(input.to_string())
        };

        Ok((
            "",
            RtspResponse {
                version: version.to_string(),
                status_code,
                reason_phrase: reason_phrase.to_string(),
                headers,
                body,
            },
        ))
    }

    fn parse_headers(input: &str) -> IResult<&str, HashMap<String, String>> {
        let mut headers = HashMap::new();
        let mut remaining = input;

        loop {
            // 检查是否到达空行
            if remaining.starts_with("\r\n") || remaining.starts_with("\n") {
                break;
            }

            match Self::parse_header_line(remaining) {
                Ok((rest, (name, value))) => {
                    headers.insert(name.to_lowercase(), value.trim().to_string());
                    remaining = rest;
                }
                Err(_) => break,
            }
        }

        Ok((remaining, headers))
    }

    fn parse_header_line(input: &str) -> IResult<&str, (&str, &str)> {
        let (input, name) = take_until(":")(input)?;
        let (input, _) = char(':')(input)?;
        let (input, _) = space0(input)?;
        let (input, value) = not_line_ending(input)?;
        let (input, _) = line_ending(input)?;

        Ok((input, (name, value)))
    }

    pub fn build_request(
        method: &str,
        uri: &str,
        headers: &HashMap<String, String>,
        body: Option<&str>,
    ) -> String {
        let mut request = format!("{} {} RTSP/1.0\r\n", method, uri);

        for (name, value) in headers {
            request.push_str(&format!("{}: {}\r\n", name, value));
        }

        request.push_str("\r\n");

        if let Some(body) = body {
            request.push_str(body);
        }

        request
    }

    pub fn build_response(
        status_code: u16,
        reason_phrase: &str,
        headers: &HashMap<String, String>,
        body: Option<&str>,
    ) -> String {
        let mut response = format!("RTSP/1.0 {} {}\r\n", status_code, reason_phrase);

        for (name, value) in headers {
            response.push_str(&format!("{}: {}\r\n", name, value));
        }

        response.push_str("\r\n");

        if let Some(body) = body {
            response.push_str(body);
        }

        response
    }
}
