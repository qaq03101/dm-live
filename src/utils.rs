use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn parse_transport(transport: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();

    for param in transport.split(';') {
        let param = param.trim();
        if let Some(pos) = param.find('=') {
            let key = param[..pos].trim().to_lowercase();
            let value = param[pos + 1..].trim().to_string();
            params.insert(key, value);
        } else {
            params.insert(param.to_lowercase(), String::new());
        }
    }

    params
}

pub fn generate_session_id() -> String {
    uuid::Uuid::new_v4().to_string().replace('-', "")[..12].to_string()
}
