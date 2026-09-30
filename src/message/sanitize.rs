//! Utilities for redacting sensitive headers and safely summarizing payload bytes.

use std::collections::HashMap;

const SENSITIVE_KEYWORDS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "api-key",
    "apikey",
    "x-api-key",
    "token",
    "secret",
    "password",
    "bearer",
    "credential",
    "session",
    "private-key",
];

pub fn is_sensitive_header(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    SENSITIVE_KEYWORDS.iter().any(|&keyword| lower.contains(keyword))
}

pub fn sanitize_headers(headers: &HashMap<String, String>) -> HashMap<String, String> {
    headers
        .iter()
        .map(|(k, v)| {
            if is_sensitive_header(k) {
                (k.clone(), "[REDACTED]".to_string())
            } else {
                (k.clone(), v.clone())
            }
        })
        .collect()
}

pub fn redact_sensitive_headers(headers: &mut HashMap<String, String>) {
    for (k, v) in headers.iter_mut() {
        if is_sensitive_header(k) {
            *v = "[REDACTED]".to_string();
        }
    }
}

pub fn safe_payload_summary(payload: &[u8], max_bytes: usize) -> String {
    let len = payload.len();
    if len == 0 {
        return "empty(0 bytes)".to_string();
    }

    let preview_len = len.min(max_bytes);
    match std::str::from_utf8(&payload[..preview_len]) {
        Ok(s) => {
            if len > max_bytes {
                format!("{}... (truncated, total {} bytes)", s, len)
            } else {
                format!("{} (total {} bytes)", s, len)
            }
        }
        Err(_) => format!("<binary data, total {} bytes>", len),
    }
}
