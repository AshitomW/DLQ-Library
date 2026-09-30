//! Identifiers and generic message representation for application payloads.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct MessageId(String);

impl MessageId {
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for MessageId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for MessageId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl FromStr for MessageId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub id: MessageId,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub content_type: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub ordering_key: Option<String>,
}

impl Message {
    pub fn new(id: impl Into<MessageId>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            id: id.into(),
            payload: payload.into(),
            headers: HashMap::new(),
            content_type: None,
            correlation_id: None,
            causation_id: None,
            ordering_key: None,
        }
    }

    pub fn from_string(id: impl Into<MessageId>, text: String) -> Self {
        let mut msg = Self::new(id, text.into_bytes());
        msg.content_type = Some("text/plain; charset=utf-8".to_string());
        msg
    }

    pub fn with_header(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.headers.insert(key.into(), val.into());
        self
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    pub fn with_causation_id(mut self, id: impl Into<String>) -> Self {
        self.causation_id = Some(id.into());
        self
    }

    pub fn with_ordering_key(mut self, key: impl Into<String>) -> Self {
        self.ordering_key = Some(key.into());
        self
    }

    pub fn payload_as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.payload)
    }
}
