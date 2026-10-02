//! Retention policy limits for maximum age, record counts, and storage bytes.

use std::time::Duration;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::DlqError;
use crate::message::DeadLetter;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub max_age: Option<Duration>,
    pub max_messages: Option<usize>,
    pub max_bytes: Option<usize>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age: Some(Duration::from_secs(14 * 86400)),
            max_messages: Some(100_000),
            max_bytes: Some(500 * 1024 * 1024),
        }
    }
}

impl RetentionPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_age(mut self, age: Duration) -> Self {
        self.max_age = Some(age);
        self
    }

    pub fn with_max_messages(mut self, max: usize) -> Self {
        self.max_messages = Some(max);
        self
    }

    pub fn with_max_bytes(mut self, bytes: usize) -> Self {
        self.max_bytes = Some(bytes);
        self
    }

    pub fn validate(&self) -> Result<(), DlqError> {
        if let Some(age) = self.max_age {
            if age.as_secs() == 0 {
                return Err(DlqError::InvalidConfiguration("max_age cannot be zero".into()));
            }
        }
        if let Some(max_msg) = self.max_messages {
            if max_msg == 0 {
                return Err(DlqError::InvalidConfiguration("max_messages cannot be zero".into()));
            }
        }
        if let Some(bytes) = self.max_bytes {
            if bytes == 0 {
                return Err(DlqError::InvalidConfiguration("max_bytes cannot be zero".into()));
            }
        }
        Ok(())
    }

    pub fn is_expired(&self, record: &DeadLetter, now: DateTime<Utc>) -> bool {
        if let Some(max_age) = self.max_age {
            let age_chrono = match chrono::Duration::from_std(max_age) {
                Ok(dur) => dur,
                Err(_) => return false,
            };
            let reference_time = record.timestamps.dead_lettered_at.unwrap_or(record.timestamps.failed_at);
            if now - reference_time > age_chrono {
                return true;
            }
        }
        false
    }
}
