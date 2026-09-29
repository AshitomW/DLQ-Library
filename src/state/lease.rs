//! Worker visibility leases and concurrency token management.

use std::time::Duration;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub worker_id: String,
    pub granted_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub version: u64,
}

impl Lease {
    pub fn new(worker_id: impl Into<String>, duration: Duration, version: u64) -> Self {
        let now = Utc::now();
        let expires_at = now + chrono::Duration::from_std(duration).unwrap_or_else(|_| chrono::Duration::seconds(30));
        Self {
            worker_id: worker_id.into(),
            granted_at: now,
            expires_at,
            version,
        }
    }

    pub fn is_valid_at(&self, now: DateTime<Utc>) -> bool {
        self.expires_at > now
    }

    pub fn extend(&mut self, extension: Duration) {
        let ext = chrono::Duration::from_std(extension).unwrap_or_else(|_| chrono::Duration::seconds(30));
        self.expires_at = self.expires_at + ext;
        self.version += 1;
    }
}
