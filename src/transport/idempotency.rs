//! Idempotency checking, key generation, and in-memory deduplication storage.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;

use crate::error::DlqError;
use crate::message::MessageId;

#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    async fn seen(&self, key: &str) -> Result<bool, DlqError>;
    async fn mark_processed(&self, key: &str, ttl: Option<Duration>) -> Result<(), DlqError>;
}

pub fn make_idempotency_key(message_id: &MessageId, attempt: u32, operation: &str) -> String {
    format!("{}:{}:{}", message_id, attempt, operation)
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryIdempotencyStore {
    seen_keys: Arc<RwLock<HashMap<String, Option<DateTime<Utc>>>>>,
}

impl InMemoryIdempotencyStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl IdempotencyStore for InMemoryIdempotencyStore {
    async fn seen(&self, key: &str) -> Result<bool, DlqError> {
        let read = self.seen_keys.read();
        if let Some(exp_opt) = read.get(key) {
            match exp_opt {
                Some(exp) => Ok(Utc::now() < *exp),
                None => Ok(true),
            }
        } else {
            Ok(false)
        }
    }

    async fn mark_processed(&self, key: &str, ttl: Option<Duration>) -> Result<(), DlqError> {
        let mut write = self.seen_keys.write();
        let expires_at = ttl.map(|t| {
            Utc::now() + chrono::Duration::from_std(t).unwrap_or_else(|_| chrono::Duration::hours(24))
        });
        write.insert(key.to_string(), expires_at);
        Ok(())
    }
}
