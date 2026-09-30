//! Transport origin, delivery state, and lifecycle timestamp metadata.

use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub broker: String,
    pub topic_or_queue: String,
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    pub consumer_group: Option<String>,
    pub subscription: Option<String>,
    pub custom: HashMap<String, String>,
}

impl SourceMetadata {
    pub fn new(broker: impl Into<String>, topic_or_queue: impl Into<String>) -> Self {
        Self {
            broker: broker.into(),
            topic_or_queue: topic_or_queue.into(),
            partition: None,
            offset: None,
            consumer_group: None,
            subscription: None,
            custom: HashMap::new(),
        }
    }

    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn with_offset(mut self, offset: i64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn with_consumer_group(mut self, group: impl Into<String>) -> Self {
        self.consumer_group = Some(group.into());
        self
    }

    pub fn with_subscription(mut self, sub: impl Into<String>) -> Self {
        self.subscription = Some(sub.into());
        self
    }

    pub fn with_custom(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.custom.insert(key.into(), val.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryMetadata {
    pub attempt_count: u32,
    pub redelivery_count: u32,
    pub delivery_tag: Option<String>,
    pub lease_holder: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub version: u64,
}

impl Default for DeliveryMetadata {
    fn default() -> Self {
        Self {
            attempt_count: 1,
            redelivery_count: 0,
            delivery_tag: None,
            lease_holder: None,
            lease_expires_at: None,
            version: 1,
        }
    }
}

impl DeliveryMetadata {
    pub fn new(attempt_count: u32) -> Self {
        Self {
            attempt_count,
            ..Default::default()
        }
    }

    pub fn is_leased_at(&self, now: DateTime<Utc>) -> bool {
        match (self.lease_holder.as_ref(), self.lease_expires_at) {
            (Some(_), Some(exp)) => exp > now,
            _ => false,
        }
    }

    pub fn increment_attempt(&mut self) {
        self.attempt_count += 1;
        self.version += 1;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampMetadata {
    pub received_at: DateTime<Utc>,
    pub failed_at: DateTime<Utc>,
    pub dead_lettered_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_attempt_at: Option<DateTime<Utc>>,
}

impl TimestampMetadata {
    pub fn now() -> Self {
        let now = Utc::now();
        Self {
            received_at: now,
            failed_at: now,
            dead_lettered_at: None,
            expires_at: None,
            last_attempt_at: Some(now),
        }
    }

    pub fn with_received_at(mut self, dt: DateTime<Utc>) -> Self {
        self.received_at = dt;
        self
    }

    pub fn with_failed_at(mut self, dt: DateTime<Utc>) -> Self {
        self.failed_at = dt;
        self
    }

    pub fn with_dead_lettered_at(mut self, dt: DateTime<Utc>) -> Self {
        self.dead_lettered_at = Some(dt);
        self
    }

    pub fn with_expires_at(mut self, dt: DateTime<Utc>) -> Self {
        self.expires_at = Some(dt);
        self
    }
}
