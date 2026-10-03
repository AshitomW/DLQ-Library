//! Configuration options and failover policies for DLQ operations.

use std::time::Duration;
use serde::{Deserialize, Serialize};

use crate::error::DlqError;
use crate::storage::retention::RetentionPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DlqUnavailablePolicy {
    #[default]
    RetryWrite,
    LocalBuffer,
    FailConsumer,
    Block,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConcurrencyConfig {
    pub max_retry_concurrency: usize,
    pub batch_size: usize,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            max_retry_concurrency: 10,
            batch_size: 100,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqConfig {
    pub concurrency: ConcurrencyConfig,
    pub retention: RetentionPolicy,
    pub dlq_unavailable_policy: DlqUnavailablePolicy,
    pub default_lease_duration: Duration,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            concurrency: ConcurrencyConfig::default(),
            retention: RetentionPolicy::default(),
            dlq_unavailable_policy: DlqUnavailablePolicy::default(),
            default_lease_duration: Duration::from_secs(30),
        }
    }
}

impl DlqConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn validate(&self) -> Result<(), DlqError> {
        if self.concurrency.max_retry_concurrency == 0 {
            return Err(DlqError::InvalidConfiguration(
                "max_retry_concurrency must be at least 1".into(),
            ));
        }
        if self.concurrency.batch_size == 0 {
            return Err(DlqError::InvalidConfiguration("batch_size must be at least 1".into()));
        }
        if self.default_lease_duration.as_secs() == 0 {
            return Err(DlqError::InvalidConfiguration(
                "default_lease_duration must be at least 1 second".into(),
            ));
        }
        self.retention.validate()?;
        Ok(())
    }
}
