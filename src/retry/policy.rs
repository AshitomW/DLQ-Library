//! Core retry decision abstraction, context, and retry policy trait.

use std::time::Duration;
use serde::{Deserialize, Serialize};

use crate::failure::{DropReason, FailureMetadata, FailureReason};
use crate::message::MessageId;

#[derive(Debug, Clone)]
pub struct RetryContext {
    pub message_id: MessageId,
    pub attempt: u32,
    pub elapsed_duration: Duration,
    pub failure: FailureMetadata,
    pub tenant_id: Option<String>,
}

impl RetryContext {
    pub fn new(message_id: MessageId, attempt: u32, failure: FailureMetadata) -> Self {
        Self {
            message_id,
            attempt,
            elapsed_duration: Duration::from_secs(0),
            failure,
            tenant_id: None,
        }
    }

    pub fn with_elapsed(mut self, elapsed: Duration) -> Self {
        self.elapsed_duration = elapsed;
        self
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryDecision {
    Retry {
        delay: Duration,
        next_attempt: u32,
    },
    DeadLetter {
        reason: FailureReason,
    },
    Drop {
        reason: DropReason,
    },
}

impl RetryDecision {
    pub fn is_retry(&self) -> bool {
        matches!(self, Self::Retry { .. })
    }

    pub fn is_dead_letter(&self) -> bool {
        matches!(self, Self::DeadLetter { .. })
    }
}

pub trait RetryPolicy: Send + Sync {
    fn next_attempt(&self, context: &RetryContext) -> RetryDecision;
}

pub struct NoRetry;

impl RetryPolicy for NoRetry {
    fn next_attempt(&self, context: &RetryContext) -> RetryDecision {
        RetryDecision::DeadLetter {
            reason: FailureReason::new(
                "NO_RETRY_POLICY",
                format!("Message {} failed and retry is disabled", context.message_id),
                "policy",
            ),
        }
    }
}
