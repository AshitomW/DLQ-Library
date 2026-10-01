//! Fixed-interval retry policy implementation.

use std::time::Duration;

use crate::failure::FailureReason;
use crate::retry::policy::{RetryContext, RetryDecision, RetryPolicy};

#[derive(Debug, Clone)]
pub struct FixedBackoff {
    pub delay: Duration,
    pub max_attempts: u32,
    pub max_duration: Option<Duration>,
}

impl FixedBackoff {
    pub fn new(delay: Duration, max_attempts: u32) -> Self {
        Self {
            delay,
            max_attempts,
            max_duration: None,
        }
    }

    pub fn with_max_duration(mut self, max_duration: Duration) -> Self {
        self.max_duration = Some(max_duration);
        self
    }
}

impl RetryPolicy for FixedBackoff {
    fn next_attempt(&self, context: &RetryContext) -> RetryDecision {
        if !context.failure.retryable {
            return RetryDecision::DeadLetter {
                reason: FailureReason::new(
                    "NON_RETRYABLE_ERROR",
                    format!("Error {} marked non-retryable", context.failure.code),
                    "classification",
                ),
            };
        }

        if context.attempt >= self.max_attempts {
            return RetryDecision::DeadLetter {
                reason: FailureReason::new(
                    "MAX_ATTEMPTS_EXCEEDED",
                    format!(
                        "Exceeded maximum attempts ({}/{}): {}",
                        context.attempt, self.max_attempts, context.failure.message
                    ),
                    "limits",
                ),
            };
        }

        if let Some(max_dur) = self.max_duration {
            if context.elapsed_duration >= max_dur {
                return RetryDecision::DeadLetter {
                    reason: FailureReason::new(
                        "MAX_DURATION_EXCEEDED",
                        format!(
                            "Exceeded maximum retry duration ({:?}/{:?})",
                            context.elapsed_duration, max_dur
                        ),
                        "limits",
                    ),
                };
            }
        }

        RetryDecision::Retry {
            delay: self.delay,
            next_attempt: context.attempt + 1,
        }
    }
}
