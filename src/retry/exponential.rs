//! Exponential backoff retry policy with multiplier, cap, and duration limits.

use std::time::Duration;

use crate::failure::FailureReason;
use crate::retry::policy::{RetryContext, RetryDecision, RetryPolicy};

#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    pub initial_delay: Duration,
    pub multiplier: f64,
    pub max_delay: Duration,
    pub max_attempts: u32,
    pub max_duration: Option<Duration>,
}

impl ExponentialBackoff {
    pub fn new(initial_delay: Duration, max_delay: Duration, max_attempts: u32) -> Self {
        Self {
            initial_delay,
            multiplier: 2.0,
            max_delay,
            max_attempts,
            max_duration: None,
        }
    }

    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier.max(1.0);
        self
    }

    pub fn with_max_duration(mut self, max_duration: Duration) -> Self {
        self.max_duration = Some(max_duration);
        self
    }

    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        if attempt <= 1 {
            return self.initial_delay.min(self.max_delay);
        }

        let power = (attempt - 1) as i32;
        let factor = self.multiplier.powi(power);
        let secs = self.initial_delay.as_secs_f64() * factor;

        if secs.is_infinite() || secs >= self.max_delay.as_secs_f64() {
            self.max_delay
        } else {
            Duration::from_secs_f64(secs).min(self.max_delay)
        }
    }
}

impl RetryPolicy for ExponentialBackoff {
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

        let delay = self.calculate_delay(context.attempt);

        RetryDecision::Retry {
            delay,
            next_attempt: context.attempt + 1,
        }
    }
}
