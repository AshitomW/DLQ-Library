//! Jitter calculations and decorator strategy for randomized backoff delays.

use std::time::Duration;
use rand::Rng;

use crate::retry::policy::{RetryContext, RetryDecision, RetryPolicy};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JitterMode {
    Full,
    Equal,
    Decorrelated,
}

pub struct JitteredBackoff {
    inner_policy: Box<dyn RetryPolicy>,
    mode: JitterMode,
}

impl JitteredBackoff {
    pub fn new(inner: impl RetryPolicy + 'static, mode: JitterMode) -> Self {
        Self {
            inner_policy: Box::new(inner),
            mode,
        }
    }

    pub fn full(inner: impl RetryPolicy + 'static) -> Self {
        Self::new(inner, JitterMode::Full)
    }

    pub fn equal(inner: impl RetryPolicy + 'static) -> Self {
        Self::new(inner, JitterMode::Equal)
    }

    pub fn apply_jitter(&self, base_delay: Duration) -> Duration {
        let mut rng = rand::thread_rng();
        let base_nanos = base_delay.as_nanos() as u64;
        if base_nanos == 0 {
            return Duration::from_millis(0);
        }

        match self.mode {
            JitterMode::Full => {
                let jittered_nanos = rng.gen_range(0..=base_nanos);
                Duration::from_nanos(jittered_nanos)
            }
            JitterMode::Equal => {
                let half = base_nanos / 2;
                let jittered_half = rng.gen_range(0..=half);
                Duration::from_nanos(half + jittered_half)
            }
            JitterMode::Decorrelated => {
                let min = base_nanos / 2;
                let max = base_nanos.saturating_mul(2);
                let jittered_nanos = rng.gen_range(min..=max);
                Duration::from_nanos(jittered_nanos)
            }
        }
    }
}

impl RetryPolicy for JitteredBackoff {
    fn next_attempt(&self, context: &RetryContext) -> RetryDecision {
        let decision = self.inner_policy.next_attempt(context);
        match decision {
            RetryDecision::Retry { delay, next_attempt } => {
                let jittered = self.apply_jitter(delay);
                RetryDecision::Retry {
                    delay: jittered,
                    next_attempt,
                }
            }
            other => other,
        }
    }
}
