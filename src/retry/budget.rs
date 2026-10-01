//! Token-bucket retry budget preventing retry storms.

use std::sync::Arc;
use std::time::Instant;
use parking_lot::Mutex;

use crate::failure::FailureReason;
use crate::retry::policy::{RetryContext, RetryDecision, RetryPolicy};

#[derive(Debug, Clone)]
pub struct RetryBudget {
    state: Arc<Mutex<TokenBucket>>,
}

#[derive(Debug)]
struct TokenBucket {
    max_tokens: f64,
    current_tokens: f64,
    refill_rate_per_sec: f64,
    last_refill: Instant,
}

impl RetryBudget {
    pub fn new(max_tokens: u32, refill_rate_per_sec: f64) -> Self {
        Self {
            state: Arc::new(Mutex::new(TokenBucket {
                max_tokens: max_tokens as f64,
                current_tokens: max_tokens as f64,
                refill_rate_per_sec,
                last_refill: Instant::now(),
            })),
        }
    }

    pub fn try_acquire(&self) -> bool {
        let mut bucket = self.state.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.last_refill = now;

        bucket.current_tokens = (bucket.current_tokens + elapsed * bucket.refill_rate_per_sec)
            .min(bucket.max_tokens);

        if bucket.current_tokens >= 1.0 {
            bucket.current_tokens -= 1.0;
            true
        } else {
            false
        }
    }

    pub fn available_tokens(&self) -> f64 {
        let bucket = self.state.lock();
        bucket.current_tokens
    }
}

pub struct BudgetedRetryPolicy {
    inner_policy: Box<dyn RetryPolicy>,
    budget: RetryBudget,
}

impl BudgetedRetryPolicy {
    pub fn new(inner: impl RetryPolicy + 'static, budget: RetryBudget) -> Self {
        Self {
            inner_policy: Box::new(inner),
            budget,
        }
    }
}

impl RetryPolicy for BudgetedRetryPolicy {
    fn next_attempt(&self, context: &RetryContext) -> RetryDecision {
        let decision = self.inner_policy.next_attempt(context);
        match decision {
            RetryDecision::Retry { delay, next_attempt } => {
                if self.budget.try_acquire() {
                    RetryDecision::Retry { delay, next_attempt }
                } else {
                    RetryDecision::DeadLetter {
                        reason: FailureReason::new(
                            "RETRY_BUDGET_EXHAUSTED",
                            "Retry rejected because retry budget token quota is exhausted",
                            "rate_limit",
                        ),
                    }
                }
            }
            other => other,
        }
    }
}
