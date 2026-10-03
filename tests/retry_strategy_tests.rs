use std::time::Duration;
use dlq_lib::failure::FailureMetadata;
use dlq_lib::message::MessageId;
use dlq_lib::retry::{
    BudgetedRetryPolicy, ExponentialBackoff, FixedBackoff, JitteredBackoff, NoRetry,
    RetryBudget, RetryContext, RetryDecision, RetryPolicy,
};

fn make_context(attempt: u32, retryable: bool) -> RetryContext {
    let failure = FailureMetadata::new("NetworkError", "NET_TIMEOUT", "Connection reset", attempt, retryable);
    RetryContext::new(MessageId::new_v4(), attempt, failure)
}

#[test]
fn test_fixed_backoff_respects_limits() {
    let policy = FixedBackoff::new(Duration::from_secs(2), 3);

    // Attempt 1: should retry (next_attempt = 2)
    let ctx1 = make_context(1, true);
    match policy.next_attempt(&ctx1) {
        RetryDecision::Retry { delay, next_attempt } => {
            assert_eq!(delay, Duration::from_secs(2));
            assert_eq!(next_attempt, 2);
        }
        _ => panic!("Expected Retry"),
    }

    // Attempt 2: should retry (next_attempt = 3)
    let ctx2 = make_context(2, true);
    match policy.next_attempt(&ctx2) {
        RetryDecision::Retry { delay, next_attempt } => {
            assert_eq!(delay, Duration::from_secs(2));
            assert_eq!(next_attempt, 3);
        }
        _ => panic!("Expected Retry"),
    }

    // Attempt 3: reached max_attempts (3), should dead-letter
    let ctx3 = make_context(3, true);
    match policy.next_attempt(&ctx3) {
        RetryDecision::DeadLetter { reason } => {
            assert_eq!(reason.code, "MAX_ATTEMPTS_EXCEEDED");
        }
        _ => panic!("Expected DeadLetter"),
    }
}

#[test]
fn test_fixed_backoff_rejects_non_retryable() {
    let policy = FixedBackoff::new(Duration::from_secs(1), 5);
    let ctx = make_context(1, false);
    match policy.next_attempt(&ctx) {
        RetryDecision::DeadLetter { reason } => {
            assert_eq!(reason.code, "NON_RETRYABLE_ERROR");
        }
        _ => panic!("Expected DeadLetter for non-retryable error"),
    }
}

#[test]
fn test_exponential_backoff_multiplication_and_cap() {
    let policy = ExponentialBackoff::new(
        Duration::from_secs(1),
        Duration::from_secs(10),
        5,
    )
    .with_multiplier(2.0);

    assert_eq!(policy.calculate_delay(1), Duration::from_secs(1));
    assert_eq!(policy.calculate_delay(2), Duration::from_secs(2));
    assert_eq!(policy.calculate_delay(3), Duration::from_secs(4));
    assert_eq!(policy.calculate_delay(4), Duration::from_secs(8));
    assert_eq!(policy.calculate_delay(5), Duration::from_secs(10)); // Capped at 10s
    assert_eq!(policy.calculate_delay(6), Duration::from_secs(10)); // Capped at 10s
}

#[test]
fn test_jittered_backoff_bounds() {
    let base = FixedBackoff::new(Duration::from_millis(100), 5);
    let jittered = JitteredBackoff::full(base);

    let ctx = make_context(1, true);
    for _ in 0..50 {
        match jittered.next_attempt(&ctx) {
            RetryDecision::Retry { delay, .. } => {
                assert!(delay <= Duration::from_millis(100));
            }
            _ => panic!("Expected Retry"),
        }
    }
}

#[test]
fn test_retry_budget_exhaustion() {
    let base = FixedBackoff::new(Duration::from_millis(10), 10);
    // Budget with 2 tokens and 0 refill rate
    let budget = RetryBudget::new(2, 0.0);
    let policy = BudgetedRetryPolicy::new(base, budget);

    let ctx = make_context(1, true);

    // First 2 should acquire token and retry
    assert!(policy.next_attempt(&ctx).is_retry());
    assert!(policy.next_attempt(&ctx).is_retry());

    // 3rd attempt: budget exhausted, should dead-letter
    match policy.next_attempt(&ctx) {
        RetryDecision::DeadLetter { reason } => {
            assert_eq!(reason.code, "RETRY_BUDGET_EXHAUSTED");
        }
        _ => panic!("Expected DeadLetter when budget is exhausted"),
    }
}

#[test]
fn test_no_retry_policy() {
    let policy = NoRetry;
    let ctx = make_context(1, true);
    assert!(policy.next_attempt(&ctx).is_dead_letter());
}
