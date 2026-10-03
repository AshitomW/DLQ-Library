use std::time::Duration;
use proptest::prelude::*;

use dlq_lib::failure::FailureMetadata;
use dlq_lib::message::MessageId;
use dlq_lib::retry::{
    ExponentialBackoff, FixedBackoff, JitterMode, JitteredBackoff, RetryContext, RetryPolicy,
};

proptest! {
    #[test]
    fn prop_exponential_delay_always_within_bounds(
        initial_millis in 1u64..1000,
        max_millis in 1000u64..60000,
        attempt in 1u32..50
    ) {
        let initial_delay = Duration::from_millis(initial_millis);
        let max_delay = Duration::from_millis(max_millis);
        let policy = ExponentialBackoff::new(initial_delay, max_delay, 10);

        let delay = policy.calculate_delay(attempt);
        prop_assert!(delay <= max_delay);
        prop_assert!(delay >= initial_delay);
    }

    #[test]
    fn prop_fixed_backoff_never_exceeds_max_attempts(
        max_attempts in 1u32..20,
        attempt in 1u32..30
    ) {
        let policy = FixedBackoff::new(Duration::from_millis(50), max_attempts);
        let failure = FailureMetadata::new("Err", "CODE", "msg", attempt, true);
        let ctx = RetryContext::new(MessageId::new_v4(), attempt, failure);

        let decision = policy.next_attempt(&ctx);
        if attempt >= max_attempts {
            prop_assert!(decision.is_dead_letter());
        } else {
            prop_assert!(decision.is_retry());
        }
    }

    #[test]
    fn prop_full_jitter_never_exceeds_base_delay(
        delay_millis in 1u64..10000
    ) {
        let base_delay = Duration::from_millis(delay_millis);
        let base_policy = FixedBackoff::new(base_delay, 5);
        let jittered = JitteredBackoff::new(base_policy, JitterMode::Full);

        let randomized = jittered.apply_jitter(base_delay);
        prop_assert!(randomized <= base_delay);
    }
}
