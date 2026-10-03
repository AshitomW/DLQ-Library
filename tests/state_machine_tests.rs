use std::time::Duration;
use chrono::Utc;
use dlq_lib::state::{Event, Lease, MessageState, MessageStateMachine};

#[test]
fn test_successful_lifecycle() {
    let mut sm = MessageStateMachine::new(MessageState::Received);
    assert_eq!(sm.current(), MessageState::Received);

    assert_eq!(sm.transition(Event::StartProcessing).unwrap(), MessageState::Processing);
    assert_eq!(sm.transition(Event::ProcessSuccess).unwrap(), MessageState::Completed);
    assert!(sm.current().is_terminal());
}

#[test]
fn test_retry_and_dead_letter_lifecycle() {
    let mut sm = MessageStateMachine::new(MessageState::Received);

    // 1st attempt: fail and schedule retry
    sm.transition(Event::StartProcessing).unwrap();
    sm.transition(Event::ScheduleRetry { delay: Duration::from_secs(1) }).unwrap();
    assert_eq!(sm.current(), MessageState::WaitingForRetry);

    // Ready for retry
    sm.transition(Event::RetryAvailable).unwrap();
    assert_eq!(sm.current(), MessageState::Received);

    // 2nd attempt: permanent failure -> DLQ
    sm.transition(Event::StartProcessing).unwrap();
    sm.transition(Event::RejectToDlq { reason: "Permanent error".into() }).unwrap();
    assert_eq!(sm.current(), MessageState::DeadLettered);

    // Operator manual retry from DLQ
    sm.transition(Event::ManualRetry).unwrap();
    assert_eq!(sm.current(), MessageState::Processing);

    // Succeeds on manual retry
    sm.transition(Event::ProcessSuccess).unwrap();
    assert_eq!(sm.current(), MessageState::Completed);
}

#[test]
fn test_illegal_state_transition_rejection() {
    let mut sm = MessageStateMachine::new(MessageState::Completed);
    // Cannot start processing a completed message
    let res = sm.transition(Event::StartProcessing);
    assert!(res.is_err());
}

#[test]
fn test_lease_expiration_and_extension() {
    let lease = Lease::new("worker-1", Duration::from_millis(50), 1);
    let now = Utc::now();
    assert!(lease.is_valid_at(now));

    // After 100ms it should be expired
    let later = now + chrono::Duration::milliseconds(100);
    assert!(!lease.is_valid_at(later));

    // Extending lease
    let mut lease2 = Lease::new("worker-2", Duration::from_secs(10), 1);
    lease2.extend(Duration::from_secs(20));
    assert_eq!(lease2.version, 2);
    assert!(lease2.is_valid_at(now + chrono::Duration::seconds(25)));
}
