use std::sync::Arc;
use std::time::Duration;
use dlq_lib::engine::{DlqConfig, DlqEngine, DlqUnavailablePolicy, ProcessingOutcome};
use dlq_lib::error::ProcessingError;
use dlq_lib::failure::ClassifierChain;
use dlq_lib::message::{Message, MessageId, SourceMetadata};
use dlq_lib::retry::FixedBackoff;
use dlq_lib::storage::{DeadLetterStore, InMemoryDeadLetterStore};
use dlq_lib::transport::{make_idempotency_key, IdempotencyStore, InMemoryIdempotencyStore, InMemoryTransport};

#[tokio::test]
async fn test_poison_message_routes_to_dlq_within_attempt_limit() {
    let store = Arc::new(InMemoryDeadLetterStore::default());
    let transport = Arc::new(InMemoryTransport::new("test-broker"));
    let retry_policy = Arc::new(FixedBackoff::new(Duration::from_millis(10), 3));
    let classifier = Arc::new(ClassifierChain::default());
    let config = DlqConfig::default();

    let engine = DlqEngine::new(store.clone(), transport.clone(), retry_policy, classifier, config).unwrap();

    let msg = Message::new(MessageId::new("poison-1"), b"poison-payload".to_vec());
    let source = SourceMetadata::new("test-broker", "orders");
    let err = ProcessingError::new("DatabaseError", "DB_TIMEOUT", "Connection timed out");

    // Attempt 1: retry scheduled
    let out1 = engine.handle_failure(msg.clone(), source.clone(), &err, 1, None).await.unwrap();
    assert!(matches!(out1, ProcessingOutcome::RetryScheduled { next_attempt: 2, .. }));

    // Attempt 2: retry scheduled
    let out2 = engine.handle_failure(msg.clone(), source.clone(), &err, 2, None).await.unwrap();
    assert!(matches!(out2, ProcessingOutcome::RetryScheduled { next_attempt: 3, .. }));

    // Attempt 3: exhausted max attempts (3) -> Routed to DLQ!
    let out3 = engine.handle_failure(msg.clone(), source.clone(), &err, 3, None).await.unwrap();
    match out3 {
        ProcessingOutcome::DeadLettered(dlq_id) => {
            let record = store.get(&dlq_id).await.unwrap();
            assert!(record.is_some());
            assert_eq!(record.unwrap().original_id().as_str(), "poison-1");
        }
        _ => panic!("Expected DeadLettered on 3rd attempt"),
    }
}

#[tokio::test]
async fn test_dlq_storage_unavailable_local_buffer_failover() {
    // Capacity 0 store that will reject puts
    let store = Arc::new(InMemoryDeadLetterStore::new(
        dlq_lib::storage::RetentionPolicy::new().with_max_messages(1)
    ));
    // Fill the store so next put fails
    let dummy = dlq_lib::message::DeadLetter::create(
        Message::new("d1", vec![]),
        SourceMetadata::new("b", "q"),
        dlq_lib::failure::FailureMetadata::new("t", "c", "m", 1, false),
        None,
    );
    store.put(dummy).await.unwrap();

    let transport = Arc::new(InMemoryTransport::new("test-broker"));
    let retry_policy = Arc::new(FixedBackoff::new(Duration::from_millis(10), 1));
    let classifier = Arc::new(ClassifierChain::default());

    let mut config = DlqConfig::default();
    config.dlq_unavailable_policy = DlqUnavailablePolicy::LocalBuffer;

    let engine = DlqEngine::new(store.clone(), transport.clone(), retry_policy, classifier, config).unwrap();

    let msg = Message::new(MessageId::new("overflow-msg"), b"overflow".to_vec());
    let source = SourceMetadata::new("test-broker", "orders");
    let err = ProcessingError::new("ValidationError", "SCHEMA_INVALID", "Invalid format");

    // Failure occurs and store is full -> failover to local buffer
    let outcome = engine.handle_failure(msg, source, &err, 1, None).await.unwrap();
    assert!(matches!(outcome, ProcessingOutcome::DeadLettered(_)));
    assert_eq!(engine.buffered_records_count(), 1);
}

#[tokio::test]
async fn test_idempotency_store_duplicate_prevention() {
    let idempotency = InMemoryIdempotencyStore::new();
    let msg_id = MessageId::new("msg-42");
    let key = make_idempotency_key(&msg_id, 1, "process");

    assert!(!idempotency.seen(&key).await.unwrap());

    // Mark as processed with 1 second TTL
    idempotency.mark_processed(&key, Some(Duration::from_secs(1))).await.unwrap();

    // Redelivery should be detected as already seen
    assert!(idempotency.seen(&key).await.unwrap());
}
