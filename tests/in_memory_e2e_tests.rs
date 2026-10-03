use std::sync::Arc;
use std::time::Duration;
use dlq_lib::engine::{CommandResult, DlqCommand, DlqConfig, DlqEngine, ProcessingOutcome};
use dlq_lib::error::ProcessingError;
use dlq_lib::failure::ClassifierChain;
use dlq_lib::message::{Message, MessageId, SourceMetadata};
use dlq_lib::retry::FixedBackoff;
use dlq_lib::state::MessageState;
use dlq_lib::storage::{DeadLetterStore, DlqQuery, InMemoryDeadLetterStore};
use dlq_lib::transport::{InMemoryTransport, MessageTransport};

#[tokio::test]
async fn test_end_to_end_dlq_workflow() {
    let store = Arc::new(InMemoryDeadLetterStore::default());
    let transport = Arc::new(InMemoryTransport::new("kafka-cluster"));
    let retry_policy = Arc::new(FixedBackoff::new(Duration::from_millis(50), 2));
    let classifier = Arc::new(ClassifierChain::default());
    let config = DlqConfig::default();

    let engine = DlqEngine::new(
        store.clone(),
        transport.clone(),
        retry_policy,
        classifier,
        config,
    )
    .unwrap();

    // 1. Enqueue incoming message in transport
    let msg1 = Message::new(MessageId::new("order-101"), b"{\"item\": \"laptop\"}".to_vec())
        .with_header("correlation-id", "corr-101");
    transport.enqueue(msg1.clone());

    // 2. Consume from transport
    let received = transport.receive().await.unwrap().expect("should receive msg1");
    let source = SourceMetadata::new("kafka-cluster", "orders-topic");

    // 3. Message processing fails permanently
    let err = ProcessingError::new("PaymentGatewayError", "CARD_DECLINED", "Card has expired");
    let outcome = engine.handle_failure(received, source.clone(), &err, 1, Some("tenant-us".into())).await.unwrap();

    let dlq_id = match outcome {
        ProcessingOutcome::DeadLettered(id) => id,
        _ => panic!("Expected DeadLettered"),
    };

    // 4. Transport should have acknowledged original message once safely dead-lettered
    assert!(transport.is_acknowledged(&msg1.id));

    // 5. Verify record exists in DLQ store
    let record = store.get(&dlq_id).await.unwrap().expect("DLQ record should exist");
    assert_eq!(record.state, MessageState::DeadLettered);
    assert_eq!(record.original_id().as_str(), "order-101");
    assert_eq!(record.failure.code, "CARD_DECLINED");

    // 6. Operator queries DLQ by failure code
    let query = DlqQuery::new().with_failure_code("CARD_DECLINED");
    let page = store.list(&query).await.unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.items[0].id, dlq_id);

    // 7. Operator executes DlqCommand::Retry
    let cmd_res = engine.execute_command(DlqCommand::Retry(dlq_id.clone())).await.unwrap();
    assert_eq!(cmd_res, CommandResult::Retried(dlq_id.clone()));

    // 8. Verify message was republished to original topic
    let republished = transport.published_to("orders-topic");
    assert_eq!(republished.len(), 1);
    assert_eq!(republished[0].id, msg1.id);

    // 9. Verify record state is now Completed
    let updated_record = store.get(&dlq_id).await.unwrap().unwrap();
    assert_eq!(updated_record.state, MessageState::Completed);
}

#[tokio::test]
async fn test_batch_retry_command() {
    let store = Arc::new(InMemoryDeadLetterStore::default());
    let transport = Arc::new(InMemoryTransport::new("kafka-cluster"));
    let retry_policy = Arc::new(FixedBackoff::new(Duration::from_millis(10), 1));
    let classifier = Arc::new(ClassifierChain::default());
    let config = DlqConfig::default();

    let engine = DlqEngine::new(
        store.clone(),
        transport.clone(),
        retry_policy,
        classifier,
        config,
    )
    .unwrap();

    // Populate store with 5 dead letters
    for i in 1..=5 {
        let msg = Message::new(format!("batch-item-{}", i), vec![]);
        let source = SourceMetadata::new("kafka", "events");
        let failure = dlq_lib::failure::FailureMetadata::new("Err", "RETRY_ME", "transient", 2, false);
        let dl = dlq_lib::message::DeadLetter::create(msg, source, failure, None);
        store.put(dl).await.unwrap();
    }

    let query = DlqQuery::new().with_failure_code("RETRY_ME");
    let cmd_res = engine.execute_command(DlqCommand::RetryBatch(query)).await.unwrap();

    match cmd_res {
        CommandResult::BatchRetried { total_matched, succeeded, failed } => {
            assert_eq!(total_matched, 5);
            assert_eq!(succeeded, 5);
            assert_eq!(failed, 0);
        }
        _ => panic!("Expected BatchRetried"),
    }

    let published = transport.published_to("events");
    assert_eq!(published.len(), 5);
}
