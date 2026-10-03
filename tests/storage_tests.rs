use std::time::Duration;
use dlq_lib::failure::FailureMetadata;
use dlq_lib::message::{DeadLetter, Message, MessageId, SourceMetadata};
use dlq_lib::state::MessageState;
use dlq_lib::storage::{DeadLetterStore, DlqQuery, InMemoryDeadLetterStore, RetentionPolicy};

fn make_record(id: &str, topic: &str, code: &str, tenant: Option<&str>) -> DeadLetter {
    let msg = Message::new(MessageId::new(id), b"data".to_vec());
    let source = SourceMetadata::new("in-memory", topic);
    let failure = FailureMetadata::new("TestFailure", code, "test failure message", 1, false);
    DeadLetter::create(msg, source, failure, tenant.map(|s| s.to_string()))
}

#[tokio::test]
async fn test_store_crud_and_pagination() {
    let store = InMemoryDeadLetterStore::default();

    for i in 1..=15 {
        let rec = make_record(&format!("msg-{}", i), "orders", "ERR_TEST", Some("tenant-a"));
        store.put(rec).await.unwrap();
    }

    // Query with pagination limit 5, offset 0
    let query1 = DlqQuery::new().with_topic("orders").with_pagination(0, 5);
    let page1 = store.list(&query1).await.unwrap();
    assert_eq!(page1.items.len(), 5);
    assert_eq!(page1.total, 15);
    assert!(page1.has_more);

    // Query page 2: offset 5, limit 5
    let query2 = DlqQuery::new().with_topic("orders").with_pagination(5, 5);
    let page2 = store.list(&query2).await.unwrap();
    assert_eq!(page2.items.len(), 5);
    assert_eq!(page2.offset, 5);
    assert!(page2.has_more);

    // Query last page: offset 10, limit 10
    let query3 = DlqQuery::new().with_topic("orders").with_pagination(10, 10);
    let page3 = store.list(&query3).await.unwrap();
    assert_eq!(page3.items.len(), 5);
    assert!(!page3.has_more);
}

#[tokio::test]
async fn test_store_capacity_limits() {
    let retention = RetentionPolicy::new().with_max_messages(2);
    let store = InMemoryDeadLetterStore::new(retention);

    let r1 = make_record("m1", "t1", "E1", None);
    let r2 = make_record("m2", "t1", "E1", None);
    let r3 = make_record("m3", "t1", "E1", None);

    store.put(r1).await.unwrap();
    store.put(r2).await.unwrap();

    // 3rd insert should fail due to capacity
    let res = store.put(r3).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_store_worker_leasing() {
    let store = InMemoryDeadLetterStore::default();
    let rec = make_record("rec-1", "t1", "E1", None);
    let id = rec.id.clone();
    store.put(rec).await.unwrap();

    // Worker 1 acquires 100ms lease
    let leased_opt = store.acquire_lease(&id, "worker-1", Duration::from_millis(100)).await.unwrap();
    assert!(leased_opt.is_some());
    let leased = leased_opt.unwrap();
    assert_eq!(leased.state, MessageState::Processing);

    // Worker 2 attempts to acquire active lease -> should fail (return None)
    let worker2_opt = store.acquire_lease(&id, "worker-2", Duration::from_secs(1)).await.unwrap();
    assert!(worker2_opt.is_none());

    // Wait for lease to expire
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Worker 2 should now succeed in acquiring expired lease
    let worker2_success = store.acquire_lease(&id, "worker-2", Duration::from_secs(1)).await.unwrap();
    assert!(worker2_success.is_some());
}
