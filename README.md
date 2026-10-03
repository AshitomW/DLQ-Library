# DLQ Library (`dlq-lib`)

A broker-independent Rust library for failure handling, retry scheduling, and dead-letter queue management.

---

## Overview

When messages fail during processing, `dlq-lib` provides a structured workflow to classify the failure, schedule retries with configurable backoff, or route unrecoverable messages to a dead-letter queue for inspection and manual or batch reprocessing.

### Core Capabilities

- **At-Least-Once Delivery**: Built around the reality of at-least-once message delivery, accounting for redeliveries, duplicate messages, and non-atomic acknowledgement boundaries.
- **Independent Identity**: DLQ records receive distinct IDs separate from original message IDs so retried messages maintain clean audit histories.
- **Machine-Readable Failure Telemetry**: Preserves error codes, categories, attempt counts, components, stack traces, and timestamps rather than just free-form error strings.
- **Configurable Retry Backoff**: Supports fixed intervals, exponential delays with maximum caps, randomized jitter modes (full, equal, decorrelated), and token-bucket retry budgets.
- **Lifecycle Tracking & Worker Leases**: Messages follow an explicit state machine (`Received`, `Processing`, `WaitingForRetry`, `DeadLettered`, `Completed`, `Expired`, `Deleted`) with lease timeouts to prevent concurrent workers from dual-processing the same record.
- **Bounded Storage & Queries**: Paginated querying (never unbounded memory dumps), retention enforcement (`max_age`, `max_messages`, `max_bytes`), and failover policies (such as local buffering) if storage is temporarily unreachable.
- **Safe Observability**: Low-cardinality metric counters and automated redaction of sensitive headers (`Authorization`, `Cookie`, API keys) and payloads from logs.

---

## Architecture Flow

```text
Incoming Message
      │
      ▼
 Processing Failure ──► Failure Classifier ──► Permanent ──► Dead Letter Store
      │                                                              │
      ▼                                                              ▼
 Retry Policy ──► Retry Scheduled (backoff)             Operator Inspection &
      │                                                 Batch/Manual Reprocessing
      ▼
 Reached Max Attempts ──► Dead Letter Store
```

---

## Quick Start

### 1. Ingesting and Handling Message Failures

```rust
use std::sync::Arc;
use std::time::Duration;
use dlq_lib::engine::{DlqConfig, DlqEngine, ProcessingOutcome};
use dlq_lib::error::ProcessingError;
use dlq_lib::failure::ClassifierChain;
use dlq_lib::message::{Message, MessageId, SourceMetadata};
use dlq_lib::retry::ExponentialBackoff;
use dlq_lib::storage::InMemoryDeadLetterStore;
use dlq_lib::transport::InMemoryTransport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error.Error>> {
    // 1. Initialize store, transport, retry rules, and engine
    let store = Arc::new(InMemoryDeadLetterStore::default());
    let transport = Arc::new(InMemoryTransport::new("orders-broker"));
    let retry_policy = Arc::new(ExponentialBackoff::new(
        Duration::from_millis(500),
        Duration::from_secs(30),
        3,
    ));
    let classifier = Arc::new(ClassifierChain::default());
    let config = DlqConfig::default();

    let engine = DlqEngine::new(
        store.clone(),
        transport.clone(),
        retry_policy,
        classifier,
        config,
    )?;

    // 2. Incoming message fails during processing
    let msg = Message::new(MessageId::new("order-101"), b"{\"item\": \"laptop\"}".to_vec())
        .with_header("Authorization", "Bearer sensitive-token")
        .with_header("correlation-id", "corr-456");

    let source = SourceMetadata::new("orders-broker", "orders-topic");
    let err = ProcessingError::new("DatabaseError", "DB_TIMEOUT", "Connection timed out");

    // 3. Engine evaluates the error and schedules a retry or routes to DLQ
    let outcome = engine.handle_failure(msg, source, &err, 1, Some("tenant-1".into())).await?;

    match outcome {
        ProcessingOutcome::RetryScheduled { delay, next_attempt } => {
            println!("Retry attempt {} scheduled in {:?}", next_attempt, delay);
        }
        ProcessingOutcome::DeadLettered(dlq_id) => {
            println!("Message exceeded attempts or failed permanently. DLQ ID: {}", dlq_id);
        }
        ProcessingOutcome::Dropped(reason) => {
            println!("Message dropped: {}", reason);
        }
    }

    Ok(())
}
```

### 2. Querying and Retrying Dead Letters

```rust
use dlq_lib::engine::{CommandResult, DlqCommand};
use dlq_lib::storage::DlqQuery;

// Search for records by error code with pagination
let query = DlqQuery::new()
    .with_failure_code("DB_TIMEOUT")
    .with_pagination(0, 50);

// Reprocess matching records with bounded concurrency
let result = engine.execute_command(DlqCommand::RetryBatch(query)).await?;

if let CommandResult::BatchRetried { total_matched, succeeded, failed } = result {
    println!("Retried {} records ({} succeeded, {} failed)", total_matched, succeeded, failed);
}
```

---

## Running Tests

The test suite covers unit logic, failure injection scenarios (poison messages, lost ACKs, storage failovers), and invariant property tests:

```bash
cargo test
```
