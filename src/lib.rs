//! Broker-independent dead-letter queue (DLQ) and failure management library.

pub mod codec;
pub mod engine;
pub mod error;
pub mod failure;
pub mod message;
pub mod observability;
pub mod retry;
pub mod state;
pub mod storage;
pub mod transport;

pub use codec::{CloudEventEnvelope, Codec, JsonCodec};
pub use engine::{
    BatchRetrier, BatchRetryResult, CommandResult, ConcurrencyConfig, DlqCommand, DlqConfig,
    DlqEngine, DlqUnavailablePolicy, ProcessingOutcome,
};
pub use error::{DlqError, ProcessingError, StorageError, TransportError};
pub use failure::{
    ClassifierChain, DropReason, FailureClassification, FailureClassifier, FailureMetadata,
    FailureReason, FnClassifier, ProcessingErrorClassifier, TimeoutClassifier,
};
pub use message::{
    is_sensitive_header, redact_sensitive_headers, safe_payload_summary, sanitize_headers,
    DeadLetter, DeliveryMetadata, Message, MessageId, SourceMetadata, TimestampMetadata,
};
pub use observability::{InMemoryMetrics, LoggingTransport, MetricsCollector, MetricsTransport};
pub use retry::{
    BudgetedRetryPolicy, ExponentialBackoff, FixedBackoff, JitterMode, JitteredBackoff, NoRetry,
    RetryBudget, RetryContext, RetryDecision, RetryPolicy,
};
pub use state::{Event, Lease, MessageState, MessageStateMachine};
pub use storage::{DeadLetterStore, DlqQuery, InMemoryDeadLetterStore, Page, RetentionPolicy};
pub use transport::{
    make_idempotency_key, IdempotencyStore, InMemoryIdempotencyStore, InMemoryTransport,
    MessageTransport,
};
