//! Error types for DLQ operations, storage backends, codecs, transports, and state transitions.

use std::time::Duration;
use thiserror::Error;

use crate::message::MessageId;

#[derive(Debug, Error)]
pub enum DlqError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Serialization / Codec error: {0}")]
    Serialization(#[from] CodecError),

    #[error("Transport error: {0}")]
    Transport(#[from] TransportError),

    #[error("State machine error: {0}")]
    State(#[from] StateError),

    #[error("Record already exists with ID: {0}")]
    AlreadyExists(MessageId),

    #[error("Record not found with ID: {0}")]
    NotFound(MessageId),

    #[error("State conflict: {0}")]
    Conflict(String),

    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),

    #[error("DLQ service or storage unavailable: {0}")]
    Unavailable(String),

    #[error("Invalid state transition: current={current}, expected={expected}")]
    InvalidState { current: String, expected: String },

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Capacity limit exceeded: {0}")]
    CapacityExceeded(String),

    #[error("Lease on message {0} expired or held by another worker")]
    LeaseExpired(MessageId),

    #[error("DLQ unavailable failover policy rejected message: {0}")]
    DlqUnavailablePolicyRejected(String),
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Record not found: {0}")]
    NotFound(String),

    #[error("Duplicate key error: {0}")]
    DuplicateKey(String),

    #[error("Optimistic concurrency conflict on record {id}, version mismatch")]
    VersionMismatch { id: String },

    #[error("Storage backend connection failure: {0}")]
    Connection(String),

    #[error("Storage query error: {0}")]
    Query(String),

    #[error("Storage internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("Payload encoding error: {0}")]
    Encode(String),

    #[error("Payload decoding error: {0}")]
    Decode(String),

    #[error("Unsupported schema version: {version}, supported up to {supported}")]
    UnsupportedSchemaVersion { version: u32, supported: u32 },

    #[error("CloudEvents conversion error: {0}")]
    CloudEvents(String),
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("Failed to receive message from transport: {0}")]
    Receive(String),

    #[error("Failed to acknowledge message {id}: {reason}")]
    Ack { id: MessageId, reason: String },

    #[error("Failed to negative-acknowledge (nack) message {id}: {reason}")]
    Nack { id: MessageId, reason: String },

    #[error("Failed to publish message to destination {destination}: {reason}")]
    Publish { destination: String, reason: String },

    #[error("Connection lost to transport broker: {0}")]
    Connection(String),
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("Illegal transition from state '{from}' to '{to}' via event '{event}'")]
    IllegalTransition {
        from: String,
        to: String,
        event: String,
    },

    #[error("Message lease error: {0}")]
    LeaseError(String),
}

#[derive(Debug, Clone, Error)]
#[error("{message} (type: {error_type}, code: {code})")]
pub struct ProcessingError {
    pub error_type: String,
    pub code: String,
    pub message: String,
    pub stacktrace: Option<String>,
    pub component: Option<String>,
    pub is_retryable: Option<bool>,
}

impl ProcessingError {
    pub fn new(
        error_type: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            error_type: error_type.into(),
            code: code.into(),
            message: message.into(),
            stacktrace: None,
            component: None,
            is_retryable: None,
        }
    }

    pub fn with_stacktrace(mut self, trace: impl Into<String>) -> Self {
        self.stacktrace = Some(trace.into());
        self
    }

    pub fn with_component(mut self, comp: impl Into<String>) -> Self {
        self.component = Some(comp.into());
        self
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.is_retryable = Some(retryable);
        self
    }
}
