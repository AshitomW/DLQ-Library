//! Dead-letter queue (DLQ) and failure management library.

pub mod codec;
pub mod error;
pub mod failure;
pub mod message;
pub mod observability;
pub mod retry;
pub mod state;
pub mod storage;
pub mod transport;
