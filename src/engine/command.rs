//! Administrative commands and execution results for dead-letter records.

use serde::{Deserialize, Serialize};

use crate::message::{DeadLetter, MessageId};
use crate::storage::query::DlqQuery;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DlqCommand {
    Retry(MessageId),
    RetryBatch(DlqQuery),
    Delete(MessageId),
    PurgeExpired,
    Inspect(MessageId),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandResult {
    Retried(MessageId),
    BatchRetried {
        total_matched: usize,
        succeeded: usize,
        failed: usize,
    },
    Deleted(MessageId),
    Purged(usize),
    Inspected(Option<DeadLetter>),
}
