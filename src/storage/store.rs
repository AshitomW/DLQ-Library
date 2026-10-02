//! Repository trait defining persistence, querying, and leasing operations for dead letters.

use std::time::Duration;
use async_trait::async_trait;

use crate::error::DlqError;
use crate::message::{DeadLetter, MessageId};
use crate::state::MessageState;
use crate::storage::query::{DlqQuery, Page};

#[async_trait]
pub trait DeadLetterStore: Send + Sync {
    async fn put(&self, record: DeadLetter) -> Result<(), DlqError>;
    async fn get(&self, id: &MessageId) -> Result<Option<DeadLetter>, DlqError>;
    async fn delete(&self, id: &MessageId) -> Result<(), DlqError>;
    async fn list(&self, query: &DlqQuery) -> Result<Page<DeadLetter>, DlqError>;
    async fn count(&self, query: &DlqQuery) -> Result<usize, DlqError>;
    async fn update_state(
        &self,
        id: &MessageId,
        expected: MessageState,
        new_state: MessageState,
    ) -> Result<(), DlqError>;
    async fn acquire_lease(
        &self,
        id: &MessageId,
        worker_id: &str,
        duration: Duration,
    ) -> Result<Option<DeadLetter>, DlqError>;
    async fn release_lease(&self, id: &MessageId, worker_id: &str) -> Result<(), DlqError>;
    async fn purge_expired(&self, max_age: Duration) -> Result<usize, DlqError>;
}
