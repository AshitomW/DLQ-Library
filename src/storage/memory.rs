//! In-memory dead-letter store repository with capacity bounds, leasing, queries, and retention.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use chrono::Utc;
use parking_lot::RwLock;

use crate::error::DlqError;
use crate::message::{DeadLetter, MessageId};
use crate::state::MessageState;
use crate::storage::query::{DlqQuery, Page};
use crate::storage::retention::RetentionPolicy;
use crate::storage::store::DeadLetterStore;

#[derive(Debug, Clone)]
pub struct InMemoryDeadLetterStore {
    records: Arc<RwLock<BTreeMap<MessageId, DeadLetter>>>,
    retention: RetentionPolicy,
}

impl Default for InMemoryDeadLetterStore {
    fn default() -> Self {
        Self::new(RetentionPolicy::default())
    }
}

impl InMemoryDeadLetterStore {
    pub fn new(retention: RetentionPolicy) -> Self {
        Self {
            records: Arc::new(RwLock::new(BTreeMap::new())),
            retention,
        }
    }

    pub fn current_bytes(&self) -> usize {
        let read = self.records.read();
        read.values().map(|r| r.original_message.payload.len()).sum()
    }

    pub fn len(&self) -> usize {
        self.records.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.read().is_empty()
    }
}

#[async_trait]
impl DeadLetterStore for InMemoryDeadLetterStore {
    async fn put(&self, record: DeadLetter) -> Result<(), DlqError> {
        let mut write = self.records.write();

        if let Some(max_msg) = self.retention.max_messages {
            if !write.contains_key(&record.id) && write.len() >= max_msg {
                return Err(DlqError::CapacityExceeded(format!(
                    "DLQ store capacity exceeded maximum message limit ({})",
                    max_msg
                )));
            }
        }

        if let Some(max_bytes) = self.retention.max_bytes {
            let record_bytes = record.original_message.payload.len();
            let current_bytes: usize = write.values().map(|r| r.original_message.payload.len()).sum();
            if current_bytes + record_bytes > max_bytes {
                return Err(DlqError::CapacityExceeded(format!(
                    "DLQ store payload capacity exceeded maximum bytes limit ({} bytes)",
                    max_bytes
                )));
            }
        }

        write.insert(record.id.clone(), record);
        Ok(())
    }

    async fn get(&self, id: &MessageId) -> Result<Option<DeadLetter>, DlqError> {
        let read = self.records.read();
        Ok(read.get(id).cloned())
    }

    async fn delete(&self, id: &MessageId) -> Result<(), DlqError> {
        let mut write = self.records.write();
        if write.remove(id).is_some() {
            Ok(())
        } else {
            Err(DlqError::NotFound(id.clone()))
        }
    }

    async fn list(&self, query: &DlqQuery) -> Result<Page<DeadLetter>, DlqError> {
        let read = self.records.read();

        let filtered: Vec<DeadLetter> = read
            .values()
            .filter(|record| {
                if let Some(ref tid) = query.tenant_id {
                    if record.tenant_id.as_deref() != Some(tid.as_str()) {
                        return false;
                    }
                }
                if let Some(expected_state) = query.state {
                    if record.state != expected_state {
                        return false;
                    }
                }
                if let Some(ref ftype) = query.failure_type {
                    if &record.failure.failure_type != ftype {
                        return false;
                    }
                }
                if let Some(ref code) = query.failure_code {
                    if &record.failure.code != code {
                        return false;
                    }
                }
                if let Some(ref broker) = query.source_broker {
                    if &record.source.broker != broker {
                        return false;
                    }
                }
                if let Some(ref topic) = query.topic_or_queue {
                    if &record.source.topic_or_queue != topic {
                        return false;
                    }
                }
                if let Some(min_att) = query.min_attempts {
                    if record.delivery.attempt_count < min_att {
                        return false;
                    }
                }
                if let Some(after) = query.failed_after {
                    if record.timestamps.failed_at < after {
                        return false;
                    }
                }
                if let Some(before) = query.failed_before {
                    if record.timestamps.failed_at > before {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        let total = filtered.len();
        let items: Vec<DeadLetter> = filtered
            .into_iter()
            .skip(query.offset)
            .take(query.limit)
            .collect();

        Ok(Page::new(items, total, query.offset, query.limit))
    }

    async fn count(&self, query: &DlqQuery) -> Result<usize, DlqError> {
        let page = self.list(query).await?;
        Ok(page.total)
    }

    async fn update_state(
        &self,
        id: &MessageId,
        expected: MessageState,
        new_state: MessageState,
    ) -> Result<(), DlqError> {
        let mut write = self.records.write();
        let record = write.get_mut(id).ok_or_else(|| DlqError::NotFound(id.clone()))?;

        if record.state != expected {
            return Err(DlqError::InvalidState {
                current: record.state.to_string(),
                expected: expected.to_string(),
            });
        }

        record.state = new_state;
        record.delivery.version += 1;
        Ok(())
    }

    async fn acquire_lease(
        &self,
        id: &MessageId,
        worker_id: &str,
        duration: Duration,
    ) -> Result<Option<DeadLetter>, DlqError> {
        let mut write = self.records.write();
        let record = match write.get_mut(id) {
            Some(r) => r,
            None => return Ok(None),
        };

        let now = Utc::now();
        if record.delivery.is_leased_at(now) {
            if record.delivery.lease_holder.as_deref() != Some(worker_id) {
                return Ok(None);
            }
        }

        let expires_at = now + chrono::Duration::from_std(duration).unwrap_or_else(|_| chrono::Duration::seconds(30));
        record.delivery.lease_holder = Some(worker_id.to_string());
        record.delivery.lease_expires_at = Some(expires_at);
        record.delivery.version += 1;
        record.state = MessageState::Processing;

        Ok(Some(record.clone()))
    }

    async fn release_lease(&self, id: &MessageId, worker_id: &str) -> Result<(), DlqError> {
        let mut write = self.records.write();
        if let Some(record) = write.get_mut(id) {
            if record.delivery.lease_holder.as_deref() == Some(worker_id) {
                record.delivery.lease_holder = None;
                record.delivery.lease_expires_at = None;
                record.delivery.version += 1;
                record.state = MessageState::WaitingForRetry;
            }
        }
        Ok(())
    }

    async fn purge_expired(&self, max_age: Duration) -> Result<usize, DlqError> {
        let mut write = self.records.write();
        let now = Utc::now();
        let age_chrono = chrono::Duration::from_std(max_age).unwrap_or_else(|_| chrono::Duration::days(14));

        let expired_ids: Vec<MessageId> = write
            .iter()
            .filter(|(_, r)| {
                let ref_time = r.timestamps.dead_lettered_at.unwrap_or(r.timestamps.failed_at);
                now - ref_time > age_chrono
            })
            .map(|(id, _)| id.clone())
            .collect();

        let count = expired_ids.len();
        for id in expired_ids {
            write.remove(&id);
        }

        Ok(count)
    }
}
