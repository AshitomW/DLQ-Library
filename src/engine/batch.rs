//! Bounded concurrency executor for batch retry operations.

use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use crate::error::DlqError;
use crate::state::MessageState;
use crate::storage::query::DlqQuery;
use crate::storage::store::DeadLetterStore;
use crate::transport::transport::MessageTransport;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchRetryResult {
    pub total_matched: usize,
    pub succeeded: usize,
    pub failed: usize,
}

pub struct BatchRetrier {
    store: Arc<dyn DeadLetterStore>,
    transport: Arc<dyn MessageTransport>,
    max_concurrency: usize,
    batch_size: usize,
}

impl BatchRetrier {
    pub fn new(
        store: Arc<dyn DeadLetterStore>,
        transport: Arc<dyn MessageTransport>,
        max_concurrency: usize,
        batch_size: usize,
    ) -> Self {
        Self {
            store,
            transport,
            max_concurrency: max_concurrency.max(1),
            batch_size: batch_size.max(1),
        }
    }

    pub async fn execute(
        &self,
        query: &DlqQuery,
        destination_override: Option<&str>,
    ) -> Result<BatchRetryResult, DlqError> {
        let semaphore = Arc::new(Semaphore::new(self.max_concurrency));
        let mut total_succeeded = 0;
        let mut total_failed = 0;
        let mut offset = 0;

        let total_count = self.store.count(query).await?;
        if total_count == 0 {
            return Ok(BatchRetryResult {
                total_matched: 0,
                succeeded: 0,
                failed: 0,
            });
        }

        while offset < total_count {
            let mut page_query = query.clone();
            page_query.offset = offset;
            page_query.limit = self.batch_size;

            let page = self.store.list(&page_query).await?;
            if page.items.is_empty() {
                break;
            }

            let mut tasks = Vec::new();

            for record in page.items {
                let sem = semaphore.clone();
                let store = self.store.clone();
                let transport = self.transport.clone();
                let dest = destination_override
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| record.source.topic_or_queue.clone());

                tasks.push(tokio::spawn(async move {
                    let _permit = sem.acquire().await.map_err(|e| DlqError::Unavailable(e.to_string()))?;

                    if let Err(e) = store
                        .update_state(&record.id, MessageState::DeadLettered, MessageState::Processing)
                        .await
                    {
                        warn!("Could not transition record {} to processing: {}", record.id, e);
                        return Err(e);
                    }

                    if let Err(e) = transport.publish(&dest, &record.original_message).await {
                        error!("Failed to republish record {}: {}", record.id, e);
                        let _ = store
                            .update_state(&record.id, MessageState::Processing, MessageState::DeadLettered)
                            .await;
                        return Err(DlqError::Transport(e));
                    }

                    if let Err(e) = store
                        .update_state(&record.id, MessageState::Processing, MessageState::Completed)
                        .await
                    {
                        warn!("Failed to mark record {} as completed: {}", record.id, e);
                    }

                    info!("Successfully retried dead letter record {}", record.id);
                    Ok(())
                }));
            }

            for task in tasks {
                match task.await {
                    Ok(Ok(())) => total_succeeded += 1,
                    _ => total_failed += 1,
                }
            }

            offset += self.batch_size;
        }

        Ok(BatchRetryResult {
            total_matched: total_count,
            succeeded: total_succeeded,
            failed: total_failed,
        })
    }
}
