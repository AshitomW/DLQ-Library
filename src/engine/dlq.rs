//! DLQ engine coordinating failure handling, retries, persistence failover, and command execution.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use parking_lot::Mutex;
use tracing::{error, info, warn};

use crate::engine::batch::BatchRetrier;
use crate::engine::command::{CommandResult, DlqCommand};
use crate::engine::config::{DlqConfig, DlqUnavailablePolicy};
use crate::error::DlqError;
use crate::failure::{ClassifierChain, FailureClassification, FailureMetadata};
use crate::message::{DeadLetter, Message, MessageId, SourceMetadata};
use crate::retry::{RetryContext, RetryDecision, RetryPolicy};
use crate::state::MessageState;
use crate::storage::store::DeadLetterStore;
use crate::transport::idempotency::IdempotencyStore;
use crate::transport::transport::MessageTransport;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessingOutcome {
    RetryScheduled {
        delay: Duration,
        next_attempt: u32,
    },
    DeadLettered(MessageId),
    Dropped(String),
}

pub struct DlqEngine {
    store: Arc<dyn DeadLetterStore>,
    transport: Arc<dyn MessageTransport>,
    retry_policy: Arc<dyn RetryPolicy>,
    classifier: Arc<ClassifierChain>,
    idempotency: Option<Arc<dyn IdempotencyStore>>,
    config: DlqConfig,
    local_buffer: Arc<Mutex<Vec<DeadLetter>>>,
}

impl DlqEngine {
    pub fn new(
        store: Arc<dyn DeadLetterStore>,
        transport: Arc<dyn MessageTransport>,
        retry_policy: Arc<dyn RetryPolicy>,
        classifier: Arc<ClassifierChain>,
        config: DlqConfig,
    ) -> Result<Self, DlqError> {
        config.validate()?;
        Ok(Self {
            store,
            transport,
            retry_policy,
            classifier,
            idempotency: None,
            config,
            local_buffer: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub fn with_idempotency(mut self, idempotency: Arc<dyn IdempotencyStore>) -> Self {
        self.idempotency = Some(idempotency);
        self
    }

    pub fn store(&self) -> &Arc<dyn DeadLetterStore> {
        &self.store
    }

    pub fn transport(&self) -> &Arc<dyn MessageTransport> {
        &self.transport
    }

    pub fn buffered_records_count(&self) -> usize {
        self.local_buffer.lock().len()
    }

    pub async fn flush_local_buffer(&self) -> Result<usize, DlqError> {
        let mut buffer = self.local_buffer.lock();
        let mut flushed = 0;
        let records = std::mem::take(&mut *buffer);

        for record in records {
            match self.store.put(record.clone()).await {
                Ok(()) => flushed += 1,
                Err(e) => {
                    error!("Failed to flush record {} from local buffer: {}", record.id, e);
                    buffer.push(record);
                }
            }
        }

        Ok(flushed)
    }

    pub async fn handle_failure(
        &self,
        message: Message,
        source: SourceMetadata,
        error: &(dyn Error + 'static),
        attempt: u32,
        tenant_id: Option<String>,
    ) -> Result<ProcessingOutcome, DlqError> {
        let classification = self.classifier.classify(error);

        match classification {
            FailureClassification::Drop { reason } => {
                info!("Message {} dropped: {}", message.id, reason.message);
                let _ = self.transport.ack(&message.id).await;
                Ok(ProcessingOutcome::Dropped(reason.message))
            }
            FailureClassification::Retryable { reason } => {
                let failure_meta = FailureMetadata::new(
                    reason.category,
                    reason.code,
                    reason.message,
                    attempt,
                    true,
                );

                let retry_ctx = RetryContext::new(message.id.clone(), attempt, failure_meta.clone());
                let decision = self.retry_policy.next_attempt(&retry_ctx);

                match decision {
                    RetryDecision::Retry { delay, next_attempt } => {
                        info!(
                            "Scheduling retry for message {} (attempt {} in {:?})",
                            message.id, next_attempt, delay
                        );
                        let _ = self.transport.nack(&message.id, false).await;
                        Ok(ProcessingOutcome::RetryScheduled { delay, next_attempt })
                    }
                    RetryDecision::DeadLetter { reason: dlq_reason } => {
                        let dead_letter = DeadLetter::create(
                            message,
                            source,
                            FailureMetadata::new(
                                dlq_reason.category,
                                dlq_reason.code,
                                dlq_reason.message,
                                attempt,
                                false,
                            ),
                            tenant_id,
                        );

                        self.persist_dead_letter_with_failover(dead_letter.clone()).await?;
                        let _ = self.transport.ack(dead_letter.original_id()).await;
                        Ok(ProcessingOutcome::DeadLettered(dead_letter.id))
                    }
                    RetryDecision::Drop { reason: drop_reason } => {
                        let _ = self.transport.ack(&message.id).await;
                        Ok(ProcessingOutcome::Dropped(drop_reason.message))
                    }
                }
            }
            FailureClassification::Permanent { reason } => {
                let dead_letter = DeadLetter::create(
                    message,
                    source,
                    FailureMetadata::new(
                        reason.category,
                        reason.code,
                        reason.message,
                        attempt,
                        false,
                    ),
                    tenant_id,
                );

                self.persist_dead_letter_with_failover(dead_letter.clone()).await?;
                let _ = self.transport.ack(dead_letter.original_id()).await;
                Ok(ProcessingOutcome::DeadLettered(dead_letter.id))
            }
        }
    }

    async fn persist_dead_letter_with_failover(&self, record: DeadLetter) -> Result<(), DlqError> {
        let initial_res = self.store.put(record.clone()).await;
        if initial_res.is_ok() {
            return Ok(());
        }

        let err = initial_res.unwrap_err();
        warn!("Primary DLQ storage write failed: {}. Invoking failover policy: {:?}", err, self.config.dlq_unavailable_policy);

        match self.config.dlq_unavailable_policy {
            DlqUnavailablePolicy::RetryWrite => {
                for attempt in 1..=3 {
                    tokio::time::sleep(Duration::from_millis(50 * attempt)).await;
                    if self.store.put(record.clone()).await.is_ok() {
                        return Ok(());
                    }
                }
                Err(DlqError::Unavailable("Exceeded DLQ storage retry attempts".into()))
            }
            DlqUnavailablePolicy::LocalBuffer => {
                self.local_buffer.lock().push(record);
                Ok(())
            }
            DlqUnavailablePolicy::FailConsumer => {
                Err(DlqError::Unavailable(format!("DLQ storage unavailable, failing consumer: {}", err)))
            }
            DlqUnavailablePolicy::Block => {
                loop {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    if self.store.put(record.clone()).await.is_ok() {
                        return Ok(());
                    }
                }
            }
            DlqUnavailablePolicy::Drop => {
                warn!("Message {} dropped due to DlqUnavailablePolicy::Drop", record.id);
                Ok(())
            }
        }
    }

    pub async fn execute_command(&self, command: DlqCommand) -> Result<CommandResult, DlqError> {
        match command {
            DlqCommand::Retry(id) => {
                let record = self
                    .store
                    .get(&id)
                    .await?
                    .ok_or_else(|| DlqError::NotFound(id.clone()))?;

                self.store
                    .update_state(&id, MessageState::DeadLettered, MessageState::Processing)
                    .await?;

                if let Err(e) = self
                    .transport
                    .publish(&record.source.topic_or_queue, &record.original_message)
                    .await
                {
                    let _ = self
                        .store
                        .update_state(&id, MessageState::Processing, MessageState::DeadLettered)
                        .await;
                    return Err(DlqError::Transport(e));
                }

                self.store
                    .update_state(&id, MessageState::Processing, MessageState::Completed)
                    .await?;

                Ok(CommandResult::Retried(id))
            }
            DlqCommand::RetryBatch(query) => {
                let retrier = BatchRetrier::new(
                    self.store.clone(),
                    self.transport.clone(),
                    self.config.concurrency.max_retry_concurrency,
                    self.config.concurrency.batch_size,
                );
                let res = retrier.execute(&query, None).await?;
                Ok(CommandResult::BatchRetried {
                    total_matched: res.total_matched,
                    succeeded: res.succeeded,
                    failed: res.failed,
                })
            }
            DlqCommand::Delete(id) => {
                self.store.delete(&id).await?;
                Ok(CommandResult::Deleted(id))
            }
            DlqCommand::PurgeExpired => {
                let max_age = self
                    .config
                    .retention
                    .max_age
                    .unwrap_or_else(|| Duration::from_secs(14 * 86400));
                let count = self.store.purge_expired(max_age).await?;
                Ok(CommandResult::Purged(count))
            }
            DlqCommand::Inspect(id) => {
                let record = self.store.get(&id).await?;
                Ok(CommandResult::Inspected(record))
            }
        }
    }
}
