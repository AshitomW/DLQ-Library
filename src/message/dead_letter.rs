//! Dead-letter container encapsulating failed messages, diagnostic metadata, and lifecycle states.

use serde::{Deserialize, Serialize};

use crate::failure::FailureMetadata;
use crate::message::message::{Message, MessageId};
use crate::message::metadata::{DeliveryMetadata, SourceMetadata, TimestampMetadata};
use crate::state::MessageState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub id: MessageId,
    pub original_message: Message,
    pub source: SourceMetadata,
    pub failure: FailureMetadata,
    pub delivery: DeliveryMetadata,
    pub timestamps: TimestampMetadata,
    pub tenant_id: Option<String>,
    pub state: MessageState,
}

impl DeadLetter {
    pub fn new(
        id: impl Into<MessageId>,
        original_message: Message,
        source: SourceMetadata,
        failure: FailureMetadata,
        delivery: DeliveryMetadata,
        timestamps: TimestampMetadata,
    ) -> Self {
        Self {
            id: id.into(),
            original_message,
            source,
            failure,
            delivery,
            timestamps,
            tenant_id: None,
            state: MessageState::DeadLettered,
        }
    }

    pub fn create(
        original_message: Message,
        source: SourceMetadata,
        failure: FailureMetadata,
        tenant_id: Option<String>,
    ) -> Self {
        let attempt = failure.attempt;
        let mut timestamps = TimestampMetadata::now();
        timestamps.dead_lettered_at = Some(timestamps.failed_at);

        Self {
            id: MessageId::new_v4(),
            original_message,
            source,
            failure,
            delivery: DeliveryMetadata::new(attempt),
            timestamps,
            tenant_id,
            state: MessageState::DeadLettered,
        }
    }

    pub fn original_id(&self) -> &MessageId {
        &self.original_message.id
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }

    pub fn with_state(mut self, state: MessageState) -> Self {
        self.state = state;
        self
    }
}
