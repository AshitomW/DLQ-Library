//! Broker-independent message transport adapter trait.

use async_trait::async_trait;

use crate::error::TransportError;
use crate::message::{Message, MessageId};

#[async_trait]
pub trait MessageTransport: Send + Sync {
    async fn receive(&self) -> Result<Option<Message>, TransportError>;
    async fn ack(&self, id: &MessageId) -> Result<(), TransportError>;
    async fn nack(&self, id: &MessageId, requeue: bool) -> Result<(), TransportError>;
    async fn publish(&self, destination: &str, message: &Message) -> Result<(), TransportError>;
}
