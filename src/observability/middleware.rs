//! Middleware decorators for transport metrics and sanitized correlation logging.

use std::sync::Arc;
use async_trait::async_trait;
use tracing::{debug, info};

use crate::error::TransportError;
use crate::message::sanitize::{safe_payload_summary, sanitize_headers};
use crate::message::{Message, MessageId};
use crate::observability::metrics::MetricsCollector;
use crate::transport::transport::MessageTransport;

pub struct MetricsTransport<T: MessageTransport> {
    inner: T,
    metrics: Arc<dyn MetricsCollector>,
}

impl<T: MessageTransport> MetricsTransport<T> {
    pub fn new(inner: T, metrics: Arc<dyn MetricsCollector>) -> Self {
        Self { inner, metrics }
    }
}

#[async_trait]
impl<T: MessageTransport> MessageTransport for MetricsTransport<T> {
    async fn receive(&self) -> Result<Option<Message>, TransportError> {
        self.inner.receive().await
    }

    async fn ack(&self, id: &MessageId) -> Result<(), TransportError> {
        self.inner.ack(id).await
    }

    async fn nack(&self, id: &MessageId, requeue: bool) -> Result<(), TransportError> {
        self.inner.nack(id, requeue).await
    }

    async fn publish(&self, destination: &str, message: &Message) -> Result<(), TransportError> {
        let res = self.inner.publish(destination, message).await;
        if res.is_ok() {
            self.metrics.increment_retries(destination);
        }
        res
    }
}

pub struct LoggingTransport<T: MessageTransport> {
    inner: T,
}

impl<T: MessageTransport> LoggingTransport<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<T: MessageTransport> MessageTransport for LoggingTransport<T> {
    async fn receive(&self) -> Result<Option<Message>, TransportError> {
        let res = self.inner.receive().await;
        if let Ok(Some(ref msg)) = res {
            let sanitized = sanitize_headers(&msg.headers);
            let preview = safe_payload_summary(&msg.payload, 64);
            debug!(
                message_id = %msg.id,
                correlation_id = ?msg.correlation_id,
                headers = ?sanitized,
                payload_preview = %preview,
                "Transport received message"
            );
        }
        res
    }

    async fn ack(&self, id: &MessageId) -> Result<(), TransportError> {
        debug!(message_id = %id, "Transport ACK message");
        self.inner.ack(id).await
    }

    async fn nack(&self, id: &MessageId, requeue: bool) -> Result<(), TransportError> {
        debug!(message_id = %id, requeue = requeue, "Transport NACK message");
        self.inner.nack(id, requeue).await
    }

    async fn publish(&self, destination: &str, message: &Message) -> Result<(), TransportError> {
        info!(
            message_id = %message.id,
            destination = destination,
            correlation_id = ?message.correlation_id,
            "Transport publishing message"
        );
        self.inner.publish(destination, message).await
    }
}
