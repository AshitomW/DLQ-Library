//! In-memory message transport with configurable failure injection controls.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use async_trait::async_trait;
use parking_lot::Mutex;

use crate::error::TransportError;
use crate::message::{Message, MessageId};
use crate::transport::transport::MessageTransport;

#[derive(Debug, Clone)]
pub struct InMemoryTransport {
    broker_name: String,
    incoming: Arc<Mutex<VecDeque<Message>>>,
    destinations: Arc<Mutex<HashMap<String, Vec<Message>>>>,
    acknowledged: Arc<Mutex<Vec<MessageId>>>,
    nacked: Arc<Mutex<Vec<(MessageId, bool)>>>,
    simulate_ack_failure: Arc<AtomicBool>,
    simulate_offline: Arc<AtomicBool>,
}

impl Default for InMemoryTransport {
    fn default() -> Self {
        Self::new("in-memory-broker")
    }
}

impl InMemoryTransport {
    pub fn new(broker_name: impl Into<String>) -> Self {
        Self {
            broker_name: broker_name.into(),
            incoming: Arc::new(Mutex::new(VecDeque::new())),
            destinations: Arc::new(Mutex::new(HashMap::new())),
            acknowledged: Arc::new(Mutex::new(Vec::new())),
            nacked: Arc::new(Mutex::new(Vec::new())),
            simulate_ack_failure: Arc::new(AtomicBool::new(false)),
            simulate_offline: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    pub fn enqueue(&self, msg: Message) {
        self.incoming.lock().push_back(msg);
    }

    pub fn set_simulate_ack_failure(&self, enable: bool) {
        self.simulate_ack_failure.store(enable, Ordering::SeqCst);
    }

    pub fn set_simulate_offline(&self, enable: bool) {
        self.simulate_offline.store(enable, Ordering::SeqCst);
    }

    pub fn is_acknowledged(&self, id: &MessageId) -> bool {
        self.acknowledged.lock().contains(id)
    }

    pub fn published_to(&self, destination: &str) -> Vec<Message> {
        self.destinations.lock().get(destination).cloned().unwrap_or_default()
    }
}

#[async_trait]
impl MessageTransport for InMemoryTransport {
    async fn receive(&self) -> Result<Option<Message>, TransportError> {
        if self.simulate_offline.load(Ordering::SeqCst) {
            return Err(TransportError::Connection("Simulated broker offline".into()));
        }
        let mut queue = self.incoming.lock();
        Ok(queue.pop_front())
    }

    async fn ack(&self, id: &MessageId) -> Result<(), TransportError> {
        if self.simulate_offline.load(Ordering::SeqCst) {
            return Err(TransportError::Connection("Simulated broker offline during ACK".into()));
        }
        if self.simulate_ack_failure.load(Ordering::SeqCst) {
            return Err(TransportError::Ack {
                id: id.clone(),
                reason: "Simulated ACK transport failure".into(),
            });
        }
        self.acknowledged.lock().push(id.clone());
        Ok(())
    }

    async fn nack(&self, id: &MessageId, requeue: bool) -> Result<(), TransportError> {
        if self.simulate_offline.load(Ordering::SeqCst) {
            return Err(TransportError::Connection("Simulated broker offline during NACK".into()));
        }
        self.nacked.lock().push((id.clone(), requeue));
        Ok(())
    }

    async fn publish(&self, destination: &str, message: &Message) -> Result<(), TransportError> {
        if self.simulate_offline.load(Ordering::SeqCst) {
            return Err(TransportError::Connection("Simulated broker offline during publish".into()));
        }
        let mut dests = self.destinations.lock();
        dests.entry(destination.to_string()).or_default().push(message.clone());
        Ok(())
    }
}
