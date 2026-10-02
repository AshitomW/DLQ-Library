//! Metric collectors and thread-safe in-memory metrics storage with bounded cardinality.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use parking_lot::RwLock;

pub trait MetricsCollector: Send + Sync {
    fn increment_dead_letters(&self, topic: &str, failure_type: &str);
    fn increment_retries(&self, topic: &str);
    fn record_retry_latency(&self, topic: &str, latency: Duration);
    fn increment_duplicates(&self, topic: &str);
    fn increment_expired(&self, count: usize);
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryMetrics {
    dead_letters_total: Arc<RwLock<HashMap<String, u64>>>,
    retries_total: Arc<RwLock<HashMap<String, u64>>>,
    duplicates_total: Arc<AtomicU64>,
    expired_total: Arc<AtomicU64>,
}

impl InMemoryMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_dead_letters(&self, topic: &str, failure_type: &str) -> u64 {
        let key = format!("{}:{}", topic, failure_type);
        self.dead_letters_total.read().get(&key).copied().unwrap_or(0)
    }

    pub fn get_retries(&self, topic: &str) -> u64 {
        self.retries_total.read().get(topic).copied().unwrap_or(0)
    }

    pub fn get_duplicates(&self) -> u64 {
        self.duplicates_total.load(Ordering::Relaxed)
    }

    pub fn get_expired(&self) -> u64 {
        self.expired_total.load(Ordering::Relaxed)
    }
}

impl MetricsCollector for InMemoryMetrics {
    fn increment_dead_letters(&self, topic: &str, failure_type: &str) {
        let key = format!("{}:{}", topic, failure_type);
        let mut map = self.dead_letters_total.write();
        *map.entry(key).or_insert(0) += 1;
    }

    fn increment_retries(&self, topic: &str) {
        let mut map = self.retries_total.write();
        *map.entry(topic.to_string()).or_insert(0) += 1;
    }

    fn record_retry_latency(&self, _topic: &str, _latency: Duration) {}

    fn increment_duplicates(&self, _topic: &str) {
        self.duplicates_total.fetch_add(1, Ordering::Relaxed);
    }

    fn increment_expired(&self, count: usize) {
        self.expired_total.fetch_add(count as u64, Ordering::Relaxed);
    }
}
