//! Query filters, search criteria, and paginated result containers.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::state::MessageState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqQuery {
    pub tenant_id: Option<String>,
    pub state: Option<MessageState>,
    pub failure_type: Option<String>,
    pub failure_code: Option<String>,
    pub source_broker: Option<String>,
    pub topic_or_queue: Option<String>,
    pub min_attempts: Option<u32>,
    pub failed_after: Option<DateTime<Utc>>,
    pub failed_before: Option<DateTime<Utc>>,
    pub limit: usize,
    pub offset: usize,
}

impl Default for DlqQuery {
    fn default() -> Self {
        Self {
            tenant_id: None,
            state: None,
            failure_type: None,
            failure_code: None,
            source_broker: None,
            topic_or_queue: None,
            min_attempts: None,
            failed_after: None,
            failed_before: None,
            limit: 50,
            offset: 0,
        }
    }
}

impl DlqQuery {
    pub const MAX_LIMIT: usize = 1000;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }

    pub fn with_state(mut self, state: MessageState) -> Self {
        self.state = Some(state);
        self
    }

    pub fn with_failure_type(mut self, ftype: impl Into<String>) -> Self {
        self.failure_type = Some(ftype.into());
        self
    }

    pub fn with_failure_code(mut self, code: impl Into<String>) -> Self {
        self.failure_code = Some(code.into());
        self
    }

    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic_or_queue = Some(topic.into());
        self
    }

    pub fn with_min_attempts(mut self, attempts: u32) -> Self {
        self.min_attempts = Some(attempts);
        self
    }

    pub fn with_failed_after(mut self, dt: DateTime<Utc>) -> Self {
        self.failed_after = Some(dt);
        self
    }

    pub fn with_failed_before(mut self, dt: DateTime<Utc>) -> Self {
        self.failed_before = Some(dt);
        self
    }

    pub fn with_pagination(mut self, offset: usize, limit: usize) -> Self {
        self.offset = offset;
        self.limit = limit.min(Self::MAX_LIMIT).max(1);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
    pub has_more: bool,
}

impl<T> Page<T> {
    pub fn new(items: Vec<T>, total: usize, offset: usize, limit: usize) -> Self {
        let has_more = offset + items.len() < total;
        Self {
            items,
            total,
            offset,
            limit,
            has_more,
        }
    }

    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            total: 0,
            offset: 0,
            limit: 50,
            has_more: false,
        }
    }
}
