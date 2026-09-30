//! Machine-readable diagnostic metadata detailing error types, codes, and attempt counts.

use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::ProcessingError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureMetadata {
    pub failure_type: String,
    pub code: String,
    pub message: String,
    pub stacktrace: Option<String>,
    pub component: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub attempt: u32,
    pub retryable: bool,
    pub details: HashMap<String, String>,
}

impl FailureMetadata {
    pub fn new(
        failure_type: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
        attempt: u32,
        retryable: bool,
    ) -> Self {
        Self {
            failure_type: failure_type.into(),
            code: code.into(),
            message: message.into(),
            stacktrace: None,
            component: None,
            timestamp: Utc::now(),
            attempt,
            retryable,
            details: HashMap::new(),
        }
    }

    pub fn from_processing_error(err: &ProcessingError, attempt: u32, retryable: bool) -> Self {
        Self {
            failure_type: err.error_type.clone(),
            code: err.code.clone(),
            message: err.message.clone(),
            stacktrace: err.stacktrace.clone(),
            component: err.component.clone(),
            timestamp: Utc::now(),
            attempt,
            retryable: err.is_retryable.unwrap_or(retryable),
            details: HashMap::new(),
        }
    }

    pub fn with_stacktrace(mut self, trace: impl Into<String>) -> Self {
        self.stacktrace = Some(trace.into());
        self
    }

    pub fn with_component(mut self, component: impl Into<String>) -> Self {
        self.component = Some(component.into());
        self
    }

    pub fn with_detail(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.details.insert(key.into(), val.into());
        self
    }
}
