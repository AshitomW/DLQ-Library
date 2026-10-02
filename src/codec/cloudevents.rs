//! CloudEvents 1.0 envelope conversion for standardized dead-letter payloads.

use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::CodecError;
use crate::message::DeadLetter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudEventEnvelope {
    pub specversion: String,
    pub id: String,
    pub source: String,
    pub r#type: String,
    pub time: Option<DateTime<Utc>>,
    pub datacontenttype: Option<String>,
    pub data: serde_json::Value,
    #[serde(flatten)]
    pub extensions: HashMap<String, serde_json::Value>,
}

impl CloudEventEnvelope {
    pub fn from_dead_letter(record: &DeadLetter) -> Result<Self, CodecError> {
        let data = serde_json::to_value(record)
            .map_err(|e| CodecError::CloudEvents(format!("Failed to serialize data payload: {}", e)))?;

        let mut extensions = HashMap::new();
        extensions.insert("originalid".to_string(), serde_json::Value::String(record.original_id().to_string()));
        extensions.insert("failuretype".to_string(), serde_json::Value::String(record.failure.failure_type.clone()));
        extensions.insert("failurecode".to_string(), serde_json::Value::String(record.failure.code.clone()));
        extensions.insert("attempt".to_string(), serde_json::Value::Number(record.delivery.attempt_count.into()));

        if let Some(ref tid) = record.tenant_id {
            extensions.insert("tenantid".to_string(), serde_json::Value::String(tid.clone()));
        }

        let source = format!("{}/{}", record.source.broker, record.source.topic_or_queue);

        Ok(Self {
            specversion: "1.0".to_string(),
            id: record.id.to_string(),
            source,
            r#type: "org.dlqlib.deadletter.v1".to_string(),
            time: record.timestamps.dead_lettered_at.or(Some(record.timestamps.failed_at)),
            datacontenttype: Some("application/json".to_string()),
            data,
            extensions,
        })
    }

    pub fn to_dead_letter(&self) -> Result<DeadLetter, CodecError> {
        serde_json::from_value::<DeadLetter>(self.data.clone())
            .map_err(|e| CodecError::CloudEvents(format!("Failed to parse DeadLetter from CloudEvent data: {}", e)))
    }
}
