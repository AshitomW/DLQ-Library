//! JSON codec implementation with explicit schema versioning.

use serde::{Deserialize, Serialize};

use crate::codec::codec::Codec;
use crate::error::CodecError;
use crate::message::DeadLetter;

pub const CURRENT_JSON_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedEnvelope {
    pub schema_version: u32,
    pub record: DeadLetter,
}

#[derive(Debug, Default, Clone)]
pub struct JsonCodec {
    target_version: u32,
}

impl JsonCodec {
    pub fn new() -> Self {
        Self {
            target_version: CURRENT_JSON_SCHEMA_VERSION,
        }
    }
}

impl Codec for JsonCodec {
    fn schema_version(&self) -> u32 {
        self.target_version
    }

    fn encode(&self, message: &DeadLetter) -> Result<Vec<u8>, CodecError> {
        let envelope = VersionedEnvelope {
            schema_version: self.target_version,
            record: message.clone(),
        };

        serde_json::to_vec(&envelope).map_err(|e| CodecError::Encode(e.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<DeadLetter, CodecError> {
        if let Ok(envelope) = serde_json::from_slice::<VersionedEnvelope>(bytes) {
            if envelope.schema_version > self.target_version {
                return Err(CodecError::UnsupportedSchemaVersion {
                    version: envelope.schema_version,
                    supported: self.target_version,
                });
            }
            return Ok(envelope.record);
        }

        serde_json::from_slice::<DeadLetter>(bytes)
            .map_err(|e| CodecError::Decode(format!("Failed to parse dead letter JSON: {}", e)))
    }
}
