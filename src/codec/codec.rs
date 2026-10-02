//! Codec trait for versioned serialization and deserialization of dead letters.

use crate::error::CodecError;
use crate::message::DeadLetter;

pub trait Codec: Send + Sync {
    fn schema_version(&self) -> u32;
    fn encode(&self, message: &DeadLetter) -> Result<Vec<u8>, CodecError>;
    fn decode(&self, bytes: &[u8]) -> Result<DeadLetter, CodecError>;
}
