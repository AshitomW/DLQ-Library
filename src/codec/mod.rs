//! Serialization codecs and standardized CloudEvents envelope adapters.

pub mod cloudevents;
pub mod codec;
pub mod json;

pub use cloudevents::CloudEventEnvelope;
pub use codec::Codec;
pub use json::{JsonCodec, VersionedEnvelope, CURRENT_JSON_SCHEMA_VERSION};
