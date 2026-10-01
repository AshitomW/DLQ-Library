//! Message domain models, metadata, dead letter envelopes, and redaction.

pub mod dead_letter;
pub mod message;
pub mod metadata;
pub mod sanitize;

pub use dead_letter::DeadLetter;
pub use message::{Message, MessageId};
pub use metadata::{DeliveryMetadata, SourceMetadata, TimestampMetadata};
pub use sanitize::{is_sensitive_header, redact_sensitive_headers, safe_payload_summary, sanitize_headers};
