use dlq_lib::codec::{CloudEventEnvelope, Codec, JsonCodec};
use dlq_lib::failure::FailureMetadata;
use dlq_lib::message::{DeadLetter, Message, MessageId, SourceMetadata};

fn create_sample_dead_letter() -> DeadLetter {
    let msg = Message::new(MessageId::new("msg-100"), b"{\"order_id\": 42}".to_vec())
        .with_header("x-correlation-id", "corr-999")
        .with_content_type("application/json");

    let source = SourceMetadata::new("kafka", "orders-topic").with_partition(2).with_offset(4500);

    let failure = FailureMetadata::new("OrderProcessingError", "INSUFFICIENT_FUNDS", "Account balance below zero", 3, false);

    DeadLetter::create(msg, source, failure, Some("tenant-corp".into()))
}

#[test]
fn test_json_codec_versioning_and_roundtrip() {
    let codec = JsonCodec::new();
    let original = create_sample_dead_letter();

    let bytes = codec.encode(&original).expect("encode should succeed");
    let decoded = codec.decode(&bytes).expect("decode should succeed");

    assert_eq!(decoded.id, original.id);
    assert_eq!(decoded.original_id(), original.original_id());
    assert_eq!(decoded.failure.code, "INSUFFICIENT_FUNDS");
    assert_eq!(decoded.tenant_id.as_deref(), Some("tenant-corp"));
}

#[test]
fn test_unsupported_schema_version_rejection() {
    let codec = JsonCodec::new();
    // Simulate future schema version 99
    let json_bytes = br#"{"schema_version": 99, "record": null}"#;
    let res = codec.decode(json_bytes);
    assert!(res.is_err());
}

#[test]
fn test_cloudevents_conversion() {
    let dl = create_sample_dead_letter();
    let cloud_event = CloudEventEnvelope::from_dead_letter(&dl).expect("CloudEvent creation");

    assert_eq!(cloud_event.specversion, "1.0");
    // Verify DLQ ID is used as event ID, not the original message ID
    assert_eq!(cloud_event.id, dl.id.to_string());
    assert_ne!(cloud_event.id, dl.original_id().to_string());
    assert_eq!(cloud_event.extensions.get("originalid").unwrap(), &serde_json::Value::String(dl.original_id().to_string()));

    let roundtrip_dl = cloud_event.to_dead_letter().expect("parse from CloudEvent");
    assert_eq!(roundtrip_dl.id, dl.id);
    assert_eq!(roundtrip_dl.original_message.payload, dl.original_message.payload);
}
