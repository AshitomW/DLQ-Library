use dlq_lib::error::ProcessingError;
use dlq_lib::failure::{
    ClassifierChain, FailureClassification, FailureClassifier, FailureReason, FnClassifier,
    TimeoutClassifier,
};

#[test]
fn test_processing_error_classification() {
    let chain = ClassifierChain::default();

    // Timeout error is retryable
    let timeout_err = ProcessingError::new("DatabaseError", "DB_TIMEOUT", "Query took too long");
    let res1 = chain.classify(&timeout_err);
    assert!(res1.is_retryable());
    assert_eq!(res1.code(), "DB_TIMEOUT");

    // Schema violation is permanent
    let schema_err = ProcessingError::new("ValidationError", "SCHEMA_INVALID", "Missing required field id");
    let res2 = chain.classify(&schema_err);
    assert!(!res2.is_retryable());
    assert_eq!(res2.code(), "SCHEMA_INVALID");
}

#[test]
fn test_timeout_string_classifier() {
    let classifier = TimeoutClassifier;
    let custom_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "socket timed out waiting for bytes");
    let res = classifier.classify(&custom_err);
    assert!(res.is_some());
    assert!(res.unwrap().is_retryable());
}

#[test]
fn test_classifier_chain_precedence_and_fallback() {
    let fallback = FailureClassification::Permanent {
        reason: FailureReason::new("CUSTOM_FALLBACK", "Fallback error", "test"),
    };

    let chain = ClassifierChain::new(fallback)
        .with_classifier(FnClassifier(|err| {
            if err.to_string().contains("special_transient") {
                Some(FailureClassification::Retryable {
                    reason: FailureReason::new("TRANSIENT", "Special transient error", "custom"),
                })
            } else {
                None
            }
        }));

    let match_err = std::io::Error::other("special_transient condition occurred");
    assert!(chain.classify(&match_err).is_retryable());

    let no_match_err = std::io::Error::other("unrecognized anomaly");
    let res = chain.classify(&no_match_err);
    assert!(!res.is_retryable());
    assert_eq!(res.code(), "CUSTOM_FALLBACK");
}
