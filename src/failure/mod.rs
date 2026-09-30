//! Failure diagnostic metadata and error classification chain.

pub mod classifier;
pub mod metadata;

pub use classifier::{
    ClassifierChain, DropReason, FailureClassification, FailureClassifier, FailureReason,
    FnClassifier, ProcessingErrorClassifier, TimeoutClassifier,
};
pub use metadata::FailureMetadata;
