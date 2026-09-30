//! Failure classification abstraction and chain of responsibility implementation.

use std::error::Error;
use std::fmt;
use serde::{Deserialize, Serialize};

use crate::error::ProcessingError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureReason {
    pub code: String,
    pub message: String,
    pub category: String,
}

impl FailureReason {
    pub fn new(code: impl Into<String>, message: impl Into<String>, category: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            category: category.into(),
        }
    }
}

impl fmt::Display for FailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.category, self.code, self.message)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropReason {
    pub code: String,
    pub message: String,
}

impl DropReason {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureClassification {
    Retryable { reason: FailureReason },
    Permanent { reason: FailureReason },
    Drop { reason: DropReason },
}

impl FailureClassification {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable { .. })
    }

    pub fn code(&self) -> &str {
        match self {
            Self::Retryable { reason } | Self::Permanent { reason } => &reason.code,
            Self::Drop { reason } => &reason.code,
        }
    }
}

pub trait FailureClassifier: Send + Sync {
    fn classify(&self, error: &(dyn Error + 'static)) -> Option<FailureClassification>;
}

pub struct ClassifierChain {
    classifiers: Vec<Box<dyn FailureClassifier>>,
    default_classification: FailureClassification,
}

impl ClassifierChain {
    pub fn new(default_classification: FailureClassification) -> Self {
        Self {
            classifiers: Vec::new(),
            default_classification,
        }
    }

    pub fn with_classifier<C: FailureClassifier + 'static>(mut self, classifier: C) -> Self {
        self.classifiers.push(Box::new(classifier));
        self
    }

    pub fn classify(&self, error: &(dyn Error + 'static)) -> FailureClassification {
        for classifier in &self.classifiers {
            if let Some(classification) = classifier.classify(error) {
                return classification;
            }
        }
        self.default_classification.clone()
    }
}

impl Default for ClassifierChain {
    fn default() -> Self {
        Self::new(FailureClassification::Permanent {
            reason: FailureReason::new(
                "UNKNOWN_ERROR",
                "Unclassified failure deemed permanent",
                "unclassified",
            ),
        })
        .with_classifier(ProcessingErrorClassifier)
        .with_classifier(TimeoutClassifier)
    }
}

pub struct ProcessingErrorClassifier;

impl FailureClassifier for ProcessingErrorClassifier {
    fn classify(&self, error: &(dyn Error + 'static)) -> Option<FailureClassification> {
        if let Some(pe) = error.downcast_ref::<ProcessingError>() {
            let retryable = pe.is_retryable.unwrap_or_else(|| {
                pe.code.contains("TIMEOUT")
                    || pe.code.contains("RATE_LIMIT")
                    || pe.code.contains("THROTTLED")
                    || pe.code.contains("UNAVAILABLE")
            });

            let reason = FailureReason::new(pe.code.clone(), pe.message.clone(), pe.error_type.clone());
            if retryable {
                return Some(FailureClassification::Retryable { reason });
            } else {
                return Some(FailureClassification::Permanent { reason });
            }
        }
        None
    }
}

pub struct TimeoutClassifier;

impl FailureClassifier for TimeoutClassifier {
    fn classify(&self, error: &(dyn Error + 'static)) -> Option<FailureClassification> {
        let err_str = error.to_string().to_lowercase();
        if err_str.contains("timeout") || err_str.contains("timed out") || err_str.contains("deadline exceeded") {
            Some(FailureClassification::Retryable {
                reason: FailureReason::new("TIMEOUT", error.to_string(), "transient_network"),
            })
        } else {
            None
        }
    }
}

pub struct FnClassifier<F>(pub F)
where
    F: Fn(&(dyn Error + 'static)) -> Option<FailureClassification> + Send + Sync;

impl<F> FailureClassifier for FnClassifier<F>
where
    F: Fn(&(dyn Error + 'static)) -> Option<FailureClassification> + Send + Sync,
{
    fn classify(&self, error: &(dyn Error + 'static)) -> Option<FailureClassification> {
        (self.0)(error)
    }
}
