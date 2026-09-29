//! State machine and lifecycle event transitions for message processing and dead letters.

use std::fmt;
use std::time::Duration;
use serde::{Deserialize, Serialize};

use crate::error::StateError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MessageState {
    Received,
    Processing,
    WaitingForRetry,
    DeadLettered,
    Completed,
    Expired,
    Deleted,
}

impl fmt::Display for MessageState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Received => write!(f, "RECEIVED"),
            Self::Processing => write!(f, "PROCESSING"),
            Self::WaitingForRetry => write!(f, "WAITING_FOR_RETRY"),
            Self::DeadLettered => write!(f, "DEAD_LETTERED"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Expired => write!(f, "EXPIRED"),
            Self::Deleted => write!(f, "DELETED"),
        }
    }
}

impl MessageState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Expired | Self::Deleted)
    }

    pub fn can_retry(&self) -> bool {
        matches!(self, Self::WaitingForRetry | Self::DeadLettered)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    StartProcessing,
    ProcessSuccess,
    ScheduleRetry { delay: Duration },
    RetryAvailable,
    RejectToDlq { reason: String },
    ManualRetry,
    Expire,
    Delete,
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StartProcessing => write!(f, "StartProcessing"),
            Self::ProcessSuccess => write!(f, "ProcessSuccess"),
            Self::ScheduleRetry { delay } => write!(f, "ScheduleRetry({:?})", delay),
            Self::RetryAvailable => write!(f, "RetryAvailable"),
            Self::RejectToDlq { reason } => write!(f, "RejectToDlq({})", reason),
            Self::ManualRetry => write!(f, "ManualRetry"),
            Self::Expire => write!(f, "Expire"),
            Self::Delete => write!(f, "Delete"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageStateMachine {
    current_state: MessageState,
}

impl MessageStateMachine {
    pub fn new(initial: MessageState) -> Self {
        Self {
            current_state: initial,
        }
    }

    pub fn current(&self) -> MessageState {
        self.current_state
    }

    pub fn transition(&mut self, event: Event) -> Result<MessageState, StateError> {
        let from = self.current_state;
        let to = match (&from, &event) {
            (MessageState::Received, Event::StartProcessing) => MessageState::Processing,
            (MessageState::Processing, Event::ProcessSuccess) => MessageState::Completed,
            (MessageState::Processing, Event::ScheduleRetry { .. }) => MessageState::WaitingForRetry,
            (MessageState::Processing, Event::RejectToDlq { .. }) => MessageState::DeadLettered,
            (MessageState::WaitingForRetry, Event::RetryAvailable) => MessageState::Received,
            (MessageState::WaitingForRetry, Event::StartProcessing) => MessageState::Processing,
            (MessageState::DeadLettered, Event::ManualRetry) => MessageState::Processing,
            (MessageState::DeadLettered, Event::StartProcessing) => MessageState::Processing,
            (s, Event::Expire) if !s.is_terminal() => MessageState::Expired,
            (_, Event::Delete) => MessageState::Deleted,
            _ => {
                return Err(StateError::IllegalTransition {
                    from: from.to_string(),
                    to: format!("(via {})", event),
                    event: event.to_string(),
                });
            }
        };

        self.current_state = to;
        Ok(to)
    }
}
