//! Message lifecycle states, transitions, and concurrency leases.

pub mod lease;
pub mod machine;

pub use lease::Lease;
pub use machine::{Event, MessageState, MessageStateMachine};
