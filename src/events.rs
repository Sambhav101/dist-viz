use crate::raft::{Message, NodeState};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum Event {
    StateChanged {
        id: u64,
        state: NodeState,
        term: u64,
    },
    MessageSent {
        from: u64,
        to: u64,
        msg: Message,
    },
}
