use crate::persister::Persister;
use crate::rpc::{RpcClient, Server};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// TODO: define RPC request/reply types.

// TODO: define persistent state.

pub struct Raft {
    me: usize,
    peers: Vec<Arc<RpcClient>>,
    persister: Arc<Persister>,
    // TODO: add persistent and volatile Raft state.
}

impl Raft {
    /// Create a new Raft peer.
    pub fn new(me: usize, peers: Vec<Arc<RpcClient>>, persister: Arc<Persister>) -> Arc<Self> {
        // TODO: start background tasks after state initialization.
        Arc::new(Self {
            me,
            peers,
            persister,
        })
    }

    /// Return (current_term, is_leader).
    pub async fn get_state(&self) -> (u64, bool) {
        todo!()
    }

    /// Submit a command to the log.
    pub async fn submit(&self, command: &str) -> Option<(usize, u64)> {
        todo!()
    }

    /// Return the committed command at `index`.
    pub async fn get_committed(&self, index: usize) -> Option<String> {
        todo!()
    }

    // TODO: define Raft RPC handlers.
}

#[async_trait::async_trait]
impl Server for Raft {
    async fn dispatch(&self, name: &str, args: String) -> String {
        // TODO: dispatch Raft RPCs.
        todo!()
    }
}
