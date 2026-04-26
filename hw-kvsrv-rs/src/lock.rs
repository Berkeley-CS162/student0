// Part 2: Distributed Lock with KV Server
// Part 4: Distributed Lock with Raft

use std::sync::Arc;

use crate::client::*;

pub struct Lock {
    client: Arc<dyn KvClient>,
    lockname: String,
    // TODO: add any other lock client state.
}

impl Lock {
    pub fn new(client: Arc<dyn KvClient>, lockname: &str) -> Self {
        // TODO: initialize lock state.
        todo!()
    }

    pub async fn acquire(&mut self) {
        // TODO: acquire the lock through the KV client.
        todo!()
    }

    pub async fn release(&mut self) {
        // TODO: release the lock if this client owns it.
        todo!()
    }
}
