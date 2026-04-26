use crate::kv_raft::raft::Raft;
use crate::rpc::*;
use async_trait::async_trait;
use std::sync::Arc;

pub struct RSM {
    raft: Arc<Raft>,
}

impl RSM {
    pub fn new(raft: Arc<Raft>) -> Arc<Self> {
        Arc::new(Self { raft })
    }

    pub fn raft(&self) -> &Arc<Raft> {
        &self.raft
    }
}

#[async_trait]
impl Server for RSM {
    async fn dispatch(&self, name: &str, args: String) -> String {
        self.raft.dispatch(name, args).await
    }
}
