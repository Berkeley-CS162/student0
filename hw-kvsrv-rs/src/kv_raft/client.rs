use crate::client::*;
use crate::rpc::RpcClient;
use async_trait::async_trait;
use std::sync::Arc;

pub struct Client {
    servers: Vec<Arc<RpcClient>>,
    // TODO: cache leader and client state.
}

impl Client {
    pub fn new(servers: Vec<Arc<RpcClient>>) -> Self {
        todo!()
    }
}

#[async_trait]
impl KvClient for Client {
    async fn get(&self, key: &str) -> Result<(String, Version), KVError> {
        todo!()
    }

    async fn put(&self, key: &str, value: &str, version: Version) -> Result<(), KVError> {
        todo!()
    }
}
