use crate::client::*;
use crate::rpc::RpcClient;
use async_trait::async_trait;

pub struct Client {
    endpoint: RpcClient,
}

impl Client {
    pub fn new(endpoint: RpcClient) -> Self {
        Client { endpoint }
    }
}

#[async_trait]
impl KvClient for Client {
    async fn get(&self, key: &str) -> Result<(String, Version), KVError> {
        // TODO: call get and decode the reply.
        todo!()
    }

    async fn put(&self, key: &str, value: &str, version: Version) -> Result<(), KVError> {
        todo!()
    }
}
