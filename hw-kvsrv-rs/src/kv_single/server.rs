use crate::client::*;
use crate::rpc::*;
use serde::{Deserialize, Serialize};

// TODO: define RPC request/reply types.

#[derive(Serialize, Deserialize)]
pub struct GetArgs {}

#[derive(Serialize, Deserialize)]
pub struct GetReply {}

#[derive(Serialize, Deserialize)]
pub struct PutArgs {}

#[derive(Serialize, Deserialize)]
pub struct PutReply {}

pub struct KVServer {
    // TODO: add KV map and synchronization.
}

impl KVServer {
    pub fn new() -> Self {
        todo!()
    }

    fn get(&self, args: &GetArgs) -> GetReply {
        todo!()
    }

    fn put(&self, args: &PutArgs) -> PutReply {
        todo!()
    }
}

#[async_trait::async_trait]
impl Server for KVServer {
    async fn dispatch(&self, name: &str, args: String) -> String {
        match name {
            "get" => {
                serde_json::to_string(&self.get(&serde_json::from_str(&args).unwrap())).unwrap()
            }
            "put" => {
                serde_json::to_string(&self.put(&serde_json::from_str(&args).unwrap())).unwrap()
            }
            _ => panic!("unknown request name"),
        }
    }
}
