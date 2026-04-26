use thiserror::Error;

/// Version 0 means the key is missing.
pub type Version = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Error)]
pub enum KVError {
    #[error("ErrNoKey")]
    NoKey,
    #[error("ErrVersion")]
    Version,
    #[error("ErrMaybe")]
    Maybe,
}

#[async_trait::async_trait]
pub trait KvClient: Send + Sync {
    async fn get(&self, key: &str) -> Result<(String, Version), KVError>;
    async fn put(&self, key: &str, value: &str, version: Version) -> Result<(), KVError>;
}
