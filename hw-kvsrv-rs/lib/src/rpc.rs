use serde::{Serialize, de::DeserializeOwned};

#[async_trait::async_trait]
pub trait Server: Send + Sync {
    async fn dispatch(&self, name: &str, args: String) -> String;
}

pub struct RpcClient {
    proxy_url: String,
    client_id: usize,
    http: reqwest::Client,
}

impl RpcClient {
    pub fn new(proxy_url: String, client_id: usize) -> Self {
        RpcClient {
            proxy_url,
            client_id,
            http: reqwest::Client::new(),
        }
    }

    /// Return None on network or decode failure.
    pub async fn call<A: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        args: &A,
    ) -> Option<R> {
        let url = format!("{}/{}", self.proxy_url, method);
        let resp = self
            .http
            .post(&url)
            .header("X-Client-Id", self.client_id.to_string())
            .body(serde_json::to_string(args).unwrap())
            .send()
            .await
            .ok()?;

        if resp.status() != reqwest::StatusCode::OK {
            return None;
        }

        let body = resp.text().await.ok()?;
        serde_json::from_str(&body).ok()
    }
}
