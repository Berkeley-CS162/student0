use std::collections::HashSet;
use std::future::IntoFuture;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::{HeaderMap, StatusCode};
use axum::{Router, extract::State, routing::post};
use rand::Rng;
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub request_drop_rate: f64,
    pub reply_drop_rate: f64,
    pub min_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl NetworkConfig {
    pub fn reliable() -> Self {
        NetworkConfig {
            request_drop_rate: 0.0,
            reply_drop_rate: 0.0,
            min_delay_ms: 0,
            max_delay_ms: 0,
        }
    }

    pub fn unreliable() -> Self {
        NetworkConfig {
            request_drop_rate: 0.5,
            reply_drop_rate: 0.5,
            min_delay_ms: 1,
            max_delay_ms: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RpcFate {
    Deliver,
    DropRequest,
    DropReply,
}

pub struct ServerProxy {
    child_url: Mutex<Option<String>>,
    listen_port: u16,
    config: Mutex<NetworkConfig>,
    blocked_clients: Mutex<HashSet<usize>>,
    http: reqwest::Client,
}

type ProxyState = Arc<ServerProxy>;

impl ServerProxy {
    pub async fn new(config: NetworkConfig) -> Arc<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let proxy = Arc::new(ServerProxy {
            child_url: Mutex::new(None),
            listen_port: port,
            config: Mutex::new(config),
            blocked_clients: Mutex::new(HashSet::new()),
            http: reqwest::Client::new(),
        });

        let app = Router::new()
            .route("/{method}", post(handle_proxy_rpc))
            .with_state(Arc::clone(&proxy));

        tokio::spawn(axum::serve(listener, app).into_future());

        proxy
    }

    pub fn set_child_url(&self, url: Option<String>) {
        *self.child_url.lock().unwrap() = url;
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.listen_port)
    }

    pub fn set_config(&self, config: NetworkConfig) {
        *self.config.lock().unwrap() = config;
    }

    pub fn set_reliable(&self) {
        self.set_config(NetworkConfig::reliable());
    }

    pub fn block_client(&self, client_id: usize) {
        self.blocked_clients.lock().unwrap().insert(client_id);
    }

    pub fn unblock_client(&self, client_id: usize) {
        self.blocked_clients.lock().unwrap().remove(&client_id);
    }

    fn is_client_blocked(&self, client_id: usize) -> bool {
        self.blocked_clients.lock().unwrap().contains(&client_id)
    }

    fn resolve_fate(&self) -> RpcFate {
        let (req_drop, reply_drop) = {
            let cfg = self.config.lock().unwrap();
            (cfg.request_drop_rate, cfg.reply_drop_rate)
        };
        let mut rng = rand::rng();
        if rng.random_bool(req_drop.clamp(0.0, 1.0)) {
            return RpcFate::DropRequest;
        }
        if rng.random_bool(reply_drop.clamp(0.0, 1.0)) {
            return RpcFate::DropReply;
        }
        RpcFate::Deliver
    }

    async fn random_delay(&self) {
        let (min_ms, max_ms) = {
            let cfg = self.config.lock().unwrap();
            (cfg.min_delay_ms, cfg.max_delay_ms)
        };
        let ms = if min_ms >= max_ms {
            min_ms
        } else {
            rand::rng().random_range(min_ms..=max_ms)
        };
        if ms > 0 {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }
}

async fn handle_proxy_rpc(
    State(proxy): State<ProxyState>,
    headers: HeaderMap,
    axum::extract::Path(method): axum::extract::Path<String>,
    body: String,
) -> (StatusCode, String) {
    let client_id: Option<usize> = headers
        .get("X-Client-Id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok());

    if let Some(id) = client_id {
        if proxy.is_client_blocked(id) {
            return (StatusCode::SERVICE_UNAVAILABLE, String::new());
        }
    }

    let fate = proxy.resolve_fate();
    if fate == RpcFate::DropRequest {
        return (StatusCode::SERVICE_UNAVAILABLE, String::new());
    }

    proxy.random_delay().await;

    let Some(child_url) = proxy.child_url.lock().unwrap().clone() else {
        return (StatusCode::SERVICE_UNAVAILABLE, String::new());
    };

    let forward_url = format!("{}/{}", child_url, method);
    let Ok(resp) = proxy.http.post(&forward_url).body(body).send().await else {
        return (StatusCode::SERVICE_UNAVAILABLE, String::new());
    };
    let Ok(resp_body) = resp.text().await else {
        return (StatusCode::SERVICE_UNAVAILABLE, String::new());
    };

    proxy.random_delay().await;

    if let Some(id) = client_id {
        if proxy.is_client_blocked(id) {
            return (StatusCode::SERVICE_UNAVAILABLE, String::new());
        }
    }

    if fate == RpcFate::DropReply {
        return (StatusCode::SERVICE_UNAVAILABLE, String::new());
    }

    (StatusCode::OK, resp_body)
}
