use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{Router, extract::State, routing::post};
use kvsrv::kv_raft::raft::Raft;
use kvsrv::kv_raft::{Persister, RSM};
use kvsrv::kv_single::KVServer;
use kvsrv::rpc::{RpcClient, Server};
use tokio::net::TcpListener;

fn server_router(server: Arc<dyn Server>) -> Router {
    async fn handle_rpc(
        State(server): State<Arc<dyn Server>>,
        axum::extract::Path(method): axum::extract::Path<String>,
        body: String,
    ) -> String {
        server.dispatch(&method, body).await
    }

    Router::new()
        .route("/{method}", post(handle_rpc))
        .with_state(server)
}

fn raft_test_routes(raft: &Arc<Raft>) -> Router {
    Router::new()
        .route(
            "/_test/get_state",
            post(|State(r): State<Arc<Raft>>| async move {
                serde_json::to_string(&r.get_state().await).unwrap()
            }),
        )
        .route(
            "/_test/get_committed",
            post(|State(r): State<Arc<Raft>>, body: String| async move {
                let index: usize = serde_json::from_str(&body).unwrap();
                serde_json::to_string(&r.get_committed(index).await).unwrap()
            }),
        )
        .route(
            "/_test/submit",
            post(|State(r): State<Arc<Raft>>, body: String| async move {
                let cmd: String = serde_json::from_str(&body).unwrap();
                serde_json::to_string(&r.submit(&cmd).await).unwrap()
            }),
        )
        .with_state(raft.clone())
}

fn build_raft_node(
    me: usize,
    persist_path: PathBuf,
    peer_urls: Vec<String>,
    kv_layer: bool,
) -> Router {
    let persister = Arc::new(Persister::new(persist_path));
    let peers = peer_urls
        .into_iter()
        .map(|url| Arc::new(RpcClient::new(url, me)))
        .collect();
    let raft = Raft::new(me, peers, persister);
    let rpc_router = if kv_layer {
        server_router(RSM::new(raft.clone()))
    } else {
        server_router(raft.clone())
    };
    rpc_router.merge(raft_test_routes(&raft))
}

fn parse_raft_args(args: &[String], kv_layer: bool) -> Router {
    if args.len() < 3 {
        panic!("usage: test_worker raft|kvraft <me> <persist_path> <peer_urls...>");
    }
    let me = args[1].parse().expect("invalid peer index");
    let persist_path = PathBuf::from(&args[2]);
    let peer_urls = args[3..].to_vec();
    build_raft_node(me, persist_path, peer_urls, kv_layer)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let app = match args.get(1).map(String::as_str) {
        Some("kv") => server_router(Arc::new(KVServer::new())),
        Some("raft") => parse_raft_args(&args[1..], false),
        Some("kvraft") => parse_raft_args(&args[1..], true),
        _ => panic!(
            "usage: test_worker kv | raft <me> <persist_path> <peer_urls...> | kvraft <me> <persist_path> <peer_urls...>"
        ),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    println!("{port}");
    std::io::stdout().flush().unwrap();

    axum::serve(listener, app).await.unwrap();
}
