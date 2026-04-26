#![allow(dead_code, unused_imports)]

pub mod cluster;
pub mod kv;
pub mod lock;
pub mod network_proxy;
pub mod raft;

use std::sync::atomic::{AtomicUsize, Ordering};

use rand::Rng;

pub use cluster::TestCluster;
pub use network_proxy::NetworkConfig;

static POOL_INIT: std::sync::Once = std::sync::Once::new();

pub fn ensure_pool_ready() {
    POOL_INIT.call_once(|| {
        kvsrv_lib::assert_runtime!("test");
    });
}

pub fn next_client_id() -> usize {
    static COUNTER: AtomicUsize = AtomicUsize::new(100);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub fn rand_string(n: usize) -> String {
    let mut rng = rand::rng();
    (0..n)
        .map(|_| rng.random_range(b'a'..=b'z') as char)
        .collect()
}
