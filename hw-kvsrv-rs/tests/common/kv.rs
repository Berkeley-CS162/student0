use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use kvsrv::client::*;
use kvsrv::rpc::RpcClient;

use super::cluster::{Child, WorkerSpec, kill_and_wait, spawn_worker};
use super::network_proxy::{NetworkConfig, ServerProxy};

pub struct TestKVMP {
    proxy: Arc<ServerProxy>,
    child: Child,
}

impl TestKVMP {
    pub async fn new(config: NetworkConfig) -> Self {
        super::ensure_pool_ready();
        let proxy = ServerProxy::new(config).await;

        let child = tokio::task::spawn_blocking(|| spawn_worker(WorkerSpec::Kv))
            .await
            .unwrap();
        proxy.set_child_url(Some(child.url.clone()));

        TestKVMP { proxy, child }
    }

    pub fn make_client(&self) -> Arc<dyn KvClient> {
        let id = super::next_client_id();
        let endpoint = RpcClient::new(self.proxy.url(), id);
        Arc::new(kvsrv::kv_single::Client::new(endpoint))
    }
}

impl Drop for TestKVMP {
    fn drop(&mut self) {
        kill_and_wait(self.child.pid);
    }
}

pub async fn race_put_test(n_clients: usize, duration: Duration, reliable: bool) {
    let ts = TestKVMP::new(if reliable {
        NetworkConfig::reliable()
    } else {
        NetworkConfig::unreliable()
    })
    .await;
    let done = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..n_clients)
        .map(|i| {
            let ck = ts.make_client();
            let done = done.clone();
            tokio::spawn(async move {
                let mut nok = 0u64;
                let mut attempts = 0u64;
                while !done.load(Ordering::SeqCst) {
                    attempts += 1;
                    let version = match ck.get("k").await {
                        Ok((_, ver)) => ver,
                        Err(KVError::NoKey) => 0,
                        Err(e) => panic!("unexpected: {:?}", e),
                    };
                    let val = format!("client-{}-{}", i, nok);
                    match ck.put("k", &val, version).await {
                        Ok(()) => nok += 1,
                        Err(KVError::Version) => {}
                        Err(KVError::Maybe) if !reliable => {}
                        other => panic!("unexpected error: {:?}", other),
                    }
                    tokio::task::yield_now().await;
                }
                (nok, attempts)
            })
        })
        .collect();

    tokio::time::sleep(duration).await;
    done.store(true, Ordering::SeqCst);

    let mut total_ok = 0u64;
    for (i, h) in handles.into_iter().enumerate() {
        let (count, attempts) = h.await.unwrap();
        total_ok += count;
        if !reliable {
            assert!(attempts > 0, "client {i} did no work on unreliable network");
        }
    }

    if reliable {
        let (_, ver) = ts.make_client().get("k").await.unwrap();
        assert_eq!(
            ver, total_ok,
            "version {} != total successful puts {}",
            ver, total_ok
        );
        assert!(total_ok > 0, "no puts succeeded");
    } else {
        assert!(total_ok > 0, "no puts succeeded on unreliable network");
    }
}
