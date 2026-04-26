use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use kvsrv::client::*;
use kvsrv::lock::Lock;

use super::kv::TestKVMP;
use super::network_proxy::NetworkConfig;
use super::rand_string;

async fn lock_check_cycle(
    lk: &mut Lock,
    ck: &dyn KvClient,
    check_key: &str,
    me: usize,
    in_cs: &AtomicUsize,
) {
    lk.acquire().await;

    let prev = in_cs.fetch_add(1, Ordering::SeqCst);
    assert_eq!(
        prev, 0,
        "{me}: entered CS while {prev} other(s) held the lock"
    );

    let (val, ver) = ck.get(check_key).await.expect(&format!("{me}: get failed"));
    assert!(val.is_empty(), "{me}: two clients acquired lock, val={val}");

    let result = ck.put(check_key, &me.to_string(), ver).await;
    assert!(
        matches!(result, Ok(()) | Err(KVError::Maybe)),
        "{me}: put failed {:?}",
        result
    );

    tokio::time::sleep(Duration::from_millis(20)).await;

    let mid = in_cs.load(Ordering::SeqCst);
    assert_eq!(
        mid, 1,
        "{me}: another client entered CS during sleep (count={mid})"
    );

    let result = ck.put(check_key, "", ver + 1).await;
    assert!(
        matches!(result, Ok(()) | Err(KVError::Maybe)),
        "{me}: put failed {:?}",
        result
    );

    in_cs.fetch_sub(1, Ordering::SeqCst);
    lk.release().await;
}

async fn one_client(
    me: usize,
    ck: Arc<dyn KvClient>,
    done: Arc<AtomicBool>,
    lock_name: String,
    check_key: String,
    in_cs: Arc<AtomicUsize>,
    started: Arc<AtomicUsize>,
) -> u64 {
    let mut lk = Lock::new(ck.clone(), &lock_name);
    let _ = ck.put(&check_key, "", 0).await;
    started.fetch_add(1, Ordering::SeqCst);

    let mut count = 0u64;
    while !done.load(Ordering::SeqCst) {
        count += 1;
        lock_check_cycle(&mut lk, &*ck, &check_key, me, &in_cs).await;
    }
    count
}

pub async fn run_lock_clients(clients: Vec<Arc<dyn KvClient>>, duration: Duration) {
    let budget = duration + Duration::from_secs(180);
    with_timeout(budget, async move {
        let done = Arc::new(AtomicBool::new(false));
        let in_cs = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let lock_name = rand_string(12);
        let check_key = format!("mutex-check-{}", rand_string(8));
        let n_clients = clients.len();

        let handles: Vec<_> = clients
            .into_iter()
            .enumerate()
            .map(|(me, ck)| {
                let done = done.clone();
                let in_cs = in_cs.clone();
                let started = started.clone();
                let ln = lock_name.clone();
                let ck_key = check_key.clone();
                tokio::spawn(
                    async move { one_client(me, ck, done, ln, ck_key, in_cs, started).await },
                )
            })
            .collect();

        while started.load(Ordering::SeqCst) < n_clients {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        tokio::time::sleep(duration).await;
        done.store(true, Ordering::SeqCst);

        for (i, h) in handles.into_iter().enumerate() {
            let count = h.await.unwrap();
            assert!(count > 0, "client {i} did no work");
        }
    })
    .await;
}

pub async fn with_timeout<F: std::future::Future<Output = ()>>(budget: Duration, fut: F) {
    if tokio::time::timeout(budget, fut).await.is_err() {
        panic!("deadlocked: did not complete within {budget:?}");
    }
}

pub async fn run_lock_clients_single(n_clients: usize, config: NetworkConfig, duration: Duration) {
    let ts = TestKVMP::new(config).await;
    let clients: Vec<Arc<dyn KvClient>> = (0..n_clients).map(|_| ts.make_client()).collect();
    run_lock_clients(clients, duration).await;
}
