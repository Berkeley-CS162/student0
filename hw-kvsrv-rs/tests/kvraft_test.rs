mod common;

use std::time::Duration;

use kvsrv::client::*;
use kvsrv::lock::Lock;

use common::lock::run_lock_clients;
use common::raft::{check_get, generic_test, put_at_least_once};
use common::{NetworkConfig, TestCluster, rand_string};

async fn run_lock_clients_kvraft(n_clients: usize, config: NetworkConfig, duration: Duration) {
    let ts = TestCluster::new_with(5, config).await;
    let clients = (0..n_clients).map(|_| ts.make_client()).collect();
    run_lock_clients(clients, duration).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_kvraft() {
    let ts = TestCluster::new(3).await;
    let ck = ts.make_client();

    assert_eq!(ck.get("missing").await.unwrap_err(), KVError::NoKey);

    let ver = put_at_least_once(&*ck, "k", "v1", 0).await;
    assert_eq!(ver, 1);

    check_get(&*ck, "k", "v1", 1).await;

    let ver = put_at_least_once(&*ck, "k", "v2", 1).await;
    assert_eq!(ver, 2);

    check_get(&*ck, "k", "v2", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_kvraft() {
    let ts = TestCluster::new(5).await;

    let mut handles = Vec::new();
    for cli in 0..5u64 {
        let ck = ts.make_client();
        handles.push(tokio::spawn(async move {
            let key = format!("key-{}", cli);
            let mut ver = put_at_least_once(&*ck, &key, "v0", 0).await;

            for i in 1..=5u64 {
                let val = format!("v{}", i);
                ver = put_at_least_once(&*ck, &key, &val, ver).await;
            }

            let (val, got_ver) = ck.get(&key).await.unwrap();
            assert_eq!(val, "v5");
            assert_eq!(got_ver, ver);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leader_failure_kvraft() {
    let ts = TestCluster::new(3).await;
    let ck = ts.make_client();

    put_at_least_once(&*ck, "k", "v1", 0).await;
    check_get(&*ck, "k", "v1", 1).await;

    let leader = ts.find_leader().await.expect("no leader found");
    ts.kill(leader);

    put_at_least_once(&*ck, "k", "v2", 1).await;
    check_get(&*ck, "k", "v2", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_persist_kvraft() {
    let ts = TestCluster::new(5).await;
    let ck = ts.make_client();

    put_at_least_once(&*ck, "k", "v1", 0).await;
    check_get(&*ck, "k", "v1", 1).await;

    ts.kill_all();
    ts.restart_all().await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    check_get(&*ck, "k", "v1", 1).await;
    put_at_least_once(&*ck, "k", "v2", 1).await;
    check_get(&*ck, "k", "v2", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_kvraft() {
    let ts = TestCluster::new(5).await;
    let ck = ts.make_client();

    put_at_least_once(&*ck, "k", "v1", 0).await;

    ts.partition(&[0, 1, 2], &[3, 4]);

    let (ck_maj, ck_maj_id) = ts.make_client_with_id();
    for j in 3..5 {
        ts.proxies[j].block_client(ck_maj_id);
    }

    put_at_least_once(&*ck_maj, "k", "v2", 1).await;
    check_get(&*ck_maj, "k", "v2", 2).await;

    ts.heal();
    tokio::time::sleep(Duration::from_secs(2)).await;

    check_get(&*ck, "k", "v2", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unreliable_kvraft() {
    let ts = TestCluster::new_with(5, NetworkConfig::unreliable()).await;
    let ck = ts.make_client();

    let ver = put_at_least_once(&*ck, "k", "v1", 0).await;
    check_get(&*ck, "k", "v1", ver).await;

    let ver = put_at_least_once(&*ck, "k", "v2", ver).await;
    check_get(&*ck, "k", "v2", ver).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_many_partitions_one_client_kvraft() {
    generic_test(5, 1, true, false, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_many_partitions_many_clients_kvraft() {
    generic_test(5, 5, true, false, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_persist_concurrent_kvraft() {
    generic_test(5, 5, true, true, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_persist_partition_kvraft() {
    generic_test(5, 3, true, true, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_basic_kvraft() {
    let ts = TestCluster::new(3).await;
    let ck = ts.make_client();
    let name = rand_string(12);

    let mut lk = Lock::new(ck, &name);
    lk.acquire().await;
    lk.release().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_reacquire_kvraft() {
    let ts = TestCluster::new(3).await;
    let ck = ts.make_client();
    let name = rand_string(12);

    let mut lk = Lock::new(ck, &name);
    for _ in 0..10 {
        lk.acquire().await;
        lk.release().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_mutual_exclusion_kvraft() {
    let ts = TestCluster::new(5).await;
    let clients = (0..3).map(|_| ts.make_client()).collect();
    run_lock_clients(clients, Duration::from_secs(6)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_1_client_reliable_kvraft() {
    run_lock_clients_kvraft(1, NetworkConfig::reliable(), Duration::from_secs(4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_2_clients_reliable_kvraft() {
    run_lock_clients_kvraft(2, NetworkConfig::reliable(), Duration::from_secs(4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_5_clients_reliable_kvraft() {
    run_lock_clients_kvraft(5, NetworkConfig::reliable(), Duration::from_secs(4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_1_client_unreliable_kvraft() {
    run_lock_clients_kvraft(1, NetworkConfig::unreliable(), Duration::from_secs(4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_2_clients_unreliable_kvraft() {
    run_lock_clients_kvraft(2, NetworkConfig::unreliable(), Duration::from_secs(4)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_lock_32_clients_reliable_kvraft() {
    run_lock_clients_kvraft(32, NetworkConfig::reliable(), Duration::from_secs(6)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_lock_8_clients_unreliable_kvraft() {
    run_lock_clients_kvraft(8, NetworkConfig::unreliable(), Duration::from_secs(8)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_with_leader_failure_kvraft() {
    let ts = TestCluster::new(5).await;
    let ck = ts.make_client();
    let name = rand_string(12);

    let mut lk = Lock::new(ck.clone(), &name);

    lk.acquire().await;

    tokio::time::sleep(Duration::from_millis(1000)).await;
    let leader = ts.find_leader().await.expect("no leader found before kill");
    ts.kill(leader);

    lk.release().await;

    lk.acquire().await;
    lk.release().await;

    let ck2 = ts.make_client();
    let mut lk2 = Lock::new(ck2, &name);
    lk2.acquire().await;
    lk2.release().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_with_crash_kvraft() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    let ts = TestCluster::new(5).await;
    let done = Arc::new(AtomicBool::new(false));
    let chaos_handle = ts.spawn_chaos(done.clone(), 3);

    let clients = (0..2).map(|_| ts.make_client()).collect();
    run_lock_clients(clients, Duration::from_secs(10)).await;
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    chaos_handle.await.unwrap();
}
