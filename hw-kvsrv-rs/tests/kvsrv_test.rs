mod common;

use std::time::Duration;

use kvsrv::client::*;
use kvsrv::lock::Lock;

use common::kv::{TestKVMP, race_put_test};
use common::lock::{run_lock_clients_single, with_timeout};
use common::network_proxy::NetworkConfig;
use common::rand_string;

#[tokio::test(flavor = "multi_thread")]
async fn test_reliable_put() {
    let ts = TestKVMP::new(NetworkConfig::reliable()).await;
    let ck = ts.make_client();

    ck.put("k", "162", 0).await.unwrap();

    let (val, ver) = ck.get("k").await.unwrap();
    assert_eq!(val, "162");
    assert_eq!(ver, 1);

    assert_eq!(ck.put("k", "162", 0).await.unwrap_err(), KVError::Version);
    assert_eq!(ck.put("y", "162", 1).await.unwrap_err(), KVError::NoKey);

    assert_eq!(ck.get("y").await.unwrap_err(), KVError::NoKey);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reliable_multiple_keys() {
    let ts = TestKVMP::new(NetworkConfig::reliable()).await;
    let ck = ts.make_client();

    ck.put("a", "alpha", 0).await.unwrap();
    ck.put("b", "beta", 0).await.unwrap();
    ck.put("c", "gamma", 0).await.unwrap();

    let (val, ver) = ck.get("a").await.unwrap();
    assert_eq!((val.as_str(), ver), ("alpha", 1));
    let (val, ver) = ck.get("b").await.unwrap();
    assert_eq!((val.as_str(), ver), ("beta", 1));
    let (val, ver) = ck.get("c").await.unwrap();
    assert_eq!((val.as_str(), ver), ("gamma", 1));

    ck.put("a", "ALPHA", 1).await.unwrap();
    let (val, ver) = ck.get("a").await.unwrap();
    assert_eq!(val, "ALPHA");
    assert_eq!(ver, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reliable_version_progression() {
    let ts = TestKVMP::new(NetworkConfig::reliable()).await;
    let ck = ts.make_client();

    for i in 0u64..50 {
        let val = format!("v{}", i);
        ck.put("k", &val, i).await.unwrap();
        let (got_val, ver) = ck.get("k").await.unwrap();
        assert_eq!(got_val, val);
        assert_eq!(ver, i + 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reliable_empty_value() {
    let ts = TestKVMP::new(NetworkConfig::reliable()).await;
    let ck = ts.make_client();

    ck.put("k", "hello", 0).await.unwrap();
    ck.put("k", "", 1).await.unwrap();

    let (val, ver) = ck.get("k").await.unwrap();
    assert_eq!(val, "");
    assert_eq!(ver, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reliable_two_clients_conflict() {
    let ts = TestKVMP::new(NetworkConfig::reliable()).await;
    let ck1 = ts.make_client();
    let ck2 = ts.make_client();

    ck1.put("k", "from-1", 0).await.unwrap();

    assert_eq!(
        ck2.put("k", "from-2", 0).await.unwrap_err(),
        KVError::Version
    );

    let (_, ver) = ck2.get("k").await.unwrap();
    assert_eq!(ver, 1);
    ck2.put("k", "from-2", ver).await.unwrap();

    let (val, ver) = ck1.get("k").await.unwrap();
    assert_eq!(val, "from-2");
    assert_eq!(ver, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reliable_large_value() {
    let ts = TestKVMP::new(NetworkConfig::reliable()).await;
    let ck = ts.make_client();

    let big = "x".repeat(10_000);
    ck.put("big", &big, 0).await.unwrap();
    let (val, _) = ck.get("big").await.unwrap();
    assert_eq!(val, big);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reliable_many_keys() {
    let ts = TestKVMP::new(NetworkConfig::reliable()).await;
    let ck = ts.make_client();

    for i in 0..100 {
        let key = format!("key-{}", i);
        let val = format!("val-{}", i);
        ck.put(&key, &val, 0).await.unwrap();
    }
    for i in 0..100 {
        let key = format!("key-{}", i);
        let val = format!("val-{}", i);
        let (got, ver) = ck.get(&key).await.unwrap();
        assert_eq!(got, val);
        assert_eq!(ver, 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_2_clients() {
    race_put_test(2, Duration::from_secs(4), true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_5_clients() {
    race_put_test(5, Duration::from_secs(4), true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unreliable_net() {
    let ts = TestKVMP::new(NetworkConfig::unreliable()).await;
    let ck = ts.make_client();

    let mut retried = false;
    for try_num in 0u64..30 {
        for i in 0u64.. {
            let result = ck.put("k", &i.to_string(), try_num).await;
            match result {
                Err(KVError::Maybe) => {
                    retried = true;
                }
                Ok(()) => break,
                Err(KVError::Version) => {
                    assert!(i > 0, "put shouldn't have happened more than once");
                    break;
                }
                Err(e) => panic!("unexpected error: {:?}", e),
            }
        }
        let (val, ver) = ck.get("k").await.unwrap();
        assert_eq!(ver, try_num + 1, "wrong version");
        assert_eq!(val, "0", "wrong value");
    }
    assert!(retried, "Client::put never returned ErrMaybe");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unreliable_2_clients() {
    race_put_test(2, Duration::from_secs(4), false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unreliable_5_clients() {
    race_put_test(5, Duration::from_secs(8), false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_basic() {
    with_timeout(Duration::from_secs(30), async {
        let ts = TestKVMP::new(NetworkConfig::reliable()).await;
        let ck = ts.make_client();
        let name = rand_string(12);

        let mut lk = Lock::new(ck, &name);
        lk.acquire().await;
        lk.release().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_reacquire() {
    with_timeout(Duration::from_secs(30), async {
        let ts = TestKVMP::new(NetworkConfig::reliable()).await;
        let ck = ts.make_client();
        let name = rand_string(12);

        let mut lk = Lock::new(ck, &name);
        for _ in 0..20 {
            lk.acquire().await;
            lk.release().await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_nested() {
    with_timeout(Duration::from_secs(30), async {
        let ts = TestKVMP::new(NetworkConfig::reliable()).await;
        let ck = ts.make_client();
        let name1 = rand_string(12);
        let name2 = rand_string(12);

        let mut lk1 = Lock::new(ck.clone(), &name1);
        let mut lk2 = Lock::new(ck, &name2);

        lk1.acquire().await;
        lk2.acquire().await;
        lk2.release().await;
        lk1.release().await;

        lk2.acquire().await;
        lk1.acquire().await;
        lk1.release().await;
        lk1.acquire().await;
        lk2.release().await;
        lk1.release().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_1_client_reliable() {
    run_lock_clients_single(1, NetworkConfig::reliable(), Duration::from_secs(4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_2_clients_reliable() {
    run_lock_clients_single(2, NetworkConfig::reliable(), Duration::from_secs(4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_5_clients_reliable() {
    run_lock_clients_single(5, NetworkConfig::reliable(), Duration::from_secs(4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_1_client_unreliable() {
    run_lock_clients_single(1, NetworkConfig::unreliable(), Duration::from_secs(4)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lock_2_clients_unreliable() {
    run_lock_clients_single(2, NetworkConfig::unreliable(), Duration::from_secs(4)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_lock_32_clients_reliable() {
    run_lock_clients_single(32, NetworkConfig::reliable(), Duration::from_secs(6)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_lock_8_clients_unreliable() {
    run_lock_clients_single(8, NetworkConfig::unreliable(), Duration::from_secs(8)).await;
}
