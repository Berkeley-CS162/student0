mod common;

use kvsrv::lock::Lock;

use common::cluster::TestCluster;
use common::kv::TestKVMP;
use common::network_proxy::NetworkConfig;
use common::raft::{check_get, put_at_least_once};
use common::rand_string;

#[tokio::test(flavor = "multi_thread")]
async fn test_student_kv_basic() {
    let ts = TestKVMP::new(NetworkConfig::reliable()).await;
    let ck = ts.make_client();

    ck.put("hello", "world", 0).await.unwrap();

    let (val, ver) = ck.get("hello").await.unwrap();
    assert_eq!(val, "world");
    assert_eq!(ver, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_student_lock() {
    let ts = TestKVMP::new(NetworkConfig::reliable()).await;
    let ck = ts.make_client();
    let name = rand_string(12);

    let mut lk = Lock::new(ck, &name);
    lk.acquire().await;
    lk.release().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_student_raft() {
    let ts = TestCluster::new_raft(3).await;

    let (leader, term) = ts.wait_for_leader().await;
    assert!(leader < 3);
    assert!(term > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_student_kvraft() {
    let ts = TestCluster::new_kvraft(3).await;
    let ck = ts.make_client();

    put_at_least_once(&*ck, "k", "v1", 0).await;
    check_get(&*ck, "k", "v1", 1).await;
}
