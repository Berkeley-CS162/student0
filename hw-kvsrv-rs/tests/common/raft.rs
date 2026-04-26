use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rand::Rng;

use kvsrv::client::*;
use kvsrv::kv_raft::Client;
use kvsrv::rpc::RpcClient;

use super::NetworkConfig;
use super::cluster::TestCluster;

const LEADER_WAIT_ATTEMPTS: usize = 15;
const LEADER_WAIT_INTERVAL: Duration = Duration::from_secs(1);
const ONE_TIMEOUT: Duration = Duration::from_secs(30);
const ONE_COMMIT_POLLS: usize = 100;
const ONE_COMMIT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const GENERIC_CLIENT_DRAIN_TIMEOUT: Duration = Duration::from_secs(90);

#[must_use]
pub fn test_watchdog(secs: u64) -> impl Drop {
    let done = Arc::new(AtomicBool::new(false));
    let d2 = done.clone();
    std::thread::spawn(move || {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(secs) {
            if d2.load(Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        eprintln!("\n!!! raft test exceeded {secs}s timeout; aborting !!!");
        std::process::exit(101);
    });

    struct Guard(Arc<AtomicBool>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    Guard(done)
}

impl TestCluster {
    pub fn make_client(&self) -> Arc<dyn KvClient> {
        self.make_client_with_id().0
    }

    pub fn make_client_with_id(&self) -> (Arc<dyn KvClient>, usize) {
        let id = super::next_client_id();
        let servers: Vec<Arc<RpcClient>> = (0..self.n)
            .map(|j| Arc::new(RpcClient::new(self.proxies[j].url(), id)))
            .collect();
        (Arc::new(Client::new(servers)), id)
    }

    pub async fn get_state_on(&self, i: usize) -> Option<(u64, bool)> {
        self.child_endpoint(i)?.call("_test/get_state", &()).await
    }

    pub async fn submit_to(&self, i: usize, cmd: &str) -> Option<(usize, u64)> {
        self.child_endpoint(i)?
            .call::<_, Option<(usize, u64)>>("_test/submit", &cmd.to_string())
            .await
            .flatten()
    }

    pub async fn get_committed_on(&self, i: usize, index: usize) -> Option<Option<String>> {
        self.child_endpoint(i)?
            .call("_test/get_committed", &index)
            .await
    }

    async fn cluster_state_summary(&self) -> String {
        let mut parts = Vec::new();
        for i in 0..self.n {
            match self.get_state_on(i).await {
                Some((term, true)) => parts.push(format!("peer {i}: term={term} LEADER")),
                Some((term, false)) => parts.push(format!("peer {i}: term={term}")),
                None => parts.push(format!("peer {i}: dead")),
            }
        }
        parts.join(", ")
    }

    pub async fn wait_for_new_leader(&self, after_term: u64) -> (usize, u64) {
        let all: Vec<usize> = (0..self.n).collect();
        for _ in 0..LEADER_WAIT_ATTEMPTS {
            tokio::time::sleep(LEADER_WAIT_INTERVAL).await;
            if let Some((id, term)) = self.leader_in(&all).await {
                if term > after_term {
                    return (id, term);
                }
            }
        }
        let state = self.cluster_state_summary().await;
        panic!(
            "no leader elected with term > {} after {:?}. Cluster state: [{}]",
            after_term,
            LEADER_WAIT_INTERVAL * LEADER_WAIT_ATTEMPTS as u32,
            state
        );
    }

    pub async fn wait_for_leader(&self) -> (usize, u64) {
        self.wait_for_new_leader(0).await
    }

    pub async fn find_leader(&self) -> Option<usize> {
        let all: Vec<usize> = (0..self.n).collect();
        self.leader_in(&all).await.map(|(id, _)| id)
    }

    pub async fn check_one_leader(&self) {
        let mut leaders_by_term: HashMap<u64, Vec<usize>> = HashMap::new();
        for i in 0..self.n {
            if let Some((term, true)) = self.get_state_on(i).await {
                leaders_by_term.entry(term).or_default().push(i);
            }
        }
        for (&term, leaders) in &leaders_by_term {
            assert!(
                leaders.len() <= 1,
                "term {}: expected at most 1 leader, got {:?}",
                term,
                leaders
            );
        }
    }

    pub async fn n_committed(&self, index: usize) -> (usize, Option<String>) {
        let mut count = 0;
        let mut value = None;
        for i in 0..self.n {
            if let Some(Some(cmd)) = self.get_committed_on(i, index).await {
                if let Some(ref v) = value {
                    assert_eq!(v, &cmd, "peers disagree on value at index {}", index);
                } else {
                    value = Some(cmd);
                }
                count += 1;
            }
        }
        (count, value)
    }

    pub async fn one(&self, cmd: &str, expected_servers: usize) -> usize {
        let all_peers: Vec<usize> = (0..self.n).collect();
        let start = Instant::now();
        let mut last_index = None;
        while start.elapsed() < ONE_TIMEOUT {
            if let Some((leader_id, _)) = self.leader_in(&all_peers).await {
                if let Some((index, _)) = self.submit_to(leader_id, cmd).await {
                    last_index = Some(index);
                    for _ in 0..ONE_COMMIT_POLLS {
                        tokio::time::sleep(ONE_COMMIT_POLL_INTERVAL).await;
                        let (nc, _) = self.n_committed(index).await;
                        if nc >= expected_servers {
                            return index;
                        }
                    }
                }
            }

            tokio::time::sleep(ONE_COMMIT_POLL_INTERVAL).await;
        }
        let state = self.cluster_state_summary().await;
        let committed = if let Some(idx) = last_index {
            let (nc, _) = self.n_committed(idx).await;
            format!(", last submitted index {idx} committed on {nc}/{expected_servers} peers")
        } else {
            ", no successful submit".to_string()
        };
        panic!(
            "one({cmd:?}) failed to reach agreement with {expected_servers} servers \
             after {:.1}s{committed}. Cluster state: [{state}]",
            start.elapsed().as_secs_f64()
        );
    }

    pub async fn leader_in(&self, peers: &[usize]) -> Option<(usize, u64)> {
        let mut best: Option<(usize, u64)> = None;
        for &i in peers {
            if let Some((term, true)) = self.get_state_on(i).await {
                if best.is_none_or(|(_, bt)| term > bt) {
                    best = Some((i, term));
                }
            }
        }
        best
    }

    pub async fn check_leader_in(&self, peers: &[usize]) -> (usize, u64) {
        for _ in 0..LEADER_WAIT_ATTEMPTS {
            tokio::time::sleep(LEADER_WAIT_INTERVAL).await;
            if let Some(result) = self.leader_in(peers).await {
                return result;
            }
        }
        let state = self.cluster_state_summary().await;
        panic!(
            "no leader elected among peers {peers:?} after {:?}. Cluster state: [{state}]",
            LEADER_WAIT_INTERVAL * LEADER_WAIT_ATTEMPTS as u32
        );
    }
}

pub async fn put_at_least_once(
    ck: &dyn KvClient,
    key: &str,
    val: &str,
    mut ver: Version,
) -> Version {
    loop {
        let result = ck.put(key, val, ver).await;
        match result {
            Ok(()) => return ver + 1,
            Err(KVError::Maybe) => {
                if let Ok((got_val, got_ver)) = ck.get(key).await {
                    if got_val == val && got_ver == ver + 1 {
                        return got_ver;
                    }
                }
            }
            Err(KVError::Version | KVError::NoKey) => {
                if let Ok((got_val, got_ver)) = ck.get(key).await {
                    if got_val == val {
                        return got_ver;
                    }
                    ver = got_ver;
                }
            }
        }
    }
}

pub async fn check_get(ck: &dyn KvClient, key: &str, expected_val: &str, expected_ver: Version) {
    let (val, ver) = ck.get(key).await.expect(&format!("get({}) failed", key));
    assert_eq!(val, expected_val, "get({}) value mismatch", key);
    assert_eq!(ver, expected_ver, "get({}) version mismatch", key);
}

pub async fn client_worker(
    ck: Arc<dyn KvClient>,
    client_id: usize,
    done: Arc<AtomicBool>,
) -> (String, Version) {
    let key = format!("key-{}", client_id);
    let mut i = 0u64;

    let mut ver = match ck.get(&key).await {
        Ok((_, v)) => v,
        Err(KVError::NoKey) => put_at_least_once(&*ck, &key, "0", 0).await,
        Err(e) => panic!("unexpected error: {:?}", e),
    };

    let mut val = format!("{}", i);
    while !done.load(Ordering::Relaxed) {
        i += 1;
        let new_val = format!("{}", i);
        ver = put_at_least_once(&*ck, &key, &new_val, ver).await;
        val = new_val;
    }
    (val, ver)
}

pub async fn generic_test(
    nservers: usize,
    nclients: usize,
    reliable: bool,
    crash: bool,
    partitions: bool,
) {
    let config = if reliable {
        NetworkConfig::reliable()
    } else {
        NetworkConfig::unreliable()
    };
    let ts = TestCluster::new_with(nservers, config).await;

    let mut final_states: Vec<Option<(String, Version)>> = vec![None; nclients];

    for _iter in 0..3 {
        let done = Arc::new(AtomicBool::new(false));

        let mut client_handles = Vec::new();
        for cli in 0..nclients {
            let ck = ts.make_client();
            let done = done.clone();
            client_handles.push(tokio::spawn(client_worker(ck, cli, done)));
        }

        let partition_handle = if partitions {
            let ts = ts.clone();
            let done = done.clone();
            Some(tokio::spawn(async move {
                while !done.load(Ordering::Relaxed) {
                    let (srv, d1, d2) = {
                        let mut rng = rand::rng();
                        (
                            rng.random_range(0..ts.n),
                            rng.random_range(100..300u64),
                            rng.random_range(100..300u64),
                        )
                    };
                    ts.disconnect(srv);
                    tokio::time::sleep(Duration::from_millis(d1)).await;
                    ts.reconnect(srv);
                    tokio::time::sleep(Duration::from_millis(d2)).await;
                }
                ts.heal();
            }))
        } else {
            None
        };

        tokio::time::sleep(Duration::from_secs(2)).await;
        done.store(true, Ordering::Relaxed);

        if let Some(h) = partition_handle {
            h.await.unwrap();
        }
        if partitions {
            ts.heal();
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        let round_states = tokio::time::timeout(GENERIC_CLIENT_DRAIN_TIMEOUT, async move {
            let mut states = Vec::with_capacity(nclients);
            for (cli, h) in client_handles.into_iter().enumerate() {
                states.push((cli, h.await.unwrap()));
            }
            states
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "generic_test clients did not finish within {:?}",
                GENERIC_CLIENT_DRAIN_TIMEOUT
            )
        });
        for (cli, state) in round_states {
            final_states[cli] = Some(state);
        }

        if crash {
            ts.kill_all();
            tokio::time::sleep(Duration::from_millis(1000)).await;
            ts.restart_all().await;
            tokio::time::sleep(Duration::from_secs(4)).await;
        }
    }

    let ck = ts.make_client();
    for (cli, state) in final_states.iter().enumerate() {
        if let Some((val, ver)) = state {
            let key = format!("key-{}", cli);
            check_get(&*ck, &key, val, *ver).await;
        }
    }
}

pub async fn churn_test(config: NetworkConfig) {
    let ts = TestCluster::new_raft_with(5, config).await;
    let stop = Arc::new(AtomicBool::new(false));

    let mut client_handles = Vec::new();
    for _client in 0..3 {
        let ts = ts.clone();
        let stop = stop.clone();
        client_handles.push(tokio::spawn(async move {
            let mut committed_count = 0usize;
            while !stop.load(Ordering::SeqCst) {
                let cmd = format!("{}", rand::rng().random::<u32>());

                let mut index = None;
                for i in 0..5 {
                    if let Some((idx, _)) = ts.submit_to(i, &cmd).await {
                        index = Some(idx);
                        break;
                    }
                }

                let Some(index) = index else {
                    tokio::time::sleep(Duration::from_millis(160)).await;
                    continue;
                };

                for &delay in &[100u64, 200, 400, 1000, 2000] {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    let (nc, val) = ts.n_committed(index).await;
                    if nc > 0 {
                        if val.as_ref() == Some(&cmd) {
                            committed_count += 1;
                        }
                        break;
                    }
                }
            }
            committed_count
        }));
    }

    for _iter in 0..20 {
        let i = rand::rng().random_range(0..5);
        let action = rand::rng().random_range(0..1000);

        if action < 200 {
            ts.disconnect(i);
        } else if action < 700 {
            if !ts.is_alive(i) {
                ts.restart(i).await;
            }
            ts.reconnect(i);
        } else if ts.is_alive(i) {
            ts.kill(i);
        }

        tokio::time::sleep(Duration::from_millis(1400)).await;
    }

    for proxy in &ts.proxies {
        proxy.set_reliable();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    ts.restart_dead().await;
    ts.heal();
    tokio::time::sleep(Duration::from_secs(10)).await;

    stop.store(true, Ordering::SeqCst);

    for h in client_handles {
        let count = h.await.unwrap();
        assert!(count > 0, "client committed nothing during churn");
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    ts.one("churn-final", 5).await;
}
