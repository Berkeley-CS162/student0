use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use nix::sys::signal::{Signal, kill};
use nix::sys::wait::waitpid;
use nix::unistd::Pid;
use rand::Rng;
use tokio::task::JoinHandle;

use kvsrv::rpc::RpcClient;

use super::NetworkConfig;
use super::network_proxy::ServerProxy;

pub(super) fn kill_and_wait(pid: Pid) {
    let _ = kill(pid, Signal::SIGKILL);
    if let Err(e) = waitpid(pid, None) {
        eprintln!("warning: waitpid({pid}) failed: {e}");
    }
}

#[derive(Clone)]
pub struct Child {
    pub pid: Pid,
    pub url: String,
}

#[derive(Clone)]
pub(super) enum WorkerSpec {
    Kv,
    Raft {
        me: usize,
        persist_path: PathBuf,
        peer_urls: Vec<String>,
    },
    KvRaft {
        me: usize,
        persist_path: PathBuf,
        peer_urls: Vec<String>,
    },
}

#[derive(Clone, Copy)]
enum ClusterKind {
    Raft,
    KvRaft,
}

fn worker_exe() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_test_worker") {
        return PathBuf::from(path);
    }

    let mut path = std::env::current_exe().expect("current test executable path");
    path.pop();
    if path.file_name().is_some_and(|name| name == "deps") {
        path.pop();
    }
    path.push("test_worker");
    path
}

pub(super) fn spawn_worker(spec: WorkerSpec) -> Child {
    let mut cmd = Command::new(worker_exe());
    match spec {
        WorkerSpec::Kv => {
            cmd.arg("kv");
        }
        WorkerSpec::Raft {
            me,
            persist_path,
            peer_urls,
        } => {
            cmd.arg("raft")
                .arg(me.to_string())
                .arg(persist_path)
                .args(peer_urls);
        }
        WorkerSpec::KvRaft {
            me,
            persist_path,
            peer_urls,
        } => {
            cmd.arg("kvraft")
                .arg(me.to_string())
                .arg(persist_path)
                .args(peer_urls);
        }
    }

    let mut process = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn test_worker");
    let stdout = process.stdout.take().expect("test_worker stdout pipe");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let n = reader
        .read_line(&mut line)
        .expect("failed to read port from test_worker");
    assert!(
        n > 0,
        "test_worker exited before reporting a port: {:?}",
        process.try_wait().ok().flatten()
    );
    let port: u16 = line.trim().parse().expect("test_worker sent invalid port");
    let pid = Pid::from_raw(process.id() as i32);

    Child {
        pid,
        url: format!("http://127.0.0.1:{port}"),
    }
}

fn spawn_child(
    kind: ClusterKind,
    i: usize,
    proxies: &[Arc<ServerProxy>],
    persist_path: PathBuf,
) -> JoinHandle<Child> {
    let peer_urls: Vec<String> = proxies.iter().map(|p| p.url()).collect();
    tokio::task::spawn_blocking(move || match kind {
        ClusterKind::Raft => spawn_worker(WorkerSpec::Raft {
            me: i,
            persist_path,
            peer_urls,
        }),
        ClusterKind::KvRaft => spawn_worker(WorkerSpec::KvRaft {
            me: i,
            persist_path,
            peer_urls,
        }),
    })
}

fn child_persist_path(dir: &Path, i: usize) -> PathBuf {
    dir.join(format!("node-{}.json", i))
}

pub struct TestCluster {
    pub n: usize,
    pub proxies: Vec<Arc<ServerProxy>>,
    children: Mutex<Vec<Option<Child>>>,
    persist_dir: PathBuf,
    kind: ClusterKind,
}

impl TestCluster {
    pub async fn new_raft(n: usize) -> Arc<Self> {
        Self::build(n, NetworkConfig::reliable(), ClusterKind::Raft).await
    }

    pub async fn new_raft_with(n: usize, config: NetworkConfig) -> Arc<Self> {
        Self::build(n, config, ClusterKind::Raft).await
    }

    pub async fn new_kvraft(n: usize) -> Arc<Self> {
        Self::build(n, NetworkConfig::reliable(), ClusterKind::KvRaft).await
    }

    pub async fn new_kvraft_with(n: usize, config: NetworkConfig) -> Arc<Self> {
        Self::build(n, config, ClusterKind::KvRaft).await
    }

    pub async fn new_with(n: usize, config: NetworkConfig) -> Arc<Self> {
        Self::new_kvraft_with(n, config).await
    }

    pub async fn new(n: usize) -> Arc<Self> {
        Self::new_kvraft(n).await
    }

    async fn build(n: usize, config: NetworkConfig, kind: ClusterKind) -> Arc<Self> {
        super::ensure_pool_ready();

        let persist_dir =
            std::env::temp_dir().join(format!("raft-mp-{}", rand::rng().random::<u32>()));
        std::fs::create_dir_all(&persist_dir).unwrap();

        let mut proxies = Vec::with_capacity(n);
        for _ in 0..n {
            proxies.push(ServerProxy::new(config.clone()).await);
        }

        let handles: Vec<_> = (0..n)
            .map(|i| spawn_child(kind, i, &proxies, child_persist_path(&persist_dir, i)))
            .collect();

        let mut children = Vec::with_capacity(n);
        for (i, handle) in handles.into_iter().enumerate() {
            let child = handle.await.unwrap();
            proxies[i].set_child_url(Some(child.url.clone()));
            children.push(Some(child));
        }

        Arc::new(TestCluster {
            n,
            proxies,
            children: Mutex::new(children),
            persist_dir,
            kind,
        })
    }

    pub fn child_endpoint(&self, i: usize) -> Option<Arc<RpcClient>> {
        let url = self.children.lock().unwrap()[i]
            .as_ref()
            .map(|c| c.url.clone())?;
        Some(Arc::new(RpcClient::new(url, usize::MAX)))
    }

    pub fn disconnect(&self, i: usize) {
        for j in 0..self.n {
            if j != i {
                self.proxies[j].block_client(i);
                self.proxies[i].block_client(j);
            }
        }
    }

    pub fn reconnect(&self, i: usize) {
        for j in 0..self.n {
            if j != i {
                self.proxies[j].unblock_client(i);
                self.proxies[i].unblock_client(j);
            }
        }
    }

    pub fn partition(&self, group_a: &[usize], group_b: &[usize]) {
        for &a in group_a {
            for &b in group_b {
                self.proxies[b].block_client(a);
                self.proxies[a].block_client(b);
            }
        }
    }

    pub fn heal(&self) {
        for a in 0..self.n {
            for b in 0..self.n {
                self.proxies[a].unblock_client(b);
            }
        }
    }

    pub fn kill(&self, i: usize) {
        if let Some(child) = self.children.lock().unwrap()[i].take() {
            kill_and_wait(child.pid);
        }
        self.proxies[i].set_child_url(None);
        self.disconnect(i);
    }

    pub async fn restart(&self, i: usize) {
        let path = child_persist_path(&self.persist_dir, i);
        let child = spawn_child(self.kind, i, &self.proxies, path)
            .await
            .unwrap();

        self.proxies[i].set_child_url(Some(child.url.clone()));
        self.children.lock().unwrap()[i] = Some(child);
        self.reconnect(i);
    }

    pub fn is_alive(&self, i: usize) -> bool {
        self.children.lock().unwrap()[i].is_some()
    }

    pub fn others(&self, exclude: usize) -> Vec<usize> {
        (0..self.n).filter(|&i| i != exclude).collect()
    }

    pub fn kill_all(&self) {
        for i in 0..self.n {
            self.kill(i);
        }
    }

    pub async fn restart_all(&self) {
        for i in 0..self.n {
            self.restart(i).await;
        }
    }

    pub async fn restart_dead(&self) {
        for i in 0..self.n {
            if !self.is_alive(i) {
                self.restart(i).await;
            }
        }
    }

    pub fn spawn_chaos(
        self: &Arc<Self>,
        done: Arc<AtomicBool>,
        min_alive: usize,
    ) -> JoinHandle<()> {
        let ts = self.clone();
        tokio::spawn(async move {
            let mut alive = vec![true; ts.n];
            while !done.load(Ordering::SeqCst) {
                let (target, sleep_ms) = {
                    let mut rng = rand::rng();
                    (rng.random_range(0..ts.n), rng.random_range(25..100))
                };
                let alive_count = alive.iter().filter(|&&a| a).count();
                if alive[target] && alive_count > min_alive {
                    ts.kill(target);
                    alive[target] = false;
                } else if !alive[target] {
                    ts.restart(target).await;
                    alive[target] = true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
            }
            for (i, &is_alive) in alive.iter().enumerate() {
                if !is_alive {
                    ts.restart(i).await;
                }
            }
        })
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for child in self.children.lock().unwrap().iter().flatten() {
            kill_and_wait(child.pid);
        }
        if let Err(e) = std::fs::remove_dir_all(&self.persist_dir) {
            eprintln!(
                "warning: failed to clean up persist dir {:?}: {e}",
                self.persist_dir
            );
        }
    }
}
