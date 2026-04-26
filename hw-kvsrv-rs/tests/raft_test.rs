mod common;

use std::time::Duration;

use rand::Rng;

use common::raft::{churn_test, test_watchdog};
use common::{NetworkConfig, TestCluster};

#[tokio::test(flavor = "multi_thread")]
async fn test_initial_election() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;
    let (leader, _) = ts.wait_for_leader().await;
    assert!(leader < 3);

    let term1 = ts.get_state_on(leader).await.unwrap().0;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let term2 = ts.get_state_on(leader).await.unwrap().0;
    assert_eq!(term1, term2, "term changed without failure");

    ts.wait_for_leader().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_re_election() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;
    let (leader1, term1) = ts.wait_for_leader().await;

    ts.disconnect(leader1);

    let (_leader2, term2) = ts.wait_for_new_leader(term1).await;

    ts.reconnect(leader1);
    let (leader3, term3) = ts.wait_for_leader().await;
    assert!(term3 >= term2, "term should not decrease");

    let others = ts.others(leader3);
    ts.disconnect(leader3);
    ts.disconnect(others[0]);

    tokio::time::sleep(Duration::from_secs(4)).await;
    let (_, is_leader) = ts.get_state_on(others[1]).await.unwrap();
    assert!(
        !is_leader,
        "peer {} became leader without quorum",
        others[1]
    );

    ts.reconnect(others[0]);
    ts.wait_for_leader().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_many_elections() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(5).await;

    ts.wait_for_leader().await;

    for _ in 0..5 {
        let (leader, term) = ts.wait_for_leader().await;
        ts.disconnect(leader);
        let other = ts.others(leader)[0];
        ts.disconnect(other);

        ts.wait_for_new_leader(term).await;

        ts.reconnect(leader);
        ts.reconnect(other);
    }

    ts.wait_for_leader().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_agree() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;

    for i in 1..=3 {
        let cmd = format!("cmd-{}", i);
        let index = ts.one(&cmd, 3).await;
        assert_eq!(index, i, "expected index {}, got {}", i, index);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_follower_failure() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;

    ts.one("cmd-1", 3).await;

    let (leader, _) = ts.wait_for_leader().await;
    let follower = ts.others(leader)[0];
    ts.disconnect(follower);

    ts.one("cmd-2", 2).await;
    ts.one("cmd-3", 2).await;

    ts.reconnect(follower);
    ts.one("cmd-4", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leader_failure() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;

    ts.one("cmd-1", 3).await;

    let (leader, _) = ts.wait_for_leader().await;
    ts.disconnect(leader);

    ts.one("cmd-2", 2).await;
    ts.one("cmd-3", 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fail_no_agree() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;

    ts.one("cmd-1", 3).await;

    let (leader, _) = ts.wait_for_leader().await;
    let followers = ts.others(leader);
    ts.disconnect(followers[0]);
    ts.disconnect(followers[1]);

    let result = ts.submit_to(leader, "no-quorum").await;
    assert!(result.is_some());

    tokio::time::sleep(Duration::from_secs(4)).await;
    let (nc, _) = ts.n_committed(2).await;
    assert_eq!(nc, 0, "command committed without quorum");

    ts.reconnect(followers[0]);
    ts.reconnect(followers[1]);
    ts.one("cmd-after-heal", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_persist_basic() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;

    ts.one("cmd-1", 3).await;

    let (leader, _) = ts.wait_for_leader().await;
    let follower = ts.others(leader)[0];

    ts.kill(follower);
    ts.restart(follower).await;

    ts.one("cmd-2", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_persist_leader() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;

    ts.one("cmd-1", 3).await;

    let (leader, _) = ts.wait_for_leader().await;

    ts.kill(leader);
    ts.restart(leader).await;

    ts.one("cmd-2", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_persist_multiple() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(5).await;

    ts.one("cmd-1", 5).await;

    let (leader, _) = ts.wait_for_leader().await;
    let followers = ts.others(leader);
    ts.kill(followers[0]);
    ts.kill(followers[1]);
    ts.restart(followers[0]).await;
    ts.restart(followers[1]).await;

    ts.one("cmd-2", 5).await;

    let (leader, _) = ts.wait_for_leader().await;
    ts.kill(leader);
    ts.restart(leader).await;

    ts.one("cmd-3", 5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_majority_commits() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(5).await;

    ts.one("before-partition", 5).await;

    let majority = vec![0, 1, 2];
    let minority = vec![3, 4];
    ts.partition(&majority, &minority);

    let (_maj_leader, _) = ts.check_leader_in(&majority).await;
    let idx = ts.one("majority-cmd", 3).await;
    assert!(idx > 0);

    for &p in &minority {
        ts.submit_to(p, "minority-cmd").await;
    }
    tokio::time::sleep(Duration::from_secs(4)).await;

    let (maj_count, _) = ts.n_committed(idx).await;
    assert!(
        maj_count >= 3,
        "majority cmd should be committed on >= 3 peers"
    );

    ts.heal();

    ts.one("after-partition", 5).await;

    let (_, val) = ts.n_committed(idx).await;
    assert_eq!(val.as_deref(), Some("majority-cmd"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_two_leaders() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(5).await;

    ts.one("cmd-1", 5).await;

    let (old_leader, _) = ts.wait_for_leader().await;

    let minority: Vec<usize> = vec![old_leader, (old_leader + 1) % 5];
    let majority: Vec<usize> = (0..5).filter(|i| !minority.contains(i)).collect();

    ts.partition(&majority, &minority);

    let (_maj_leader, maj_term) = ts.check_leader_in(&majority).await;

    let idx_maj = ts.one("majority-only", 3).await;

    ts.submit_to(old_leader, "minority-only").await;
    tokio::time::sleep(Duration::from_secs(4)).await;

    ts.heal();

    let (_leader_after, term_after) = ts.wait_for_leader().await;
    assert!(
        term_after >= maj_term,
        "term should not decrease after heal"
    );

    let (_count, val) = ts.n_committed(idx_maj).await;
    assert_eq!(val.as_deref(), Some("majority-only"));

    ts.one("after-heal", 5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_leader_alone() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;

    ts.one("cmd-1", 3).await;

    let (leader, _) = ts.wait_for_leader().await;
    let others = ts.others(leader);

    ts.partition(&[leader], &others);
    assert!(
        ts.submit_to(leader, "will-be-lost").await.is_some(),
        "isolated old leader should still accept a local command"
    );

    let (_new_leader, _) = ts.check_leader_in(&others).await;
    ts.one("new-leader-cmd", 2).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    ts.heal();

    ts.one("after-heal", 3).await;

    let mut found_new = false;
    let mut found_lost = false;
    for i in 0..ts.n {
        for idx in 1..100 {
            let Some(Some(cmd)) = ts.get_committed_on(i, idx).await else {
                break;
            };
            found_new |= cmd == "new-leader-cmd";
            found_lost |= cmd == "will-be-lost";
        }
    }
    assert!(found_new, "new-leader-cmd should be committed");
    assert!(!found_lost, "will-be-lost should NOT be committed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_repeated() {
    let _wd = test_watchdog(180);
    let ts = TestCluster::new_raft(5).await;

    ts.one("init", 5).await;

    for round in 0..4 {
        let (leader, _) = ts.wait_for_leader().await;

        let partner = (leader + 1) % 5;
        let minority = vec![leader, partner];
        let majority: Vec<usize> = (0..5).filter(|i| !minority.contains(i)).collect();

        ts.partition(&majority, &minority);

        let cmd = format!("round-{}", round);
        ts.one(&cmd, 3).await;

        ts.heal();
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }

    ts.one("final", 5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_conflicting_commits() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(5).await;

    ts.one("agreed-1", 5).await;

    let (old_leader, _) = ts.wait_for_leader().await;
    let minority = vec![old_leader, (old_leader + 1) % 5];
    let majority: Vec<usize> = (0..5).filter(|i| !minority.contains(i)).collect();

    ts.partition(&majority, &minority);

    ts.submit_to(old_leader, "minority-a").await;
    ts.submit_to(old_leader, "minority-b").await;

    let idx_x = ts.one("majority-x", 3).await;
    let idx_y = ts.one("majority-y", 3).await;

    ts.heal();

    ts.one("agreed-2", 5).await;

    let (_, val_x) = ts.n_committed(idx_x).await;
    assert_eq!(val_x.as_deref(), Some("majority-x"));
    let (_, val_y) = ts.n_committed(idx_y).await;
    assert_eq!(val_y.as_deref(), Some("majority-y"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fail_agree() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;

    ts.one("101", 3).await;

    let (leader, _) = ts.wait_for_leader().await;
    let follower = (leader + 1) % 3;
    ts.disconnect(follower);

    ts.one("102", 2).await;
    ts.one("103", 2).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    ts.one("104", 2).await;
    ts.one("105", 2).await;

    ts.reconnect(follower);

    ts.one("106", 3).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    ts.one("107", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_starts() {
    let _wd = test_watchdog(240);
    let ts = TestCluster::new_raft(3).await;

    for attempt in 0..5 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(6)).await;
        }

        let (leader, _) = ts.wait_for_leader().await;
        let Some((_, term)) = ts.submit_to(leader, "0").await else {
            continue;
        };

        let mut handles = Vec::new();
        for ii in 0..5u8 {
            let ts_c = ts.clone();
            handles.push(tokio::spawn(async move {
                let cmd = format!("{}", 100 + ii);
                let Some((idx, t)) = ts_c.submit_to(leader, &cmd).await else {
                    return None;
                };
                if t != term {
                    return None;
                }
                Some(idx)
            }));
        }

        let mut indices = Vec::new();
        for h in handles {
            if let Some(idx) = h.await.unwrap() {
                indices.push(idx);
            }
        }

        let mut term_ok = true;
        for i in 0..3 {
            if let Some((t, _)) = ts.get_state_on(i).await {
                if t != term {
                    term_ok = false;
                    break;
                }
            }
        }
        if !term_ok {
            continue;
        }

        let mut cmds = Vec::new();
        let mut all_committed = true;
        'outer: for &idx in &indices {
            for _ in 0..50 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if let (nc, Some(val)) = ts.n_committed(idx).await {
                    if nc >= 3 {
                        cmds.push(val);
                        continue 'outer;
                    }
                }
            }
            all_committed = false;
            break;
        }

        if !all_committed {
            continue;
        }

        for ii in 0..5u8 {
            let expected = format!("{}", 100 + ii);
            assert!(
                cmds.iter().any(|c| c == &expected),
                "cmd {} missing from committed set",
                100 + ii
            );
        }
        return;
    }
    panic!("concurrent starts failed after 5 attempts");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rejoin() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;

    ts.one("101", 3).await;

    let (leader1, _) = ts.wait_for_leader().await;
    ts.disconnect(leader1);
    ts.submit_to(leader1, "102").await;
    ts.submit_to(leader1, "103").await;
    ts.submit_to(leader1, "104").await;

    ts.one("103", 2).await;

    let (leader2, _) = ts.wait_for_leader().await;
    ts.disconnect(leader2);

    ts.reconnect(leader1);
    ts.one("104", 2).await;

    ts.reconnect(leader2);
    ts.one("105", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_backup() {
    let _wd = test_watchdog(240);
    let ts = TestCluster::new_raft(5).await;

    ts.one("init", 5).await;

    let (leader1, _) = ts.wait_for_leader().await;
    ts.disconnect((leader1 + 2) % 5);
    ts.disconnect((leader1 + 3) % 5);
    ts.disconnect((leader1 + 4) % 5);

    for _ in 0..50 {
        let cmd = format!("{}", rand::rng().random::<u32>());
        ts.submit_to(leader1, &cmd).await;
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;

    ts.disconnect(leader1);
    ts.disconnect((leader1 + 1) % 5);
    ts.reconnect((leader1 + 2) % 5);
    ts.reconnect((leader1 + 3) % 5);
    ts.reconnect((leader1 + 4) % 5);

    for _ in 0..50 {
        let cmd = format!("{}", rand::rng().random::<u32>());
        ts.one(&cmd, 3).await;
    }

    let (leader2, _) = ts.wait_for_leader().await;
    let other = if (leader1 + 2) % 5 != leader2 {
        (leader1 + 2) % 5
    } else {
        (leader2 + 1) % 5
    };
    ts.disconnect(other);

    for _ in 0..50 {
        let cmd = format!("{}", rand::rng().random::<u32>());
        ts.submit_to(leader2, &cmd).await;
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;

    for i in 0..5 {
        ts.disconnect(i);
    }
    ts.reconnect(leader1);
    ts.reconnect((leader1 + 1) % 5);
    ts.reconnect(other);

    for _ in 0..50 {
        let cmd = format!("{}", rand::rng().random::<u32>());
        ts.one(&cmd, 3).await;
    }

    for i in 0..5 {
        ts.reconnect(i);
    }
    ts.one("final", 5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_persist1() {
    let _wd = test_watchdog(180);
    let ts = TestCluster::new_raft(3).await;

    ts.one("11", 3).await;

    ts.kill_all();
    ts.restart_all().await;

    ts.one("12", 3).await;

    let (leader1, _) = ts.wait_for_leader().await;
    ts.kill(leader1);
    ts.restart(leader1).await;

    ts.one("13", 3).await;

    let (leader2, _) = ts.wait_for_leader().await;
    ts.kill(leader2);
    ts.one("14", 2).await;

    ts.restart(leader2).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    ts.one("15-after-rejoin", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_persist2() {
    let _wd = test_watchdog(360);
    let ts = TestCluster::new_raft(5).await;

    for iter in 0..5 {
        ts.one(&format!("iter-{}-a", iter), 5).await;

        let (leader, _) = ts.wait_for_leader().await;

        ts.kill((leader + 1) % 5);
        ts.kill((leader + 2) % 5);

        ts.one(&format!("iter-{}-b", iter), 3).await;

        ts.kill(leader);
        ts.kill((leader + 3) % 5);
        ts.kill((leader + 4) % 5);

        ts.restart((leader + 1) % 5).await;
        ts.restart((leader + 2) % 5).await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        ts.restart((leader + 3) % 5).await;

        ts.one(&format!("iter-{}-c", iter), 3).await;

        ts.restart((leader + 4) % 5).await;
        ts.restart(leader).await;
    }

    ts.one("final", 5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_persist3() {
    let _wd = test_watchdog(120);
    let ts = TestCluster::new_raft(3).await;

    ts.one("101", 3).await;

    let (leader, _) = ts.wait_for_leader().await;
    let disconnected = (leader + 2) % 3;
    ts.disconnect(disconnected);

    ts.one("102", 2).await;

    ts.kill(leader);
    ts.kill((leader + 1) % 3);

    ts.reconnect(disconnected);
    ts.restart(leader % 3).await;

    ts.one("103", 2).await;

    ts.restart((leader + 1) % 3).await;
    ts.one("104", 3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_figure8() {
    let _wd = test_watchdog(480);
    let ts = TestCluster::new_raft(5).await;

    ts.one("init", 1).await;

    let mut nup = 5;
    for _iter in 0..1000 {
        let mut leader = None;
        for i in 0..5 {
            if ts.is_alive(i) {
                let cmd = format!("{}", rand::rng().random::<u32>());
                if ts.submit_to(i, &cmd).await.is_some() {
                    leader = Some(i);
                }
            }
        }

        let mut rng = rand::rng();
        let sleep_ms = if rng.random_range(0..1000) < 100 {
            rng.random_range(0..1000)
        } else {
            rng.random_range(0..26)
        };
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

        if let Some(leader) = leader {
            ts.kill(leader);
            nup -= 1;
        }

        if nup < 3 {
            let s = rand::rng().random_range(0..5);
            if !ts.is_alive(s) {
                ts.restart(s).await;
                nup += 1;
            }
        }
    }

    ts.restart_dead().await;

    ts.one("figure8-final", 5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reliable_churn() {
    let _wd = test_watchdog(240);
    churn_test(NetworkConfig::reliable()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unreliable_churn() {
    let _wd = test_watchdog(240);
    churn_test(NetworkConfig::unreliable()).await;
}
