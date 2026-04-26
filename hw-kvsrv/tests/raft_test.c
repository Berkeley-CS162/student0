#include "tests/test_common.h"
#include "tests/raft_test_helpers.h"
#include "lib/rpc.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stdatomic.h>
#include <pthread.h>

static void test_initial_election(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    int leader;
    uint64_t term;
    raft_check_one_leader(tc, &leader, &term);
    ASSERT(leader >= 0 && leader < 3, "leader out of range: %d", leader);

    uint64_t term1;
    bool is_leader1;
    ASSERT(test_cluster_get_state_on(tc, leader, &term1, &is_leader1),
           "could not get state from leader %d", leader);

    sleep(2);

    uint64_t term2;
    bool is_leader2;
    ASSERT(test_cluster_get_state_on(tc, leader, &term2, &is_leader2),
           "could not get state from leader %d", leader);
    ASSERT(term1 == term2, "term changed without failure: %llu -> %llu",
           (unsigned long long)term1, (unsigned long long)term2);

    raft_check_one_leader(tc, &leader, &term);

    test_cluster_free(tc);
}

static void test_re_election(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    int leader1;
    uint64_t term1;
    raft_check_one_leader(tc, &leader1, &term1);

    test_cluster_disconnect(tc, leader1);

    int leader2;
    uint64_t term2;
    raft_check_one_leader(tc, &leader2, &term2);

    test_cluster_reconnect(tc, leader1);

    int leader3;
    uint64_t term3;
    raft_check_one_leader(tc, &leader3, &term3);
    ASSERT(term3 >= term2, "term should not decrease");

    int other = -1, remaining = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader3) {
            if (other < 0) other = i;
            else remaining = i;
        }
    }

    test_cluster_disconnect(tc, leader3);
    test_cluster_disconnect(tc, other);

    sleep(2);

    uint64_t t;
    bool is_leader;
    if (test_cluster_get_state_on(tc, remaining, &t, &is_leader)) {
        ASSERT(!is_leader, "peer %d became leader without quorum", remaining);
    }

    test_cluster_reconnect(tc, other);

    int leader_final;
    uint64_t term_final;
    raft_check_one_leader(tc, &leader_final, &term_final);

    test_cluster_free(tc);
}

static void test_many_elections(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());

    int leader;
    uint64_t term;
    raft_check_one_leader(tc, &leader, &term);

    for (int iter = 0; iter < 5; iter++) {
        raft_check_one_leader(tc, &leader, &term);
        test_cluster_disconnect(tc, leader);
        int other = -1;
        for (int i = 0; i < 5; i++) {
            if (i != leader) { other = i; break; }
        }
        test_cluster_disconnect(tc, other);

        raft_check_one_leader(tc, &leader, &term);

        test_cluster_reconnect(tc, leader);
        test_cluster_reconnect(tc, other);
    }

    raft_check_one_leader(tc, &leader, &term);

    test_cluster_free(tc);
}

static void test_basic_agree(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    for (int i = 1; i <= 3; i++) {
        char cmd[32];
        snprintf(cmd, sizeof(cmd), "cmd-%d", i);
        int index = raft_one(tc, cmd, 3);
        ASSERT(index == i, "expected index %d, got %d", i, index);
    }

    test_cluster_free(tc);
}

static void test_follower_failure(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    raft_one(tc, "cmd-1", 3);

    int leader;
    uint64_t term;
    raft_check_one_leader(tc, &leader, &term);
    int follower = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader) { follower = i; break; }
    }
    test_cluster_disconnect(tc, follower);

    raft_one(tc, "cmd-2", 2);
    raft_one(tc, "cmd-3", 2);

    test_cluster_reconnect(tc, follower);
    raft_one(tc, "cmd-4", 3);

    test_cluster_free(tc);
}

static void test_leader_failure(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    raft_one(tc, "cmd-1", 3);

    int leader;
    uint64_t term;
    raft_check_one_leader(tc, &leader, &term);
    test_cluster_disconnect(tc, leader);

    raft_one(tc, "cmd-2", 2);
    raft_one(tc, "cmd-3", 2);

    test_cluster_free(tc);
}

static void test_fail_no_agree(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    raft_one(tc, "cmd-1", 3);

    int leader;
    uint64_t term;
    raft_check_one_leader(tc, &leader, &term);

    int followers[2];
    int fi = 0;
    for (int i = 0; i < 3; i++) {
        if (i != leader) followers[fi++] = i;
    }
    test_cluster_disconnect(tc, followers[0]);
    test_cluster_disconnect(tc, followers[1]);

    int idx;
    uint64_t t;
    bool is_leader;
    raft_start_or_default(tc, leader, "no-quorum", &idx, &t, &is_leader);
    ASSERT(is_leader, "leader should still think it's leader");

    sleep(2);

    int nc = raft_n_committed(tc, 2, NULL);
    ASSERT(nc == 0, "command committed without quorum: %d peers committed", nc);

    test_cluster_reconnect(tc, followers[0]);
    test_cluster_reconnect(tc, followers[1]);

    raft_one(tc, "cmd-after-heal", 3);

    test_cluster_free(tc);
}

static void test_persist_basic(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    raft_one(tc, "cmd-1", 3);

    int leader;
    uint64_t term;
    raft_check_one_leader(tc, &leader, &term);
    int follower = -1;
    for (int i = 0; i < 3; i++) {
        if (i != leader) { follower = i; break; }
    }

    test_cluster_kill(tc, follower);
    test_cluster_restart(tc, follower);

    raft_one(tc, "cmd-2", 3);

    test_cluster_free(tc);
}

static void test_persist_leader(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    raft_one(tc, "cmd-1", 3);

    int leader;
    uint64_t term;
    raft_check_one_leader(tc, &leader, &term);

    test_cluster_kill(tc, leader);
    test_cluster_restart(tc, leader);

    raft_one(tc, "cmd-2", 3);

    test_cluster_free(tc);
}

static void test_persist_multiple(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());

    raft_one(tc, "cmd-1", 5);

    int leader;
    uint64_t term;
    raft_check_one_leader(tc, &leader, &term);

    int followers[4];
    int fi = 0;
    for (int i = 0; i < 5; i++) {
        if (i != leader) followers[fi++] = i;
    }

    test_cluster_kill(tc, followers[0]);
    test_cluster_kill(tc, followers[1]);
    test_cluster_restart(tc, followers[0]);
    test_cluster_restart(tc, followers[1]);

    raft_one(tc, "cmd-2", 5);

    raft_check_one_leader(tc, &leader, &term);
    test_cluster_kill(tc, leader);
    test_cluster_restart(tc, leader);

    raft_one(tc, "cmd-3", 5);

    test_cluster_free(tc);
}

static void test_partition_majority_commits(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());

    raft_one(tc, "before-partition", 5);

    int majority[] = {0, 1, 2};
    int minority[] = {3, 4};
    test_cluster_partition(tc, majority, 3, minority, 2);

    int maj_leader;
    uint64_t maj_term;
    raft_check_leader_in(tc, majority, 3, &maj_leader, &maj_term);

    int idx = raft_one(tc, "majority-cmd", 3);
    ASSERT(idx > 0, "majority-cmd got invalid index");

    for (int pi = 0; pi < 2; pi++) {
        int dummy_idx;
        uint64_t dummy_t;
        bool dummy_ok;
        raft_start_on(tc, minority[pi], "minority-cmd",
                 &dummy_idx, &dummy_t, &dummy_ok);
    }
    sleep(2);

    int maj_count = raft_n_committed(tc, idx, NULL);
    ASSERT(maj_count >= 3,
           "majority cmd should be committed on >= 3 peers, got %d", maj_count);

    test_cluster_heal(tc);

    raft_one(tc, "after-partition", 5);

    char *val = NULL;
    raft_n_committed(tc, idx, &val);
    ASSERT(val && strcmp(val, "majority-cmd") == 0,
           "expected 'majority-cmd', got '%s'", val ? val : "(null)");
    free(val);

    test_cluster_free(tc);
}

static void test_partition_two_leaders(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());

    raft_one(tc, "cmd-1", 5);

    int old_leader;
    uint64_t old_term;
    raft_check_one_leader(tc, &old_leader, &old_term);

    int minority[2] = {old_leader, (old_leader + 1) % 5};
    int majority[3];
    int mi = 0;
    for (int i = 0; i < 5; i++) {
        bool in_minority = false;
        for (int j = 0; j < 2; j++) {
            if (i == minority[j]) { in_minority = true; break; }
        }
        if (!in_minority) majority[mi++] = i;
    }

    test_cluster_partition(tc, majority, 3, minority, 2);

    int maj_leader;
    uint64_t maj_term;
    raft_check_leader_in(tc, majority, 3, &maj_leader, &maj_term);

    int idx_maj = raft_one(tc, "majority-only", 3);

    int dummy_idx;
    uint64_t dummy_t;
    bool dummy_ok;
    raft_start_on(tc, old_leader, "minority-only", &dummy_idx, &dummy_t, &dummy_ok);
    sleep(2);

    test_cluster_heal(tc);

    int leader_after;
    uint64_t term_after;
    raft_check_one_leader(tc, &leader_after, &term_after);
    ASSERT(term_after >= maj_term,
           "term should not decrease after heal: %llu < %llu",
           (unsigned long long)term_after, (unsigned long long)maj_term);

    char *val = NULL;
    raft_n_committed(tc, idx_maj, &val);
    ASSERT(val && strcmp(val, "majority-only") == 0,
           "expected 'majority-only', got '%s'", val ? val : "(null)");
    free(val);

    raft_one(tc, "after-heal", 5);

    test_cluster_free(tc);
}

static void test_partition_leader_alone(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    raft_one(tc, "cmd-1", 3);

    int leader;
    uint64_t term;
    raft_check_one_leader(tc, &leader, &term);

    int others[2];
    int oi = 0;
    for (int i = 0; i < 3; i++) {
        if (i != leader) others[oi++] = i;
    }

    int lone[] = {leader};
    test_cluster_partition(tc, lone, 1, others, 2);

    int dummy_idx;
    uint64_t dummy_t;
    bool dummy_ok;
    ASSERT(raft_start_on(tc, leader, "will-be-lost",
                         &dummy_idx, &dummy_t, &dummy_ok) && dummy_ok,
           "isolated old leader should still accept a local command");

    int new_leader;
    uint64_t new_term;
    raft_check_leader_in(tc, others, 2, &new_leader, &new_term);
    raft_one(tc, "new-leader-cmd", 2);

    sleep(1);

    test_cluster_heal(tc);

    raft_one(tc, "after-heal", 3);

    bool found_new = false;
    bool found_lost = false;
    for (int i = 0; i < tc->n; i++) {
        for (int idx = 1; idx < 100; idx++) {
            char *cmd = NULL;
            if (!raft_get_committed_on(tc, i, idx, &cmd) || !cmd)
                break;
            if (strcmp(cmd, "new-leader-cmd") == 0) found_new = true;
            if (strcmp(cmd, "will-be-lost") == 0) found_lost = true;
            free(cmd);
        }
    }
    ASSERT(found_new, "new-leader-cmd should be committed");
    ASSERT(!found_lost, "will-be-lost should NOT be committed");

    test_cluster_free(tc);
}

static void test_partition_repeated(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());

    raft_one(tc, "init", 5);

    for (int round = 0; round < 4; round++) {
        int leader;
        uint64_t term;
        raft_check_one_leader(tc, &leader, &term);

        int partner = (leader + 1) % 5;
        int minority[2] = {leader, partner};
        int majority[3];
        int mi = 0;
        for (int i = 0; i < 5; i++) {
            if (i != leader && i != partner) majority[mi++] = i;
        }

        test_cluster_partition(tc, majority, 3, minority, 2);

        char cmd[32];
        snprintf(cmd, sizeof(cmd), "round-%d", round);
        raft_one(tc, cmd, 3);

        test_cluster_heal(tc);
        usleep(500000);
    }

    raft_one(tc, "final", 5);

    test_cluster_free(tc);
}

static void test_partition_conflicting_commits(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());

    raft_one(tc, "agreed-1", 5);

    int old_leader;
    uint64_t old_term;
    raft_check_one_leader(tc, &old_leader, &old_term);

    int minority[2] = {old_leader, (old_leader + 1) % 5};
    int majority[3];
    int mi = 0;
    for (int i = 0; i < 5; i++) {
        bool in_min = false;
        for (int j = 0; j < 2; j++) {
            if (i == minority[j]) { in_min = true; break; }
        }
        if (!in_min) majority[mi++] = i;
    }

    test_cluster_partition(tc, majority, 3, minority, 2);

    int dummy_idx;
    uint64_t dummy_t;
    bool dummy_ok;
    raft_start_on(tc, old_leader, "minority-a", &dummy_idx, &dummy_t, &dummy_ok);
    raft_start_on(tc, old_leader, "minority-b", &dummy_idx, &dummy_t, &dummy_ok);

    int idx_x = raft_one(tc, "majority-x", 3);
    int idx_y = raft_one(tc, "majority-y", 3);

    test_cluster_heal(tc);

    raft_one(tc, "agreed-2", 5);

    char *val_x = NULL;
    raft_n_committed(tc, idx_x, &val_x);
    ASSERT(val_x && strcmp(val_x, "majority-x") == 0,
           "expected 'majority-x', got '%s'", val_x ? val_x : "(null)");
    free(val_x);

    char *val_y = NULL;
    raft_n_committed(tc, idx_y, &val_y);
    ASSERT(val_y && strcmp(val_y, "majority-y") == 0,
           "expected 'majority-y', got '%s'", val_y ? val_y : "(null)");
    free(val_y);

    test_cluster_free(tc);
}

static void test_fail_agree(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    raft_one(tc, "101", 3);

    int leader;
    uint64_t term;
    raft_check_one_leader(tc, &leader, &term);
    int follower = (leader + 1) % 3;
    test_cluster_disconnect(tc, follower);

    raft_one(tc, "102", 2);
    raft_one(tc, "103", 2);
    sleep(1);
    raft_one(tc, "104", 2);
    raft_one(tc, "105", 2);

    test_cluster_reconnect(tc, follower);

    raft_one(tc, "106", 3);
    sleep(1);
    raft_one(tc, "107", 3);

    test_cluster_free(tc);
}

typedef struct {
    test_cluster_t *tc;
    int leader;
    uint64_t term;
    int result_index;
} concurrent_start_arg_t;

static void *concurrent_start_worker(void *arg) {
    concurrent_start_arg_t *a = (concurrent_start_arg_t *)arg;
    char cmd[32];
    snprintf(cmd, sizeof(cmd), "%d", 100 + a->result_index);

    int idx;
    uint64_t t;
    bool ok;
    raft_start_or_default(a->tc, a->leader, cmd, &idx, &t, &ok);

    if (t != a->term || !ok) {
        a->result_index = 0;
    } else {
        a->result_index = idx;
    }
    return NULL;
}

static void test_concurrent_starts(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    for (int attempt = 0; attempt < 5; attempt++) {
        if (attempt > 0)
            sleep(3);

        int leader;
        uint64_t term;
        raft_check_one_leader(tc, &leader, &term);

        int idx0;
        uint64_t t0;
        bool ok0;
        raft_start_or_default(tc, leader, "0", &idx0, &t0, &ok0);
        if (!ok0)
            continue;

        pthread_t threads[5];
        concurrent_start_arg_t args[5];

        for (int ii = 0; ii < 5; ii++) {
            args[ii].tc = tc;
            args[ii].leader = leader;
            args[ii].term = t0;
            args[ii].result_index = ii;
            pthread_create(&threads[ii], NULL, concurrent_start_worker, &args[ii]);
        }

        int indices[5];
        int nindices = 0;
        for (int ii = 0; ii < 5; ii++) {
            pthread_join(threads[ii], NULL);
            if (args[ii].result_index > 0)
                indices[nindices++] = args[ii].result_index;
        }

        bool term_ok = true;
        for (int i = 0; i < 3; i++) {
            uint64_t t;
            bool il;
            if (test_cluster_get_state_on(tc, i, &t, &il)) {
                if (t != t0) { term_ok = false; break; }
            }
        }
        if (!term_ok)
            continue;

        char *cmds[5];
        int ncmds = 0;
        bool all_committed = true;

        for (int ii = 0; ii < nindices; ii++) {
            bool found = false;
            for (int poll = 0; poll < 50; poll++) {
                usleep(50000);
                char *val = NULL;
                int nc = raft_n_committed(tc, indices[ii], &val);
                if (nc >= 3 && val) {
                    cmds[ncmds++] = val;
                    found = true;
                    break;
                }
                free(val);
            }
            if (!found) {
                all_committed = false;
                break;
            }
        }

        if (!all_committed) {
            for (int i = 0; i < ncmds; i++) free(cmds[i]);
            continue;
        }

        for (int ii = 0; ii < 5; ii++) {
            char expected[32];
            snprintf(expected, sizeof(expected), "%d", 100 + ii);
            bool found = false;
            for (int ci = 0; ci < ncmds; ci++) {
                if (strcmp(cmds[ci], expected) == 0) {
                    found = true;
                    break;
                }
            }
            ASSERT(found, "cmd %d missing from committed set", 100 + ii);
        }

        for (int i = 0; i < ncmds; i++) free(cmds[i]);

        test_cluster_free(tc);
        return;
    }
    ASSERT(0, "concurrent starts failed after 5 attempts");
}

static void test_rejoin(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    raft_one(tc, "101", 3);

    int leader1;
    uint64_t term1;
    raft_check_one_leader(tc, &leader1, &term1);
    test_cluster_disconnect(tc, leader1);

    int dummy_idx;
    uint64_t dummy_t;
    bool dummy_ok;
    raft_start_on(tc, leader1, "102", &dummy_idx, &dummy_t, &dummy_ok);
    raft_start_on(tc, leader1, "103", &dummy_idx, &dummy_t, &dummy_ok);
    raft_start_on(tc, leader1, "104", &dummy_idx, &dummy_t, &dummy_ok);

    raft_one(tc, "103", 2);

    int leader2;
    uint64_t term2;
    raft_check_one_leader(tc, &leader2, &term2);
    test_cluster_disconnect(tc, leader2);

    test_cluster_reconnect(tc, leader1);
    raft_one(tc, "104", 2);

    test_cluster_reconnect(tc, leader2);
    raft_one(tc, "105", 3);

    test_cluster_free(tc);
}

static void test_backup(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());

    raft_one(tc, "init", 5);

    int leader1;
    uint64_t term1;
    raft_check_one_leader(tc, &leader1, &term1);

    test_cluster_disconnect(tc, (leader1 + 2) % 5);
    test_cluster_disconnect(tc, (leader1 + 3) % 5);
    test_cluster_disconnect(tc, (leader1 + 4) % 5);

    for (int i = 0; i < 50; i++) {
        char cmd[32];
        snprintf(cmd, sizeof(cmd), "%u", (unsigned)rand());
        int dummy_idx;
        uint64_t dummy_t;
        bool dummy_ok;
        raft_start_on(tc, leader1, cmd, &dummy_idx, &dummy_t, &dummy_ok);
    }

    usleep(500000);

    test_cluster_disconnect(tc, leader1);
    test_cluster_disconnect(tc, (leader1 + 1) % 5);
    test_cluster_reconnect(tc, (leader1 + 2) % 5);
    test_cluster_reconnect(tc, (leader1 + 3) % 5);
    test_cluster_reconnect(tc, (leader1 + 4) % 5);

    for (int i = 0; i < 50; i++) {
        char cmd[32];
        snprintf(cmd, sizeof(cmd), "%u", (unsigned)rand());
        raft_one(tc, cmd, 3);
    }

    int leader2;
    uint64_t term2;
    raft_check_one_leader(tc, &leader2, &term2);

    int other;
    if ((leader1 + 2) % 5 != leader2)
        other = (leader1 + 2) % 5;
    else
        other = (leader2 + 1) % 5;
    test_cluster_disconnect(tc, other);

    for (int i = 0; i < 50; i++) {
        char cmd[32];
        snprintf(cmd, sizeof(cmd), "%u", (unsigned)rand());
        int dummy_idx;
        uint64_t dummy_t;
        bool dummy_ok;
        raft_start_on(tc, leader2, cmd, &dummy_idx, &dummy_t, &dummy_ok);
    }

    usleep(500000);

    for (int i = 0; i < 5; i++)
        test_cluster_disconnect(tc, i);
    test_cluster_reconnect(tc, leader1);
    test_cluster_reconnect(tc, (leader1 + 1) % 5);
    test_cluster_reconnect(tc, other);

    for (int i = 0; i < 50; i++) {
        char cmd[32];
        snprintf(cmd, sizeof(cmd), "%u", (unsigned)rand());
        raft_one(tc, cmd, 3);
    }

    for (int i = 0; i < 5; i++)
        test_cluster_reconnect(tc, i);
    raft_one(tc, "final", 5);

    test_cluster_free(tc);
}

static void test_persist1(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    raft_one(tc, "11", 3);

    for (int i = 0; i < 3; i++)
        test_cluster_kill(tc, i);
    for (int i = 0; i < 3; i++)
        test_cluster_restart(tc, i);

    raft_one(tc, "12", 3);

    int leader1;
    uint64_t term1;
    raft_check_one_leader(tc, &leader1, &term1);
    test_cluster_kill(tc, leader1);
    test_cluster_restart(tc, leader1);

    raft_one(tc, "13", 3);

    int leader2;
    uint64_t term2;
    raft_check_one_leader(tc, &leader2, &term2);
    test_cluster_kill(tc, leader2);

    raft_one(tc, "14", 2);

    test_cluster_restart(tc, leader2);
    sleep(2);
    raft_one(tc, "15-after-rejoin", 3);

    test_cluster_free(tc);
}

static void test_persist2(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());

    for (int iter = 0; iter < 5; iter++) {
        char cmd[64];

        snprintf(cmd, sizeof(cmd), "iter-%d-a", iter);
        raft_one(tc, cmd, 5);

        int leader;
        uint64_t term;
        raft_check_one_leader(tc, &leader, &term);

        test_cluster_kill(tc, (leader + 1) % 5);
        test_cluster_kill(tc, (leader + 2) % 5);

        snprintf(cmd, sizeof(cmd), "iter-%d-b", iter);
        raft_one(tc, cmd, 3);

        test_cluster_kill(tc, leader);
        test_cluster_kill(tc, (leader + 3) % 5);
        test_cluster_kill(tc, (leader + 4) % 5);

        test_cluster_restart(tc, (leader + 1) % 5);
        test_cluster_restart(tc, (leader + 2) % 5);

        sleep(1);

        test_cluster_restart(tc, (leader + 3) % 5);

        snprintf(cmd, sizeof(cmd), "iter-%d-c", iter);
        raft_one(tc, cmd, 3);

        test_cluster_restart(tc, (leader + 4) % 5);
        test_cluster_restart(tc, leader);
    }

    raft_one(tc, "final", 5);

    test_cluster_free(tc);
}

static void test_persist3(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());

    raft_one(tc, "101", 3);

    int leader;
    uint64_t term;
    raft_check_one_leader(tc, &leader, &term);

    int disconnected = (leader + 2) % 3;
    test_cluster_disconnect(tc, disconnected);

    raft_one(tc, "102", 2);

    test_cluster_kill(tc, leader);
    test_cluster_kill(tc, (leader + 1) % 3);

    test_cluster_reconnect(tc, disconnected);
    test_cluster_restart(tc, leader % 3);

    raft_one(tc, "103", 2);

    test_cluster_restart(tc, (leader + 1) % 3);
    raft_one(tc, "104", 3);

    test_cluster_free(tc);
}

static void test_figure8(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());

    raft_one(tc, "init", 1);

    int nup = 5;
    for (int iter = 0; iter < 1000; iter++) {
        int leader = -1;
        for (int i = 0; i < 5; i++) {
            if (test_cluster_is_alive(tc, i)) {
                char cmd[32];
                snprintf(cmd, sizeof(cmd), "%u", (unsigned)rand());
                int idx;
                uint64_t t;
                bool ok;
                raft_start_or_default(tc, i, cmd, &idx, &t, &ok);
                if (ok) {
                    leader = i;
                }
            }
        }

        int sleep_ms;
        if (rand() % 1000 < 100)
            sleep_ms = rand() % 500;
        else
            sleep_ms = rand() % 13;
        usleep((unsigned)sleep_ms * 1000);

        if (leader >= 0) {
            test_cluster_kill(tc, leader);
            nup--;
        }

        if (nup < 3) {
            int s = rand() % 5;
            if (!test_cluster_is_alive(tc, s)) {
                test_cluster_restart(tc, s);
                nup++;
            }
        }
    }

    for (int i = 0; i < 5; i++) {
        if (!test_cluster_is_alive(tc, i))
            test_cluster_restart(tc, i);
    }

    raft_one(tc, "figure8-final", 5);

    test_cluster_free(tc);
}

typedef struct {
    test_cluster_t *tc;
    atomic_bool *stop;
    char **committed_values;
    int committed_count;
    int committed_cap;
} churn_client_arg_t;

static void *churn_client_worker(void *arg) {
    churn_client_arg_t *a = (churn_client_arg_t *)arg;

    a->committed_count = 0;
    a->committed_cap = 64;
    a->committed_values = malloc((size_t)a->committed_cap * sizeof(char *));

    while (!atomic_load(a->stop)) {
        char cmd[32];
        snprintf(cmd, sizeof(cmd), "%u", (unsigned)rand());

        int index = -1;
        for (int i = 0; i < 5; i++) {
            int idx;
            uint64_t t;
            bool ok;
            if (raft_start_on(a->tc, i, cmd, &idx, &t, &ok) && ok) {
                index = idx;
                break;
            }
        }

        if (index < 0) {
            usleep(80000);
            continue;
        }

        int delays[] = {50, 100, 200, 500, 1000};
        for (int d = 0; d < 5; d++) {
            usleep((unsigned)delays[d] * 1000);
            char *val = NULL;
            int nc = raft_n_committed(a->tc, index, &val);
            if (nc > 0) {
                if (val && strcmp(val, cmd) == 0) {
                    if (a->committed_count >= a->committed_cap) {
                        a->committed_cap *= 2;
                        a->committed_values = realloc(a->committed_values,
                            (size_t)a->committed_cap * sizeof(char *));
                    }
                    a->committed_values[a->committed_count++] = strdup(cmd);
                }
                free(val);
                break;
            }
            free(val);
        }
    }
    return NULL;
}

static void churn_test(network_config_t config) {
    test_cluster_t *tc = test_cluster_new(5, config);

    atomic_bool stop;
    atomic_store(&stop, false);

    pthread_t client_threads[3];
    churn_client_arg_t client_args[3];

    for (int c = 0; c < 3; c++) {
        client_args[c].tc = tc;
        client_args[c].stop = &stop;
        client_args[c].committed_values = NULL;
        client_args[c].committed_count = 0;
        client_args[c].committed_cap = 0;
        pthread_create(&client_threads[c], NULL, churn_client_worker, &client_args[c]);
    }

    for (int iter = 0; iter < 20; iter++) {
        int i = rand() % 5;
        int action = rand() % 1000;

        if (action < 200) {
            test_cluster_disconnect(tc, i);
        } else if (action < 700) {
            if (!test_cluster_is_alive(tc, i))
                test_cluster_restart(tc, i);
            test_cluster_reconnect(tc, i);
        } else if (test_cluster_is_alive(tc, i)) {
            test_cluster_kill(tc, i);
        }

        usleep(700000);
    }

    for (int i = 0; i < 5; i++)
        server_proxy_set_reliable(tc->proxies[i]);
    sleep(1);
    for (int i = 0; i < 5; i++) {
        if (!test_cluster_is_alive(tc, i))
            test_cluster_restart(tc, i);
        test_cluster_reconnect(tc, i);
    }
    sleep(5);

    atomic_store(&stop, true);

    for (int c = 0; c < 3; c++) {
        pthread_join(client_threads[c], NULL);
        ASSERT(client_args[c].committed_count > 0,
               "client %d committed nothing during churn", c);
    }

    sleep(1);
    raft_one(tc, "churn-final", 5);

    for (int c = 0; c < 3; c++) {
        for (int i = 0; i < client_args[c].committed_count; i++)
            free(client_args[c].committed_values[i]);
        free(client_args[c].committed_values);
    }

    test_cluster_free(tc);
}

static void test_reliable_churn(void) {
    churn_test(network_config_reliable());
}

static void test_unreliable_churn(void) {
    churn_test(network_config_unreliable());
}

int main(int argc, char **argv) {
    srand((unsigned)(time(NULL) ^ getpid()));
    test_common_set_filter(argc > 1 ? argv[1] : NULL);

    RUN_TEST(test_initial_election);
    RUN_TEST(test_re_election);
    RUN_TEST(test_many_elections);
    RUN_TEST(test_basic_agree);
    RUN_TEST(test_follower_failure);
    RUN_TEST(test_leader_failure);
    RUN_TEST(test_fail_no_agree);
    RUN_TEST(test_persist_basic);
    RUN_TEST(test_persist_leader);
    RUN_TEST(test_persist_multiple);
    RUN_TEST(test_partition_majority_commits);
    RUN_TEST(test_partition_two_leaders);
    RUN_TEST(test_partition_leader_alone);
    RUN_TEST(test_partition_repeated);
    RUN_TEST(test_partition_conflicting_commits);
    RUN_TEST(test_fail_agree);
    RUN_TEST(test_concurrent_starts);
    RUN_TEST(test_rejoin);
    RUN_TEST(test_backup);
    RUN_TEST(test_persist1);
    RUN_TEST(test_persist2);
    RUN_TEST(test_persist3);
    RUN_TEST(test_figure8);
    RUN_TEST(test_reliable_churn);
    RUN_TEST(test_unreliable_churn);

    if (test_common_ran_count() == 0) {
        fprintf(stderr, "no tests matched filter \"%s\"\n", argv[1]);
        return 1;
    }
    printf("\nAll raft tests passed!\n");
    return 0;
}
