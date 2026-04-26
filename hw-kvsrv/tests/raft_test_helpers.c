#include "tests/raft_test_helpers.h"

#include "lib/cjson/cJSON.h"
#include "lib/rpc.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define RAFT_LEADER_WAIT_ATTEMPTS 20
#define RAFT_LEADER_WAIT_USEC 500000
#define RAFT_ONE_TIMEOUT_SEC 30.0
#define RAFT_ONE_COMMIT_POLLS 100
#define RAFT_ONE_POLL_USEC 50000

bool raft_start_on(test_cluster_t *tc, int i, const char *cmd,
                   int *out_index, uint64_t *out_term, bool *out_is_leader) {
    rpc_client_t *ep = test_cluster_child_endpoint(tc, i);
    if (!ep) return false;

    cJSON *cmd_json = cJSON_CreateString(cmd);
    char *body = cJSON_PrintUnformatted(cmd_json);
    cJSON_Delete(cmd_json);

    char *reply = rpc_call(ep, "_test/submit", body);
    free(body);
    rpc_client_free(ep);

    if (!reply) return false;

    cJSON *j = cJSON_Parse(reply);
    free(reply);
    if (!j) return false;

    if (cJSON_IsNull(j)) {
        *out_index = 0;
        *out_term = 0;
        *out_is_leader = false;
    } else {
        *out_index = cJSON_GetArrayItem(j, 0)->valueint;
        *out_term = (uint64_t)cJSON_GetArrayItem(j, 1)->valuedouble;
        *out_is_leader = true;
    }
    cJSON_Delete(j);
    return true;
}

void raft_start_or_default(test_cluster_t *tc, int peer, const char *cmd,
                           int *out_index, uint64_t *out_term,
                           bool *out_is_leader) {
    if (!raft_start_on(tc, peer, cmd, out_index, out_term, out_is_leader)) {
        *out_index = 0;
        *out_term = 0;
        *out_is_leader = false;
    }
}

bool raft_get_committed_on(test_cluster_t *tc, int i, int index,
                           char **out_cmd) {
    rpc_client_t *ep = test_cluster_child_endpoint(tc, i);
    if (!ep) return false;

    char body[32];
    snprintf(body, sizeof(body), "%d", index);

    char *reply = rpc_call(ep, "_test/get_committed", body);
    rpc_client_free(ep);

    if (!reply) return false;

    cJSON *j = cJSON_Parse(reply);
    free(reply);

    if (!j || cJSON_IsNull(j)) {
        *out_cmd = NULL;
    } else if (cJSON_IsString(j)) {
        *out_cmd = strdup(j->valuestring);
    } else {
        *out_cmd = NULL;
    }
    cJSON_Delete(j);
    return true;
}

void raft_check_one_leader(test_cluster_t *tc, int *out_leader,
                           uint64_t *out_term) {
    for (int attempt = 0; attempt < RAFT_LEADER_WAIT_ATTEMPTS; attempt++) {
        usleep(RAFT_LEADER_WAIT_USEC);

        uint64_t best_term = 0;
        int leader = -1;
        int leader_count = 0;

        for (int i = 0; i < tc->n; i++) {
            uint64_t term;
            bool is_leader;
            if (test_cluster_get_state_on(tc, i, &term, &is_leader) && is_leader) {
                if (term > best_term) {
                    best_term = term;
                    leader = i;
                    leader_count = 1;
                } else if (term == best_term) {
                    leader_count++;
                }
            }
        }

        if (leader_count == 1) {
            *out_leader = leader;
            *out_term = best_term;
            return;
        }
        ASSERT(leader_count <= 1, "term %llu: %d leaders",
               (unsigned long long)best_term, leader_count);
    }
    ASSERT(0, "no leader elected");
}

int raft_n_committed(test_cluster_t *tc, int index, char **out_value) {
    int count = 0;
    char *value = NULL;

    for (int i = 0; i < tc->n; i++) {
        char *cmd = NULL;
        if (raft_get_committed_on(tc, i, index, &cmd) && cmd) {
            if (value) {
                ASSERT(strcmp(value, cmd) == 0,
                       "peers disagree at index %d: '%s' vs '%s'",
                       index, value, cmd);
                free(cmd);
            } else {
                value = cmd;
            }
            count++;
        }
    }

    if (out_value) *out_value = value;
    else free(value);
    return count;
}

int raft_leader_in(test_cluster_t *tc, int *peers, int npeers,
                   uint64_t *out_term) {
    int best = -1;
    uint64_t best_term = 0;
    for (int pi = 0; pi < npeers; pi++) {
        int i = peers[pi];
        uint64_t term;
        bool is_leader;
        if (test_cluster_get_state_on(tc, i, &term, &is_leader) && is_leader) {
            if (term > best_term) {
                best_term = term;
                best = i;
            }
        }
    }
    if (out_term) *out_term = best_term;
    return best;
}

void raft_check_leader_in(test_cluster_t *tc, int *peers, int npeers,
                          int *out_leader, uint64_t *out_term) {
    for (int attempt = 0; attempt < RAFT_LEADER_WAIT_ATTEMPTS; attempt++) {
        usleep(RAFT_LEADER_WAIT_USEC);
        int leader = raft_leader_in(tc, peers, npeers, out_term);
        if (leader >= 0) {
            *out_leader = leader;
            return;
        }
    }
    ASSERT(0, "no leader elected among specified peers");
}

int raft_one(test_cluster_t *tc, const char *cmd, int expected_servers) {
    int all_peers[10];
    for (int i = 0; i < tc->n; i++) all_peers[i] = i;

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);

    while (1) {
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        double elapsed = (now.tv_sec - start.tv_sec) +
                         (now.tv_nsec - start.tv_nsec) / 1e9;
        ASSERT(elapsed < RAFT_ONE_TIMEOUT_SEC,
               "one(\"%s\") timed out after %.0fs", cmd, RAFT_ONE_TIMEOUT_SEC);

        uint64_t term;
        int leader = raft_leader_in(tc, all_peers, tc->n, &term);
        if (leader >= 0) {
            int index;
            uint64_t t;
            bool ok;
            if (raft_start_on(tc, leader, cmd, &index, &t, &ok) && ok) {
                for (int poll = 0; poll < RAFT_ONE_COMMIT_POLLS; poll++) {
                    usleep(RAFT_ONE_POLL_USEC);
                    int nc = raft_n_committed(tc, index, NULL);
                    if (nc >= expected_servers)
                        return index;
                }
            }
        }
        usleep(RAFT_ONE_POLL_USEC);
    }
}
