#ifndef RAFT_TEST_HELPERS_H
#define RAFT_TEST_HELPERS_H

#include "tests/test_common.h"

bool raft_start_on(test_cluster_t *tc, int i, const char *cmd,
                   int *out_index, uint64_t *out_term, bool *out_is_leader);
void raft_start_or_default(test_cluster_t *tc, int peer, const char *cmd,
                           int *out_index, uint64_t *out_term,
                           bool *out_is_leader);
bool raft_get_committed_on(test_cluster_t *tc, int i, int index,
                           char **out_cmd);
void raft_check_one_leader(test_cluster_t *tc, int *out_leader,
                           uint64_t *out_term);
int raft_n_committed(test_cluster_t *tc, int index, char **out_value);
int raft_leader_in(test_cluster_t *tc, int *peers, int npeers,
                   uint64_t *out_term);
void raft_check_leader_in(test_cluster_t *tc, int *peers, int npeers,
                          int *out_leader, uint64_t *out_term);
int raft_one(test_cluster_t *tc, const char *cmd, int expected_servers);

#endif
