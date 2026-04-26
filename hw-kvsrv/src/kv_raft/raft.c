#include "src/kv_raft/raft.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

/* TODO: define RPC payloads and log entries. */

raft_t *raft_new(int me, rpc_client_t **peers, int n_peers, persister_t *p) {
    raft_t *r = calloc(1, sizeof(raft_t));
    r->me = me;
    r->n_peers = n_peers;
    r->peers = peers;
    r->persister = p;

    /* TODO: initialize Raft state and start background threads. */

    return r;
}

void raft_free(raft_t *r) {
    if (!r) return;
    /* TODO: free Raft state. */
    free(r);
}

void raft_get_state(raft_t *r, uint64_t *term, bool *is_leader) {
    /* TODO: return current term and leadership state. */
    *term = 0;
    *is_leader = false;
}

int raft_submit(raft_t *r, const char *cmd, uint64_t *term, bool *is_leader) {
    /* TODO: append cmd if leader and return its 1-based index. */
    *term = 0;
    *is_leader = false;
    return 0;
}

char *raft_get_committed(raft_t *r, int index) {
    /* TODO: return a copy of the committed command at index. */
    return NULL;
}

char *raft_dispatch(void *ctx, const char *method, const char *body, int client_id) {
    (void)ctx;
    (void)client_id;

    return strdup("{\"error\":\"unknown method\"}");
}
