#include "src/kv_raft/rsm.h"
#include "lib/cjson/cJSON.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdatomic.h>

struct rsm {
    int me;
    raft_t *raft;
    // TODO: add KV state and applied-command cache.
};

rsm_t *rsm_new(int me, rpc_client_t **peers, int n_peers, persister_t *p) {
    rsm_t *r = calloc(1, sizeof(rsm_t));
    r->me = me;
    r->raft = raft_new(me, peers, n_peers, p);
    // TODO: initialize RSM state.
    return r;
}

void rsm_free(rsm_t *r) {
    if (!r) return;
    raft_free(r->raft);
    // TODO: free RSM state.
    free(r);
}

raft_t *rsm_raft(rsm_t *r) {
    return r->raft;
}

void rsm_get_state(rsm_t *r, uint64_t *term, bool *is_leader) {
    raft_get_state(r->raft, term, is_leader);
}

static char *rsm_handle_get(rsm_t *r, const char *body) {
    (void)r; (void)body;
    // TODO: submit get through Raft.
    return strdup("{\"value\":\"\",\"version\":0,\"err\":\"WrongLeader\"}");
}

static char *rsm_handle_put(rsm_t *r, const char *body) {
    (void)r; (void)body;
    // TODO: submit put through Raft.
    return strdup("{\"err\":\"WrongLeader\"}");
}

char *rsm_dispatch(void *ctx, const char *method, const char *body, int client_id) {
    rsm_t *r = (rsm_t *)ctx;

    if (strcmp(method, "get") == 0)
        return rsm_handle_get(r, body);
    if (strcmp(method, "put") == 0)
        return rsm_handle_put(r, body);

    return raft_dispatch(r->raft, method, body, client_id);
}
