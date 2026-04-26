#include "src/kv_raft/client.h"
#include "lib/cjson/cJSON.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdatomic.h>

struct kvraft_client {
    rpc_client_t **servers;
    int n_servers;
    // TODO: cache leader and client state.
};

kvraft_client_t *kvraft_client_new(rpc_client_t **servers, int n_servers) {
    kvraft_client_t *c = calloc(1, sizeof(kvraft_client_t));
    c->servers = malloc(n_servers * sizeof(rpc_client_t *));
    memcpy(c->servers, servers, n_servers * sizeof(rpc_client_t *));
    c->n_servers = n_servers;
    return c;
}

void kvraft_client_free(kvraft_client_t *c) {
    if (!c) return;
    free(c->servers);
    free(c);
}

static void client_get(void *ctx, const char *key,
                       char **out_value, version_t *out_version, kv_err_t *out_err) {
    kvraft_client_t *c = (kvraft_client_t *)ctx;
    (void)c;

    // TODO: try servers until a get succeeds.

    *out_value = strdup("");
    *out_version = 0;
    *out_err = KV_NO_KEY;
}

static kv_err_t client_put(void *ctx, const char *key, const char *value, version_t ver) {
    kvraft_client_t *c = (kvraft_client_t *)ctx;
    (void)c;

    // TODO: retry puts and report Maybe after uncertain failures.

    return KV_OK;
}

kv_client_t kvraft_client_as_kv_client(kvraft_client_t *c) {
    kv_client_t out;
    out.ctx = c;
    out.get = client_get;
    out.put = client_put;
    return out;
}
