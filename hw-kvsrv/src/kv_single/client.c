#include "src/kv_single/client.h"
#include "lib/cjson/cJSON.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct kv_single_client {
    rpc_client_t *endpoint;
};

kv_single_client_t *kv_single_client_new(rpc_client_t *endpoint) {
    kv_single_client_t *c = calloc(1, sizeof(kv_single_client_t));
    c->endpoint = endpoint;
    return c;
}

void kv_single_client_free(kv_single_client_t *c) {
    if (c) {
        rpc_client_free(c->endpoint);
        free(c);
    }
}

static void client_get(void *ctx, const char *key,
                       char **out_value, version_t *out_version, kv_err_t *out_err) {
    kv_single_client_t *c = (kv_single_client_t *)ctx;
    (void)c;

    /* TODO: call get and decode the reply. */

    *out_value = strdup("");
    *out_version = 0;
    *out_err = KV_NO_KEY;
}

static kv_err_t client_put(void *ctx, const char *key, const char *value, version_t ver) {
    kv_single_client_t *c = (kv_single_client_t *)ctx;
    (void)c;

    /* TODO: call put and decode the reply. */

    return KV_OK;
}

kv_client_t kv_single_client_as_kv_client(kv_single_client_t *c) {
    kv_client_t out;
    out.ctx = c;
    out.get = client_get;
    out.put = client_put;
    return out;
}
