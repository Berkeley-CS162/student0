#ifndef RPC_H
#define RPC_H

#include <stdint.h>

/* Returns an owned response body. */
typedef char *(*rpc_dispatch_fn)(void *ctx, const char *method, const char *body, int client_id);

/* Start HTTP server on 127.0.0.1:0 and return the bound port. */
uint16_t rpc_server_start(void *ctx, rpc_dispatch_fn dispatch);

typedef struct {
    char host[64];
    uint16_t port;
    int client_id;
} rpc_client_t;

rpc_client_t *rpc_client_new(const char *url, int client_id);
void rpc_client_free(rpc_client_t *c);

/* Returns an owned response body, or NULL. */
char *rpc_call(rpc_client_t *c, const char *method, const char *body);

/* Same as rpc_call with a custom timeout. */
char *rpc_call_timeout(rpc_client_t *c, const char *method, const char *body, int timeout_sec);

#endif
