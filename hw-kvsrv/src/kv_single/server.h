#ifndef KV_SINGLE_SERVER_H
#define KV_SINGLE_SERVER_H

#include "lib/rpc.h"

typedef struct kv_server kv_server_t;

kv_server_t *kv_server_new(void);
void kv_server_free(kv_server_t *s);

char *kv_server_dispatch(void *ctx, const char *method, const char *body, int client_id);

#endif
