#ifndef KV_SINGLE_CLIENT_H
#define KV_SINGLE_CLIENT_H

#include "lib/rpc.h"
#include "lib/kv_types.h"

typedef struct kv_single_client kv_single_client_t;

kv_single_client_t *kv_single_client_new(rpc_client_t *endpoint);
void kv_single_client_free(kv_single_client_t *c);

kv_client_t kv_single_client_as_kv_client(kv_single_client_t *c);

#endif
