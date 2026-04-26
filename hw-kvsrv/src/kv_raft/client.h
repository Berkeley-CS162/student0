#ifndef KVRAFT_CLIENT_H
#define KVRAFT_CLIENT_H

#include "lib/rpc.h"
#include "lib/kv_types.h"

typedef struct kvraft_client kvraft_client_t;

kvraft_client_t *kvraft_client_new(rpc_client_t **servers, int n_servers);
void kvraft_client_free(kvraft_client_t *c);
kv_client_t kvraft_client_as_kv_client(kvraft_client_t *c);

#endif
