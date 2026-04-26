#ifndef RSM_H
#define RSM_H

#include <stdint.h>
#include <stdbool.h>
#include "lib/rpc.h"
#include "lib/kv_types.h"
#include "lib/persister.h"
#include "src/kv_raft/raft.h"

typedef struct rsm rsm_t;

rsm_t *rsm_new(int me, rpc_client_t **peers, int n_peers, persister_t *p);
void rsm_free(rsm_t *r);

raft_t *rsm_raft(rsm_t *r);
void rsm_get_state(rsm_t *r, uint64_t *term, bool *is_leader);

char *rsm_dispatch(void *ctx, const char *method, const char *body, int client_id);

#endif
