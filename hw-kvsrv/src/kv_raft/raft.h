#ifndef RAFT_H
#define RAFT_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "lib/rpc.h"
#include "lib/persister.h"

/* TODO: define RPC payloads and log entries. */

typedef struct raft {
    int me;
    int n_peers;
    rpc_client_t **peers;
    persister_t *persister;
    /* TODO: add persistent and volatile Raft state. */
} raft_t;

/* Create a Raft peer. */
raft_t *raft_new(int me, rpc_client_t **peers, int n_peers, persister_t *p);
void raft_free(raft_t *r);

/* Return (term, is_leader). */
void raft_get_state(raft_t *r, uint64_t *term, bool *is_leader);

/* Submit a command. */
int raft_submit(raft_t *r, const char *cmd, uint64_t *term, bool *is_leader);

/* Return a committed command. */
char *raft_get_committed(raft_t *r, int index);

char *raft_dispatch(void *ctx, const char *method, const char *body, int client_id);

#endif
