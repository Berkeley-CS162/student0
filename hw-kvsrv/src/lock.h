#ifndef LOCK_H
#define LOCK_H

#include "lib/kv_types.h"

typedef struct lock {
    kv_client_t *client;
    // TODO: add lock name, holder id, and version.
} lock_t;

lock_t *lock_new(kv_client_t *client, const char *lockname);
void lock_free(lock_t *l);
void lock_acquire(lock_t *l);
void lock_release(lock_t *l);

#endif
