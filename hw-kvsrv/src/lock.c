#include "src/lock.h"

#include <stdlib.h>
#include <string.h>

lock_t *lock_new(kv_client_t *client, const char *lockname) {
    lock_t *l = calloc(1, sizeof(lock_t));
    l->client = client;
    // TODO: initialize lock identity and version.
    return l;
}

void lock_free(lock_t *l) {
    free(l);
}

void lock_acquire(lock_t *l) {
    // TODO: acquire the lock through the KV client.
}

void lock_release(lock_t *l) {
    // TODO: release the lock if this client owns it.
}
