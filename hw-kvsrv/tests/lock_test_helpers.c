#include "tests/lock_test_helpers.h"

#include "lib/.internal/pool.h"
#include "src/lock.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

typedef struct {
    int me;
    kv_client_t ck;
    _Atomic int *done;
    _Atomic int *in_cs;
    _Atomic int *started;
    char *lock_name;
    char *check_key;
    uint64_t count;
} lock_worker_args_t;

static void *lock_worker(void *arg) {
    lock_worker_args_t *a = arg;
    lock_t *lk = lock_new(&a->ck, a->lock_name);

    a->count = 0;
    atomic_fetch_add(a->started, 1);
    while (!atomic_load(a->done)) {
        a->count++;
        lock_acquire(lk);

        int prev = atomic_fetch_add(a->in_cs, 1);
        ASSERT(prev == 0, "%d: entered critical section with %d holder(s)",
               a->me, prev);

        char *val = NULL;
        version_t ver = 0;
        kv_err_t err;
        a->ck.get(a->ck.ctx, a->check_key, &val, &ver, &err);
        ASSERT(err == KV_OK, "%d: get failed %s",
               a->me, kv_err_to_str(err));
        ASSERT(val[0] == '\0', "%d: two clients acquired lock, val=%s",
               a->me, val);
        free(val);

        char me_str[16];
        snprintf(me_str, sizeof(me_str), "%d", a->me);
        kv_err_t perr = a->ck.put(a->ck.ctx, a->check_key, me_str, ver);
        ASSERT(perr == KV_OK || perr == KV_MAYBE,
               "%d: put failed %s", a->me, kv_err_to_str(perr));

        usleep(10000);

        int mid = atomic_load(a->in_cs);
        ASSERT(mid == 1, "%d: another client entered critical section", a->me);

        perr = a->ck.put(a->ck.ctx, a->check_key, "", ver + 1);
        ASSERT(perr == KV_OK || perr == KV_MAYBE || perr == KV_VERSION,
               "%d: clear failed %s", a->me, kv_err_to_str(perr));

        atomic_fetch_sub(a->in_cs, 1);
        lock_release(lk);
    }

    lock_free(lk);
    return NULL;
}

void test_run_lock_clients(int n_clients, int duration_sec,
                           test_kv_client_factory_t make_client,
                           test_kv_client_destructor_t free_client,
                           void *ctx) {
    POOL_ENSURE_READY();

    _Atomic int done = 0;
    _Atomic int in_cs = 0;
    _Atomic int started = 0;
    char *lock_name = rand_string(12);
    char *suffix = rand_string(8);
    char check_key[256];
    snprintf(check_key, sizeof(check_key), "mutex-check-%s", suffix);
    free(suffix);

    lock_worker_args_t *args = calloc((size_t)n_clients, sizeof(*args));
    pthread_t *threads = calloc((size_t)n_clients, sizeof(*threads));
    test_kv_client_ref_t *clients = calloc((size_t)n_clients, sizeof(*clients));

    for (int i = 0; i < n_clients; i++)
        clients[i] = make_client(ctx);

    test_put_at_least_once(&clients[0].client, check_key, "", 0);

    for (int i = 0; i < n_clients; i++) {
        args[i].me = i;
        args[i].ck = clients[i].client;
        args[i].done = &done;
        args[i].in_cs = &in_cs;
        args[i].started = &started;
        args[i].lock_name = lock_name;
        args[i].check_key = check_key;
        args[i].count = 0;
        pthread_create(&threads[i], NULL, lock_worker, &args[i]);
    }

    while (atomic_load(&started) < n_clients)
        usleep(10000);

    usleep(duration_sec * 1000000);
    atomic_store(&done, 1);

    for (int i = 0; i < n_clients; i++) {
        pthread_join(threads[i], NULL);
        ASSERT(args[i].count > 0, "client %d did no work", i);
    }

    for (int i = 0; i < n_clients; i++)
        free_client(clients[i].raw);
    free(clients);
    free(args);
    free(threads);
    free(lock_name);
}
