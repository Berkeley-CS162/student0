#include "tests/test_common.h"
#include "tests/kv_test_helpers.h"
#include "tests/lock_test_helpers.h"
#include "src/kv_single/client.h"
#include "src/lock.h"
#include "lib/rpc.h"
#include "lib/kv_types.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>
#include <time.h>

static void test_reliable_put(void) {
    test_kv_t *ts = test_kv_new(network_config_reliable());
    kv_single_client_t *raw = test_kv_make_client(ts);
    kv_client_t ck = kv_single_client_as_kv_client(raw);

    kv_err_t perr = ck.put(ck.ctx, "k", "162", 0);
    ASSERT_EQ_INT(perr, KV_OK, "put k=162 at v0 should be OK, got %s", kv_err_to_str(perr));

    char *val = NULL; version_t ver = 0; kv_err_t err;
    ck.get(ck.ctx, "k", &val, &ver, &err);
    ASSERT_EQ_INT(err, KV_OK, "get k should be OK");
    ASSERT_EQ_STR(val, "162", "get k value mismatch");
    ASSERT_EQ_INT(ver, 1, "get k version should be 1");
    free(val);

    perr = ck.put(ck.ctx, "k", "162", 0);
    ASSERT_EQ_INT(perr, KV_VERSION, "put at wrong version should be Version");

    perr = ck.put(ck.ctx, "y", "162", 1);
    ASSERT_EQ_INT(perr, KV_NO_KEY, "put nonexistent key with ver>0 should be NoKey");

    val = NULL;
    ck.get(ck.ctx, "y", &val, &ver, &err);
    ASSERT_EQ_INT(err, KV_NO_KEY, "get nonexistent key should be NoKey");
    free(val);

    kv_single_client_free(raw);
    test_kv_free(ts);
}

static void test_reliable_multiple_keys(void) {
    test_kv_t *ts = test_kv_new(network_config_reliable());
    kv_single_client_t *raw = test_kv_make_client(ts);
    kv_client_t ck = kv_single_client_as_kv_client(raw);

    ASSERT_EQ_INT(ck.put(ck.ctx, "a", "alpha", 0), KV_OK, "put a=alpha");
    ASSERT_EQ_INT(ck.put(ck.ctx, "b", "beta", 0), KV_OK, "put b=beta");
    ASSERT_EQ_INT(ck.put(ck.ctx, "c", "gamma", 0), KV_OK, "put c=gamma");

    char *val = NULL; version_t ver = 0; kv_err_t err;

    ck.get(ck.ctx, "a", &val, &ver, &err);
    ASSERT_EQ_INT(err, KV_OK, "get a"); ASSERT_EQ_STR(val, "alpha", "get a val"); ASSERT_EQ_INT(ver, 1, "get a ver");
    free(val); val = NULL;

    ck.get(ck.ctx, "b", &val, &ver, &err);
    ASSERT_EQ_INT(err, KV_OK, "get b"); ASSERT_EQ_STR(val, "beta", "get b val"); ASSERT_EQ_INT(ver, 1, "get b ver");
    free(val); val = NULL;

    ck.get(ck.ctx, "c", &val, &ver, &err);
    ASSERT_EQ_INT(err, KV_OK, "get c"); ASSERT_EQ_STR(val, "gamma", "get c val"); ASSERT_EQ_INT(ver, 1, "get c ver");
    free(val); val = NULL;

    ASSERT_EQ_INT(ck.put(ck.ctx, "a", "ALPHA", 1), KV_OK, "put a=ALPHA at v1");
    ck.get(ck.ctx, "a", &val, &ver, &err);
    ASSERT_EQ_STR(val, "ALPHA", "get a after update");
    ASSERT_EQ_INT(ver, 2, "get a ver after update");
    free(val);

    kv_single_client_free(raw);
    test_kv_free(ts);
}

static void test_reliable_version_progression(void) {
    test_kv_t *ts = test_kv_new(network_config_reliable());
    kv_single_client_t *raw = test_kv_make_client(ts);
    kv_client_t ck = kv_single_client_as_kv_client(raw);

    for (uint64_t i = 0; i < 50; i++) {
        char vbuf[32];
        snprintf(vbuf, sizeof(vbuf), "v%llu", (unsigned long long)i);
        ASSERT_EQ_INT(ck.put(ck.ctx, "k", vbuf, i), KV_OK, "put k=v%llu at ver %llu", (unsigned long long)i, (unsigned long long)i);

        char *val = NULL; version_t ver = 0; kv_err_t err;
        ck.get(ck.ctx, "k", &val, &ver, &err);
        ASSERT_EQ_INT(err, KV_OK, "get k at iter %llu", (unsigned long long)i);
        ASSERT_EQ_STR(val, vbuf, "get k value at iter %llu", (unsigned long long)i);
        ASSERT_EQ_INT(ver, i + 1, "get k version at iter %llu", (unsigned long long)i);
        free(val);
    }

    kv_single_client_free(raw);
    test_kv_free(ts);
}

static void test_reliable_empty_value(void) {
    test_kv_t *ts = test_kv_new(network_config_reliable());
    kv_single_client_t *raw = test_kv_make_client(ts);
    kv_client_t ck = kv_single_client_as_kv_client(raw);

    ASSERT_EQ_INT(ck.put(ck.ctx, "k", "hello", 0), KV_OK, "put k=hello");
    ASSERT_EQ_INT(ck.put(ck.ctx, "k", "", 1), KV_OK, "put k=\"\" at v1");

    char *val = NULL; version_t ver = 0; kv_err_t err;
    ck.get(ck.ctx, "k", &val, &ver, &err);
    ASSERT_EQ_INT(err, KV_OK, "get k");
    ASSERT_EQ_STR(val, "", "get k should be empty");
    ASSERT_EQ_INT(ver, 2, "get k version should be 2");
    free(val);

    kv_single_client_free(raw);
    test_kv_free(ts);
}

static void test_reliable_two_clients_conflict(void) {
    test_kv_t *ts = test_kv_new(network_config_reliable());
    kv_single_client_t *raw1 = test_kv_make_client(ts);
    kv_single_client_t *raw2 = test_kv_make_client(ts);
    kv_client_t ck1 = kv_single_client_as_kv_client(raw1);
    kv_client_t ck2 = kv_single_client_as_kv_client(raw2);

    ASSERT_EQ_INT(ck1.put(ck1.ctx, "k", "from-1", 0), KV_OK, "ck1 put k=from-1");

    ASSERT_EQ_INT(ck2.put(ck2.ctx, "k", "from-2", 0), KV_VERSION, "ck2 stale put should be Version");

    char *val = NULL; version_t ver = 0; kv_err_t err;
    ck2.get(ck2.ctx, "k", &val, &ver, &err);
    ASSERT_EQ_INT(ver, 1, "ck2 get ver should be 1");
    free(val); val = NULL;

    ASSERT_EQ_INT(ck2.put(ck2.ctx, "k", "from-2", ver), KV_OK, "ck2 put at fresh version");

    ck1.get(ck1.ctx, "k", &val, &ver, &err);
    ASSERT_EQ_STR(val, "from-2", "ck1 get should see from-2");
    ASSERT_EQ_INT(ver, 2, "ck1 get ver should be 2");
    free(val);

    kv_single_client_free(raw1);
    kv_single_client_free(raw2);
    test_kv_free(ts);
}

static void test_reliable_large_value(void) {
    test_kv_t *ts = test_kv_new(network_config_reliable());
    kv_single_client_t *raw = test_kv_make_client(ts);
    kv_client_t ck = kv_single_client_as_kv_client(raw);

    char *big = malloc(10001);
    memset(big, 'x', 10000);
    big[10000] = '\0';

    ASSERT_EQ_INT(ck.put(ck.ctx, "big", big, 0), KV_OK, "put big value");

    char *val = NULL; version_t ver = 0; kv_err_t err;
    ck.get(ck.ctx, "big", &val, &ver, &err);
    ASSERT_EQ_STR(val, big, "get big value mismatch");
    free(val);
    free(big);

    kv_single_client_free(raw);
    test_kv_free(ts);
}

static void test_reliable_many_keys(void) {
    test_kv_t *ts = test_kv_new(network_config_reliable());
    kv_single_client_t *raw = test_kv_make_client(ts);
    kv_client_t ck = kv_single_client_as_kv_client(raw);

    for (int i = 0; i < 100; i++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "key-%d", i);
        snprintf(val, sizeof(val), "val-%d", i);
        ASSERT_EQ_INT(ck.put(ck.ctx, key, val, 0), KV_OK, "put %s=%s", key, val);
    }
    for (int i = 0; i < 100; i++) {
        char key[32], expected[32];
        snprintf(key, sizeof(key), "key-%d", i);
        snprintf(expected, sizeof(expected), "val-%d", i);

        char *val = NULL; version_t ver = 0; kv_err_t err;
        ck.get(ck.ctx, key, &val, &ver, &err);
        ASSERT_EQ_INT(err, KV_OK, "get %s", key);
        ASSERT_EQ_STR(val, expected, "get %s value", key);
        ASSERT_EQ_INT(ver, 1, "get %s version", key);
        free(val);
    }

    kv_single_client_free(raw);
    test_kv_free(ts);
}

typedef struct {
    kv_client_t ck;
    int client_num;
    bool reliable;
    _Atomic int *done;
    uint64_t nok;
} race_put_args_t;

static void *race_put_worker(void *arg) {
    race_put_args_t *a = (race_put_args_t *)arg;
    a->nok = 0;

    while (!atomic_load(a->done)) {
        char *val = NULL; version_t ver = 0; kv_err_t err;
        a->ck.get(a->ck.ctx, "k", &val, &ver, &err);
        version_t version = (err == KV_NO_KEY) ? 0 : ver;
        free(val);

        char vbuf[64];
        snprintf(vbuf, sizeof(vbuf), "client-%d-%llu",
                 a->client_num, (unsigned long long)a->nok);

        kv_err_t perr = a->ck.put(a->ck.ctx, "k", vbuf, version);
        if (perr == KV_OK) {
            a->nok++;
        } else if (perr == KV_VERSION || (!a->reliable && perr == KV_MAYBE)) {
        } else {
            ASSERT(0, "unexpected error: %s", kv_err_to_str(perr));
        }
    }
    return NULL;
}

static void race_put_test(int n_clients, int duration_sec, bool reliable) {
    test_kv_t *ts = test_kv_new(reliable
        ? network_config_reliable()
        : network_config_unreliable());

    _Atomic int done = 0;
    race_put_args_t *args = calloc(n_clients, sizeof(*args));
    pthread_t *threads = calloc(n_clients, sizeof(*threads));
    kv_single_client_t **raws = calloc(n_clients, sizeof(*raws));

    for (int i = 0; i < n_clients; i++) {
        raws[i] = test_kv_make_client(ts);
        args[i].ck = kv_single_client_as_kv_client(raws[i]);
        args[i].client_num = i;
        args[i].reliable = reliable;
        args[i].done = &done;
        args[i].nok = 0;
        pthread_create(&threads[i], NULL, race_put_worker, &args[i]);
    }

    usleep(duration_sec * 1000000);
    atomic_store(&done, 1);

    uint64_t total_ok = 0;
    for (int i = 0; i < n_clients; i++) {
        pthread_join(threads[i], NULL);
        total_ok += args[i].nok;
        if (!reliable)
            ASSERT(args[i].nok > 0, "client %d did no work on unreliable network", i);
    }

    if (reliable) {
        kv_single_client_t *raw_check = test_kv_make_client(ts);
        kv_client_t ck_check = kv_single_client_as_kv_client(raw_check);
        char *val = NULL; version_t ver = 0; kv_err_t err;
        ck_check.get(ck_check.ctx, "k", &val, &ver, &err);
        ASSERT_EQ_INT(err, KV_OK, "final get should be OK");
        ASSERT_EQ_INT(ver, total_ok, "version %llu != total successful puts %llu",
                      (unsigned long long)ver, (unsigned long long)total_ok);
        ASSERT(total_ok > 0, "no puts succeeded");
        free(val);
        kv_single_client_free(raw_check);
    }

    for (int i = 0; i < n_clients; i++)
        kv_single_client_free(raws[i]);
    free(raws);
    free(args);
    free(threads);
    test_kv_free(ts);
}

static void test_concurrent_2_clients(void) {
    race_put_test(2, 4, true);
}

static void test_concurrent_5_clients(void) {
    race_put_test(5, 4, true);
}

static void test_unreliable_net(void) {
    test_kv_t *ts = test_kv_new(network_config_unreliable());
    kv_single_client_t *raw = test_kv_make_client(ts);
    kv_client_t ck = kv_single_client_as_kv_client(raw);

    int retried = 0;
    for (uint64_t try_num = 0; try_num < 100; try_num++) {
        for (uint64_t i = 0; ; i++) {
            char ibuf[32];
            snprintf(ibuf, sizeof(ibuf), "%llu", (unsigned long long)i);
            kv_err_t err = ck.put(ck.ctx, "k", ibuf, try_num);
            if (err != KV_MAYBE) {
                if (i > 0) {
                    ASSERT_EQ_INT(err, KV_VERSION, "put shouldn't have happened more than once");
                }
                break;
            }
            retried = 1;
        }
        char *val = NULL; version_t ver = 0; kv_err_t gerr;
        ck.get(ck.ctx, "k", &val, &ver, &gerr);
        ASSERT_EQ_INT(gerr, KV_OK, "get after put should be OK");
        ASSERT_EQ_INT(ver, try_num + 1, "wrong version at try %llu", (unsigned long long)try_num);
        ASSERT_EQ_STR(val, "0", "wrong value at try %llu", (unsigned long long)try_num);
        free(val);
    }
    ASSERT(retried, "Clerk::put never returned ErrMaybe");

    kv_single_client_free(raw);
    test_kv_free(ts);
}

static void test_unreliable_2_clients(void) {
    race_put_test(2, 4, false);
}

static void test_unreliable_5_clients(void) {
    race_put_test(5, 4, false);
}

static void test_lock_basic(void) {
    test_kv_t *ts = test_kv_new(network_config_reliable());
    kv_single_client_t *raw = test_kv_make_client(ts);
    kv_client_t ck = kv_single_client_as_kv_client(raw);

    char *name = rand_string(12);
    lock_t *lk = lock_new(&ck, name);

    lock_acquire(lk);
    lock_release(lk);

    lock_free(lk);
    free(name);
    kv_single_client_free(raw);
    test_kv_free(ts);
}

static void test_lock_reacquire(void) {
    test_kv_t *ts = test_kv_new(network_config_reliable());
    kv_single_client_t *raw = test_kv_make_client(ts);
    kv_client_t ck = kv_single_client_as_kv_client(raw);

    char *name = rand_string(12);
    lock_t *lk = lock_new(&ck, name);

    for (int i = 0; i < 20; i++) {
        lock_acquire(lk);
        lock_release(lk);
    }

    lock_free(lk);
    free(name);
    kv_single_client_free(raw);
    test_kv_free(ts);
}

static void test_lock_nested(void) {
    test_kv_t *ts = test_kv_new(network_config_reliable());
    kv_single_client_t *raw = test_kv_make_client(ts);
    kv_client_t ck = kv_single_client_as_kv_client(raw);

    char *name1 = rand_string(12);
    char *name2 = rand_string(12);

    lock_t *lk1 = lock_new(&ck, name1);
    lock_t *lk2 = lock_new(&ck, name2);

    lock_acquire(lk1);
    lock_acquire(lk2);
    lock_release(lk2);
    lock_release(lk1);

    lock_acquire(lk2);
    lock_acquire(lk1);
    lock_release(lk1);
    lock_acquire(lk1);
    lock_release(lk2);
    lock_release(lk1);

    lock_free(lk1);
    lock_free(lk2);
    free(name1);
    free(name2);
    kv_single_client_free(raw);
    test_kv_free(ts);
}

static test_kv_client_ref_t make_single_lock_client(void *ctx) {
    test_kv_t *ts = ctx;
    kv_single_client_t *raw = test_kv_make_client(ts);
    return (test_kv_client_ref_t){
        .client = kv_single_client_as_kv_client(raw),
        .raw = raw,
    };
}

static void free_single_lock_client(void *raw) {
    kv_single_client_free(raw);
}

static void run_lock_clients(int n_clients, network_config_t config,
                             int duration_sec) {
    test_kv_t *ts = test_kv_new(config);
    test_run_lock_clients(n_clients, duration_sec, make_single_lock_client,
                          free_single_lock_client, ts);
    test_kv_free(ts);
}

static void test_lock_1_client_reliable(void) {
    run_lock_clients(1, network_config_reliable(), 4);
}

static void test_lock_2_clients_reliable(void) {
    run_lock_clients(2, network_config_reliable(), 4);
}

static void test_lock_5_clients_reliable(void) {
    run_lock_clients(5, network_config_reliable(), 4);
}

static void test_lock_1_client_unreliable(void) {
    run_lock_clients(1, network_config_unreliable(), 4);
}

static void test_lock_2_clients_unreliable(void) {
    run_lock_clients(2, network_config_unreliable(), 4);
}

int main(int argc, char **argv) {
    srand(time(NULL) ^ getpid());
    test_common_set_filter(argc > 1 ? argv[1] : NULL);

    RUN_TEST(test_reliable_put);
    RUN_TEST(test_reliable_multiple_keys);
    RUN_TEST(test_reliable_version_progression);
    RUN_TEST(test_reliable_empty_value);
    RUN_TEST(test_reliable_two_clients_conflict);
    RUN_TEST(test_reliable_large_value);
    RUN_TEST(test_reliable_many_keys);
    RUN_TEST(test_concurrent_2_clients);
    RUN_TEST(test_concurrent_5_clients);
    RUN_TEST(test_unreliable_net);
    RUN_TEST(test_unreliable_2_clients);
    RUN_TEST(test_unreliable_5_clients);
    RUN_TEST(test_lock_basic);
    RUN_TEST(test_lock_reacquire);
    RUN_TEST(test_lock_nested);
    RUN_TEST(test_lock_1_client_reliable);
    RUN_TEST(test_lock_2_clients_reliable);
    RUN_TEST(test_lock_5_clients_reliable);
    RUN_TEST(test_lock_1_client_unreliable);
    RUN_TEST(test_lock_2_clients_unreliable);

    if (test_common_ran_count() == 0) {
        fprintf(stderr, "no tests matched filter \"%s\"\n", argv[1]);
        return 1;
    }
    printf("\nAll kvsrv tests passed!\n");
    return 0;
}
