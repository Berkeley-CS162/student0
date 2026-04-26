#include "tests/test_common.h"
#include "tests/kv_test_helpers.h"
#include "tests/lock_test_helpers.h"
#include "src/kv_raft/client.h"
#include "src/lock.h"
#include "lib/rpc.h"
#include "lib/kv_types.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <stdatomic.h>
#include <pthread.h>

static void test_basic_kvraft(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());
    kvraft_client_t *raw = test_cluster_make_kvraft_client(tc);
    kv_client_t ck = kvraft_client_as_kv_client(raw);

    char *val = NULL;
    version_t ver;
    kv_err_t err;
    ck.get(ck.ctx, "missing", &val, &ver, &err);
    ASSERT(err == KV_NO_KEY, "get(missing) expected NoKey, got %d", err);
    free(val);

    ver = test_put_at_least_once(&ck, "k", "v1", 0);
    ASSERT(ver == 1, "put v1 expected ver 1, got %llu", (unsigned long long)ver);

    test_check_get(&ck, "k", "v1", 1);

    ver = test_put_at_least_once(&ck, "k", "v2", 1);
    ASSERT(ver == 2, "put v2 expected ver 2, got %llu", (unsigned long long)ver);

    test_check_get(&ck, "k", "v2", 2);

    kvraft_client_free(raw);
    test_cluster_free(tc);
}

typedef struct {
    kv_client_t ck;
    int cli;
} concurrent_kvraft_args_t;

static void *concurrent_kvraft_worker(void *arg) {
    concurrent_kvraft_args_t *a = arg;
    char key[32];
    snprintf(key, sizeof(key), "key-%d", a->cli);

    version_t ver = test_put_at_least_once(&a->ck, key, "v0", 0);

    for (int i = 1; i <= 5; i++) {
        char val[32];
        snprintf(val, sizeof(val), "v%d", i);
        ver = test_put_at_least_once(&a->ck, key, val, ver);
    }

    char *got_val = NULL;
    version_t got_ver;
    kv_err_t err;
    a->ck.get(a->ck.ctx, key, &got_val, &got_ver, &err);
    ASSERT(err == KV_OK, "cli %d: final get err %d", a->cli, err);
    ASSERT(strcmp(got_val, "v5") == 0,
           "cli %d: expected v5, got %s", a->cli, got_val);
    ASSERT(got_ver == ver,
           "cli %d: version mismatch %llu vs %llu",
           a->cli, (unsigned long long)got_ver, (unsigned long long)ver);
    free(got_val);
    return NULL;
}

static void test_concurrent_kvraft(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());

    pthread_t threads[5];
    concurrent_kvraft_args_t args[5];
    kvraft_client_t *raws[5];

    for (int i = 0; i < 5; i++) {
        raws[i] = test_cluster_make_kvraft_client(tc);
        args[i].ck = kvraft_client_as_kv_client(raws[i]);
        args[i].cli = i;
        pthread_create(&threads[i], NULL, concurrent_kvraft_worker, &args[i]);
    }

    for (int i = 0; i < 5; i++) {
        pthread_join(threads[i], NULL);
        kvraft_client_free(raws[i]);
    }

    test_cluster_free(tc);
}

static void test_leader_failure_kvraft(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());
    kvraft_client_t *raw = test_cluster_make_kvraft_client(tc);
    kv_client_t ck = kvraft_client_as_kv_client(raw);

    test_put_at_least_once(&ck, "k", "v1", 0);
    test_check_get(&ck, "k", "v1", 1);

    int leader = test_cluster_find_leader(tc);
    ASSERT(leader >= 0, "no leader found");
    test_cluster_kill(tc, leader);

    test_put_at_least_once(&ck, "k", "v2", 1);
    test_check_get(&ck, "k", "v2", 2);

    kvraft_client_free(raw);
    test_cluster_free(tc);
}

static void test_persist_kvraft(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());
    kvraft_client_t *raw = test_cluster_make_kvraft_client(tc);
    kv_client_t ck = kvraft_client_as_kv_client(raw);

    test_put_at_least_once(&ck, "k", "v1", 0);
    test_check_get(&ck, "k", "v1", 1);

    for (int i = 0; i < 5; i++)
        test_cluster_kill(tc, i);
    for (int i = 0; i < 5; i++)
        test_cluster_restart(tc, i);

    usleep(2000000); /* 2s */

    test_check_get(&ck, "k", "v1", 1);
    test_put_at_least_once(&ck, "k", "v2", 1);
    test_check_get(&ck, "k", "v2", 2);

    kvraft_client_free(raw);
    test_cluster_free(tc);
}

static void test_partition_kvraft(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());
    kvraft_client_t *raw = test_cluster_make_kvraft_client(tc);
    kv_client_t ck = kvraft_client_as_kv_client(raw);

    test_put_at_least_once(&ck, "k", "v1", 0);

    int group_a[] = {0, 1, 2};
    int group_b[] = {3, 4};
    test_cluster_partition(tc, group_a, 3, group_b, 2);

    int ck_maj_id;
    kvraft_client_t *raw_maj = test_cluster_make_kvraft_client_with_id(tc, &ck_maj_id);
    kv_client_t ck_maj = kvraft_client_as_kv_client(raw_maj);
    for (int j = 3; j < 5; j++)
        server_proxy_block_client(tc->proxies[j], ck_maj_id);

    test_put_at_least_once(&ck_maj, "k", "v2", 1);
    test_check_get(&ck_maj, "k", "v2", 2);

    test_cluster_heal(tc);
    usleep(2000000); /* 2s */

    test_check_get(&ck, "k", "v2", 2);

    kvraft_client_free(raw_maj);
    kvraft_client_free(raw);
    test_cluster_free(tc);
}

static void test_unreliable_kvraft(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_unreliable());
    kvraft_client_t *raw = test_cluster_make_kvraft_client(tc);
    kv_client_t ck = kvraft_client_as_kv_client(raw);

    version_t ver = test_put_at_least_once(&ck, "k", "v1", 0);
    test_check_get(&ck, "k", "v1", ver);

    ver = test_put_at_least_once(&ck, "k", "v2", ver);
    test_check_get(&ck, "k", "v2", ver);

    kvraft_client_free(raw);
    test_cluster_free(tc);
}

typedef struct {
    kv_client_t ck;
    int client_id;
    _Atomic int *done;
    char final_val[64];
    version_t final_ver;
} client_worker_args_t;

static void *client_worker(void *arg) {
    client_worker_args_t *a = arg;
    char key[32];
    snprintf(key, sizeof(key), "key-%d", a->client_id);
    uint64_t i = 0;

    char *val = NULL;
    version_t ver;
    kv_err_t err;
    a->ck.get(a->ck.ctx, key, &val, &ver, &err);
    if (err == KV_NO_KEY) {
        ver = test_put_at_least_once(&a->ck, key, "0", 0);
    }
    free(val);

    snprintf(a->final_val, sizeof(a->final_val), "%llu", (unsigned long long)i);
    a->final_ver = ver;

    while (!atomic_load(a->done)) {
        i++;
        char new_val[64];
        snprintf(new_val, sizeof(new_val), "%llu", (unsigned long long)i);
        ver = test_put_at_least_once(&a->ck, key, new_val, ver);
        snprintf(a->final_val, sizeof(a->final_val), "%s", new_val);
        a->final_ver = ver;
    }
    return NULL;
}

typedef struct {
    test_cluster_t *tc;
    _Atomic int *done;
} partition_chaos_args_t;

static void *partition_chaos_worker(void *arg) {
    partition_chaos_args_t *a = arg;
    while (!atomic_load(a->done)) {
        int srv = rand() % a->tc->n;
        int d1 = 50 + rand() % 100;  /* 50..149 ms */
        int d2 = 50 + rand() % 100;

        test_cluster_disconnect(a->tc, srv);
        usleep(d1 * 1000);
        test_cluster_reconnect(a->tc, srv);
        usleep(d2 * 1000);
    }
    test_cluster_heal(a->tc);
    return NULL;
}

static void generic_test(int nservers, int nclients, bool reliable,
                         bool crash, bool partitions) {
    network_config_t config = reliable
        ? network_config_reliable()
        : network_config_unreliable();
    test_cluster_t *tc = test_cluster_new(nservers, config);

    client_worker_args_t *final_states = calloc(nclients, sizeof(client_worker_args_t));

    for (int iter = 0; iter < 3; iter++) {
        _Atomic int done = 0;
        pthread_t *threads = malloc(nclients * sizeof(pthread_t));
        client_worker_args_t *args = malloc(nclients * sizeof(client_worker_args_t));
        kvraft_client_t **raws = malloc(nclients * sizeof(kvraft_client_t *));

        for (int cli = 0; cli < nclients; cli++) {
            raws[cli] = test_cluster_make_kvraft_client(tc);
            args[cli].ck = kvraft_client_as_kv_client(raws[cli]);
            args[cli].client_id = cli;
            args[cli].done = &done;
            args[cli].final_ver = 0;
            memset(args[cli].final_val, 0, sizeof(args[cli].final_val));
            pthread_create(&threads[cli], NULL, client_worker, &args[cli]);
        }

        pthread_t partition_thread;
        partition_chaos_args_t pargs;
        bool has_partition = false;
        if (partitions) {
            pargs.tc = tc;
            pargs.done = &done;
            pthread_create(&partition_thread, NULL, partition_chaos_worker, &pargs);
            has_partition = true;
        }

        usleep(1000000); /* 1s */
        atomic_store(&done, 1);

        for (int cli = 0; cli < nclients; cli++) {
            pthread_join(threads[cli], NULL);
            final_states[cli] = args[cli];
        }

        if (has_partition)
            pthread_join(partition_thread, NULL);

        for (int cli = 0; cli < nclients; cli++)
            kvraft_client_free(raws[cli]);

        if (partitions) {
            test_cluster_heal(tc);
            usleep(1000000);
        }

        if (crash) {
            for (int i = 0; i < nservers; i++)
                test_cluster_kill(tc, i);
            usleep(500000);
            for (int i = 0; i < nservers; i++)
                test_cluster_restart(tc, i);
            usleep(2000000);
        }

        free(threads);
        free(args);
        free(raws);
    }

    kvraft_client_t *raw = test_cluster_make_kvraft_client(tc);
    kv_client_t ck = kvraft_client_as_kv_client(raw);
    for (int cli = 0; cli < nclients; cli++) {
        if (final_states[cli].final_ver > 0) {
            char key[32];
            snprintf(key, sizeof(key), "key-%d", cli);
            test_check_get(&ck, key, final_states[cli].final_val,
                      final_states[cli].final_ver);
        }
    }

    kvraft_client_free(raw);
    free(final_states);
    test_cluster_free(tc);
}

static void test_many_partitions_one_client(void) {
    generic_test(5, 1, true, false, true);
}

static void test_many_partitions_many_clients(void) {
    generic_test(5, 5, true, false, true);
}

static void test_persist_concurrent(void) {
    generic_test(5, 5, true, true, false);
}

static void test_persist_partition(void) {
    generic_test(5, 3, true, true, true);
}

static test_kv_client_ref_t make_kvraft_lock_client(void *ctx) {
    test_cluster_t *tc = ctx;
    kvraft_client_t *raw = test_cluster_make_kvraft_client(tc);
    return (test_kv_client_ref_t){
        .client = kvraft_client_as_kv_client(raw),
        .raw = raw,
    };
}

static void free_kvraft_lock_client(void *raw) {
    kvraft_client_free(raw);
}

static void test_lock_basic_kvraft(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());
    kvraft_client_t *raw = test_cluster_make_kvraft_client(tc);
    kv_client_t ck = kvraft_client_as_kv_client(raw);

    char *name = rand_string(12);
    lock_t *lk = lock_new(&ck, name);

    lock_acquire(lk);
    lock_release(lk);

    lock_free(lk);
    free(name);
    kvraft_client_free(raw);
    test_cluster_free(tc);
}

static void test_lock_reacquire_kvraft(void) {
    test_cluster_t *tc = test_cluster_new(3, network_config_reliable());
    kvraft_client_t *raw = test_cluster_make_kvraft_client(tc);
    kv_client_t ck = kvraft_client_as_kv_client(raw);

    char *name = rand_string(12);
    lock_t *lk = lock_new(&ck, name);

    for (int i = 0; i < 10; i++) {
        lock_acquire(lk);
        lock_release(lk);
    }

    lock_free(lk);
    free(name);
    kvraft_client_free(raw);
    test_cluster_free(tc);
}

static void test_lock_mutual_exclusion_kvraft(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());
    test_run_lock_clients(3, 3, make_kvraft_lock_client,
                          free_kvraft_lock_client, tc);
    test_cluster_free(tc);
}

static void run_lock_clients_kvraft(int n_clients, network_config_t config,
                                    int duration_sec) {
    test_cluster_t *tc = test_cluster_new(5, config);
    test_run_lock_clients(n_clients, duration_sec, make_kvraft_lock_client,
                          free_kvraft_lock_client, tc);
    test_cluster_free(tc);
}

static void test_lock_1_client_reliable_kvraft(void) {
    run_lock_clients_kvraft(1, network_config_reliable(), 4);
}

static void test_lock_2_clients_reliable_kvraft(void) {
    run_lock_clients_kvraft(2, network_config_reliable(), 4);
}

static void test_lock_5_clients_reliable_kvraft(void) {
    run_lock_clients_kvraft(5, network_config_reliable(), 4);
}

static void test_lock_1_client_unreliable_kvraft(void) {
    run_lock_clients_kvraft(1, network_config_unreliable(), 4);
}

static void test_lock_2_clients_unreliable_kvraft(void) {
    run_lock_clients_kvraft(2, network_config_unreliable(), 4);
}

static void test_lock_32_clients_reliable_kvraft(void) {
    run_lock_clients_kvraft(32, network_config_reliable(), 6);
}

static void test_lock_8_clients_unreliable_kvraft(void) {
    run_lock_clients_kvraft(8, network_config_unreliable(), 8);
}

static void test_lock_with_leader_failure_kvraft(void) {
    test_cluster_t *tc = test_cluster_new(5, network_config_reliable());
    kvraft_client_t *raw = test_cluster_make_kvraft_client(tc);
    kv_client_t ck = kvraft_client_as_kv_client(raw);

    char *name = rand_string(12);
    lock_t *lk = lock_new(&ck, name);

    lock_acquire(lk);

    usleep(500000); /* 500ms */
    int leader = test_cluster_find_leader(tc);
    ASSERT(leader >= 0, "no leader found before kill");
    test_cluster_kill(tc, leader);

    lock_release(lk);

    lock_acquire(lk);
    lock_release(lk);

    lock_free(lk);

    kvraft_client_t *raw2 = test_cluster_make_kvraft_client(tc);
    kv_client_t ck2 = kvraft_client_as_kv_client(raw2);
    lock_t *lk2 = lock_new(&ck2, name);
    lock_acquire(lk2);
    lock_release(lk2);

    lock_free(lk2);
    free(name);
    kvraft_client_free(raw2);
    kvraft_client_free(raw);
    test_cluster_free(tc);
}

typedef struct {
    test_cluster_t *tc;
    _Atomic int *done;
    int n_servers;
} crash_chaos_args_t;

static void *crash_chaos_worker(void *arg) {
    crash_chaos_args_t *a = arg;
    bool alive[16];
    for (int i = 0; i < a->n_servers; i++)
        alive[i] = true;

    while (!atomic_load(a->done)) {
        int target = rand() % a->n_servers;
        int sleep_ms = 200 + rand() % 300; /* 200..499 ms */

        int alive_count = 0;
        for (int i = 0; i < a->n_servers; i++)
            if (alive[i]) alive_count++;

        if (alive[target] && alive_count > 3) {
            test_cluster_kill(a->tc, target);
            alive[target] = false;
        } else if (!alive[target]) {
            test_cluster_restart(a->tc, target);
            alive[target] = true;
        }

        usleep(sleep_ms * 1000);
    }

    for (int i = 0; i < a->n_servers; i++) {
        if (!alive[i])
            test_cluster_restart(a->tc, i);
    }

    return NULL;
}

static void test_lock_with_crash_kvraft(void) {
    int n_servers = 5;
    test_cluster_t *tc = test_cluster_new(n_servers, network_config_reliable());
    _Atomic int done = 0;

    crash_chaos_args_t chaos_args;
    chaos_args.tc = tc;
    chaos_args.done = &done;
    chaos_args.n_servers = n_servers;
    pthread_t chaos_thread;
    pthread_create(&chaos_thread, NULL, crash_chaos_worker, &chaos_args);

    test_run_lock_clients(2, 5, make_kvraft_lock_client,
                          free_kvraft_lock_client, tc);
    atomic_store(&done, 1);

    pthread_join(chaos_thread, NULL);
    test_cluster_free(tc);
}

int main(int argc, char **argv) {
    srand(time(NULL) ^ getpid());
    test_common_set_filter(argc > 1 ? argv[1] : NULL);

    RUN_TEST(test_basic_kvraft);
    RUN_TEST(test_concurrent_kvraft);
    RUN_TEST(test_leader_failure_kvraft);
    RUN_TEST(test_persist_kvraft);
    RUN_TEST(test_partition_kvraft);
    RUN_TEST(test_unreliable_kvraft);
    RUN_TEST(test_many_partitions_one_client);
    RUN_TEST(test_many_partitions_many_clients);
    RUN_TEST(test_persist_concurrent);
    RUN_TEST(test_persist_partition);
    RUN_TEST(test_lock_basic_kvraft);
    RUN_TEST(test_lock_reacquire_kvraft);
    RUN_TEST(test_lock_mutual_exclusion_kvraft);
    RUN_TEST(test_lock_1_client_reliable_kvraft);
    RUN_TEST(test_lock_2_clients_reliable_kvraft);
    RUN_TEST(test_lock_5_clients_reliable_kvraft);
    RUN_TEST(test_lock_1_client_unreliable_kvraft);
    RUN_TEST(test_lock_2_clients_unreliable_kvraft);
    RUN_TEST(test_lock_32_clients_reliable_kvraft);
    RUN_TEST(test_lock_8_clients_unreliable_kvraft);
    RUN_TEST(test_lock_with_leader_failure_kvraft);
    RUN_TEST(test_lock_with_crash_kvraft);

    if (test_common_ran_count() == 0) {
        fprintf(stderr, "no tests matched filter \"%s\"\n", argv[1]);
        return 1;
    }
    printf("\nAll kvraft tests passed!\n");
    return 0;
}
