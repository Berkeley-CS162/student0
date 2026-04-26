#ifndef KV_TEST_HELPERS_H
#define KV_TEST_HELPERS_H

#include "tests/test_common.h"
#include "src/kv_single/client.h"
#include "src/kv_raft/client.h"
#include "lib/kv_types.h"

typedef struct {
    kv_client_t client;
    void *raw;
} test_kv_client_ref_t;

typedef test_kv_client_ref_t (*test_kv_client_factory_t)(void *ctx);
typedef void (*test_kv_client_destructor_t)(void *raw);

kv_single_client_t *test_kv_make_client(test_kv_t *ts);
kvraft_client_t *test_cluster_make_kvraft_client(test_cluster_t *tc);
kvraft_client_t *test_cluster_make_kvraft_client_with_id(test_cluster_t *tc,
                                                         int *out_id);

int test_cluster_find_leader(test_cluster_t *tc);

version_t test_put_at_least_once(kv_client_t *ck, const char *key,
                                 const char *val, version_t ver);
void test_check_get(kv_client_t *ck, const char *key,
                    const char *expected_val, version_t expected_ver);

#endif
