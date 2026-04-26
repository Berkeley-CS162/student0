#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include "tests/network_proxy.h"
#include "lib/rpc.h"
#include "lib/kv_types.h"
#include <sys/types.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


typedef struct {
    server_proxy_t *proxy;
    pid_t child_pid;
} test_kv_t;

test_kv_t *test_kv_new(network_config_t config);
void test_kv_free(test_kv_t *t);


typedef struct {
    int n;
    server_proxy_t **proxies;
    pid_t *child_pids;
    char **child_urls;
    char persist_dir[256];
    bool is_raft_test;
} test_cluster_t;

test_cluster_t *test_cluster_new(int n, network_config_t config);
void test_cluster_free(test_cluster_t *tc);

void test_cluster_disconnect(test_cluster_t *tc, int i);
void test_cluster_reconnect(test_cluster_t *tc, int i);
void test_cluster_partition(test_cluster_t *tc, int *group_a, int na,
                            int *group_b, int nb);
void test_cluster_heal(test_cluster_t *tc);

void test_cluster_kill(test_cluster_t *tc, int i);
void test_cluster_restart(test_cluster_t *tc, int i);
bool test_cluster_is_alive(test_cluster_t *tc, int i);

rpc_client_t *test_cluster_child_endpoint(test_cluster_t *tc, int i);

bool test_cluster_get_state_on(test_cluster_t *tc, int i,
                               uint64_t *term, bool *is_leader);

char *rand_string(int n);
int next_client_id(void);
void test_common_start_timeout(int seconds, const char *test_name);
void test_common_clear_timeout(void);

void test_common_set_filter(const char *filter);
bool test_common_should_run(const char *name);
int  test_common_ran_count(void);


#ifndef TEST_TIMEOUT_SEC
#define TEST_TIMEOUT_SEC 900
#endif

#define RUN_TEST(fn) do { \
    if (test_common_should_run(#fn)) { \
        printf("=== RUN   %s\n", #fn); \
        fflush(stdout); \
        test_common_start_timeout(TEST_TIMEOUT_SEC, #fn); \
        fn(); \
        test_common_clear_timeout(); \
        printf("--- PASS  %s\n", #fn); \
        fflush(stdout); \
    } \
} while(0)

#define ASSERT(cond, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
        fprintf(stderr, __VA_ARGS__); \
        fprintf(stderr, "\n"); \
        fflush(stderr); \
        exit(1); \
    } \
} while(0)

#define ASSERT_EQ_INT(a, b, ...) ASSERT((a) == (b), __VA_ARGS__)
#define ASSERT_EQ_STR(a, b, ...) ASSERT(strcmp((a), (b)) == 0, __VA_ARGS__)

#endif
