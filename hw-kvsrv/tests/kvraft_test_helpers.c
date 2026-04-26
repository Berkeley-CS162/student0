#include "tests/kv_test_helpers.h"

#include <stdlib.h>

kvraft_client_t *test_cluster_make_kvraft_client(test_cluster_t *tc) {
    int id = next_client_id();
    rpc_client_t **servers = malloc((size_t)tc->n * sizeof(rpc_client_t *));
    for (int i = 0; i < tc->n; i++) {
        char *url = server_proxy_url(tc->proxies[i]);
        servers[i] = rpc_client_new(url, id);
        free(url);
    }
    kvraft_client_t *ck = kvraft_client_new(servers, tc->n);
    free(servers);
    return ck;
}

kvraft_client_t *test_cluster_make_kvraft_client_with_id(test_cluster_t *tc,
                                                         int *out_id) {
    int id = next_client_id();
    *out_id = id;

    rpc_client_t **servers = malloc((size_t)tc->n * sizeof(rpc_client_t *));
    for (int i = 0; i < tc->n; i++) {
        char *url = server_proxy_url(tc->proxies[i]);
        servers[i] = rpc_client_new(url, id);
        free(url);
    }
    kvraft_client_t *ck = kvraft_client_new(servers, tc->n);
    free(servers);
    return ck;
}

int test_cluster_find_leader(test_cluster_t *tc) {
    for (int i = 0; i < tc->n; i++) {
        uint64_t term;
        bool is_leader;
        if (test_cluster_get_state_on(tc, i, &term, &is_leader) && is_leader)
            return i;
    }
    return -1;
}
