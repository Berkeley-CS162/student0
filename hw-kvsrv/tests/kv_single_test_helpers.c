#include "tests/kv_test_helpers.h"

#include <stdlib.h>

kv_single_client_t *test_kv_make_client(test_kv_t *ts) {
    int id = next_client_id();
    char *url = server_proxy_url(ts->proxy);
    rpc_client_t *ep = rpc_client_new(url, id);
    free(url);
    return kv_single_client_new(ep);
}
