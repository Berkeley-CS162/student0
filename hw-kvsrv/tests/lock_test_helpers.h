#ifndef LOCK_TEST_HELPERS_H
#define LOCK_TEST_HELPERS_H

#include "tests/kv_test_helpers.h"

void test_run_lock_clients(int n_clients, int duration_sec,
                           test_kv_client_factory_t make_client,
                           test_kv_client_destructor_t free_client,
                           void *ctx);

#endif
