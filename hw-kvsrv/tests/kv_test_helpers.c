#include "tests/kv_test_helpers.h"

#include <stdlib.h>
#include <string.h>

version_t test_put_at_least_once(kv_client_t *ck, const char *key,
                                 const char *val, version_t ver) {
    while (1) {
        kv_err_t err = ck->put(ck->ctx, key, val, ver);
        if (err == KV_OK)
            return ver + 1;

        if (err == KV_MAYBE) {
            char *got_val = NULL;
            version_t got_ver = 0;
            kv_err_t gerr;
            ck->get(ck->ctx, key, &got_val, &got_ver, &gerr);
            if (got_val && strcmp(got_val, val) == 0 && got_ver == ver + 1) {
                free(got_val);
                return got_ver;
            }
            free(got_val);
        } else if (err == KV_VERSION || err == KV_NO_KEY) {
            char *got_val = NULL;
            version_t got_ver = 0;
            kv_err_t gerr;
            ck->get(ck->ctx, key, &got_val, &got_ver, &gerr);
            if (got_val && strcmp(got_val, val) == 0) {
                version_t rv = got_ver;
                free(got_val);
                return rv;
            }
            ver = got_ver;
            free(got_val);
        } else {
            ASSERT(0, "put(%s, %s, %llu) unexpected: %s",
                   key, val, (unsigned long long)ver, kv_err_to_str(err));
        }
    }
}

void test_check_get(kv_client_t *ck, const char *key,
                    const char *expected_val, version_t expected_ver) {
    char *val = NULL;
    version_t ver = 0;
    kv_err_t err;

    ck->get(ck->ctx, key, &val, &ver, &err);
    ASSERT(err == KV_OK, "get(%s) returned %s", key, kv_err_to_str(err));
    ASSERT(strcmp(val, expected_val) == 0,
           "get(%s) value: got '%s' want '%s'", key, val, expected_val);
    ASSERT(ver == expected_ver,
           "get(%s) version: got %llu want %llu",
           key, (unsigned long long)ver, (unsigned long long)expected_ver);
    free(val);
}
