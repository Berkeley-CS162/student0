#ifndef KV_TYPES_H
#define KV_TYPES_H

#include <stdint.h>

/* Version 0 means the key is missing. */
typedef uint64_t version_t;

typedef enum {
    KV_OK = 0,
    KV_NO_KEY,
    KV_VERSION,
    KV_MAYBE,
    KV_WRONG_LEADER
} kv_err_t;

/* String conversions. */
const char *kv_err_to_str(kv_err_t err);
kv_err_t kv_err_from_str(const char *s);

typedef struct kv_client {
    void *ctx;
    void (*get)(void *ctx, const char *key,
                char **out_value, version_t *out_version, kv_err_t *out_err);
    kv_err_t (*put)(void *ctx, const char *key, const char *value, version_t ver);
} kv_client_t;

#endif
