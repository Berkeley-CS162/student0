#include "src/kv_single/server.h"
#include "lib/hash.h"
#include "lib/kv_types.h"
#include "lib/cjson/cJSON.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>

typedef struct { char *key; } get_args_t;
typedef struct { char *key; char *value; version_t version; } put_args_t;

static bool parse_get_args(cJSON *j, get_args_t *out) {
    cJSON *k = cJSON_GetObjectItemCaseSensitive(j, "key");
    if (!cJSON_IsString(k)) return false;
    out->key = k->valuestring;
    return true;
}

static bool parse_put_args(cJSON *j, put_args_t *out) {
    cJSON *k = cJSON_GetObjectItemCaseSensitive(j, "key");
    cJSON *v = cJSON_GetObjectItemCaseSensitive(j, "value");
    cJSON *ver = cJSON_GetObjectItemCaseSensitive(j, "version");
    if (!cJSON_IsString(k) || !cJSON_IsString(v) || !cJSON_IsNumber(ver)) return false;
    out->key = k->valuestring;
    out->value = v->valuestring;
    out->version = (version_t)ver->valuedouble;
    return true;
}

static char *make_get_reply(const char *value, version_t version, kv_err_t err) {
    cJSON *j = cJSON_CreateObject();
    cJSON_AddStringToObject(j, "value", value ? value : "");
    cJSON_AddNumberToObject(j, "version", (double)version);
    cJSON_AddStringToObject(j, "err", kv_err_to_str(err));
    char *s = cJSON_PrintUnformatted(j);
    cJSON_Delete(j);
    return s;
}

static char *make_put_reply(kv_err_t err) {
    cJSON *j = cJSON_CreateObject();
    cJSON_AddStringToObject(j, "err", kv_err_to_str(err));
    char *s = cJSON_PrintUnformatted(j);
    cJSON_Delete(j);
    return s;
}

/* TODO: define the stored key/value entries. */

struct kv_server {
    /* TODO: add hash table and lock fields. */
};

kv_server_t *kv_server_new(void) {
    /* TODO: initialize server state. */
    return NULL;
}

void kv_server_free(kv_server_t *s) {
    /* TODO: free server state. */
}

static char *handle_get(kv_server_t *s, cJSON *args) {
    get_args_t a = {0};
    if (!parse_get_args(args, &a))
        return make_get_reply("", 0, KV_NO_KEY);

    /* TODO: return the value and version for a.key. */

    return make_get_reply("", 0, KV_NO_KEY);
}

static char *handle_put(kv_server_t *s, cJSON *args) {
    put_args_t a = {0};
    if (!parse_put_args(args, &a))
        return make_put_reply(KV_NO_KEY);

    /* TODO: apply versioned put semantics. */

    return make_put_reply(KV_OK);
}

char *kv_server_dispatch(void *ctx, const char *method, const char *body, int client_id) {
    kv_server_t *s = (kv_server_t *)ctx;
    (void)client_id;

    cJSON *args = cJSON_Parse(body);
    if (!args) return strdup("{\"err\":\"ParseError\"}");

    char *result = NULL;
    if (strcmp(method, "get") == 0) {
        result = handle_get(s, args);
    } else if (strcmp(method, "put") == 0) {
        result = handle_put(s, args);
    } else {
        result = strdup("{\"err\":\"UnknownMethod\"}");
    }

    cJSON_Delete(args);
    return result;
}
