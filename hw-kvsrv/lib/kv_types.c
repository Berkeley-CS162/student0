#include "kv_types.h"
#include <string.h>

const char *kv_err_to_str(kv_err_t err) {
    if (err == KV_OK)           return "OK";
    if (err == KV_NO_KEY)       return "NoKey";
    if (err == KV_VERSION)      return "Version";
    if (err == KV_MAYBE)        return "Maybe";
    if (err == KV_WRONG_LEADER) return "WrongLeader";
    return "Unknown";
}

kv_err_t kv_err_from_str(const char *s) {
    if (strcmp(s, "OK") == 0)           return KV_OK;
    if (strcmp(s, "NoKey") == 0)        return KV_NO_KEY;
    if (strcmp(s, "Version") == 0)      return KV_VERSION;
    if (strcmp(s, "Maybe") == 0)        return KV_MAYBE;
    if (strcmp(s, "WrongLeader") == 0)  return KV_WRONG_LEADER;
    return KV_OK;
}
