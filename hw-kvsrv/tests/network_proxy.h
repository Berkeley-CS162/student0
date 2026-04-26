#ifndef NETWORK_PROXY_H
#define NETWORK_PROXY_H

#include "lib/rpc.h"
#include <stdbool.h>
#include <pthread.h>

typedef struct {
    double request_drop_rate;
    double reply_drop_rate;
    uint64_t min_delay_ms;
    uint64_t max_delay_ms;
} network_config_t;

static inline network_config_t network_config_reliable(void) {
    return (network_config_t){0.0, 0.0, 0, 0};
}

static inline network_config_t network_config_unreliable(void) {
    return (network_config_t){0.5, 0.5, 1, 5};
}

typedef struct server_proxy {
    char *child_url;
    uint16_t listen_port;
    network_config_t config;
    bool blocked_clients[4096];
    pthread_mutex_t mu;
} server_proxy_t;

server_proxy_t *server_proxy_new(network_config_t config);
void server_proxy_free(server_proxy_t *p);
void server_proxy_set_child_url(server_proxy_t *p, const char *url);
char *server_proxy_url(server_proxy_t *p);
void server_proxy_block_client(server_proxy_t *p, int client_id);
void server_proxy_unblock_client(server_proxy_t *p, int client_id);
void server_proxy_set_reliable(server_proxy_t *p);

#endif
