#include "tests/network_proxy.h"
#include "lib/.internal/pool.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>

static double random_double(void) {
    return drand48();
}

enum rpc_fate {
    FATE_DELIVER = 0,
    FATE_DROP_REQUEST = 1,
    FATE_DROP_REPLY = 2,
};

static enum rpc_fate resolve_fate(network_config_t config) {
    if (random_double() < config.request_drop_rate)
        return FATE_DROP_REQUEST;
    if (random_double() < config.reply_drop_rate)
        return FATE_DROP_REPLY;
    return FATE_DELIVER;
}

static void random_delay(network_config_t config) {
    if (config.min_delay_ms == 0 && config.max_delay_ms == 0)
        return;

    uint64_t ms;
    if (config.min_delay_ms >= config.max_delay_ms) {
        ms = config.min_delay_ms;
    } else {
        uint64_t range = config.max_delay_ms - config.min_delay_ms + 1;
        ms = config.min_delay_ms + ((uint64_t)rand() % range);
    }

    if (ms > 0)
        usleep((useconds_t)(ms * 1000));
}

static char *proxy_dispatch(void *ctx, const char *method, const char *body,
                            int client_id) {
    server_proxy_t *proxy = (server_proxy_t *)ctx;

    pthread_mutex_lock(&proxy->mu);

    if (client_id >= 0 && client_id < 4096 && proxy->blocked_clients[client_id]) {
        pthread_mutex_unlock(&proxy->mu);
        return NULL;
    }

    network_config_t config = proxy->config;
    char *child_url = proxy->child_url ? strdup(proxy->child_url) : NULL;

    pthread_mutex_unlock(&proxy->mu);

    random_delay(config);

    enum rpc_fate fate = resolve_fate(config);
    if (fate == FATE_DROP_REQUEST) {
        free(child_url);
        return NULL;
    }

    if (!child_url)
        return NULL;

    rpc_client_t *child = rpc_client_new(child_url, -1);
    char *reply = rpc_call(child, method, body);
    rpc_client_free(child);
    free(child_url);

    if (!reply)
        return NULL;

    random_delay(config);

    pthread_mutex_lock(&proxy->mu);
    bool blocked = (client_id >= 0 && client_id < 4096 &&
                    proxy->blocked_clients[client_id]);
    pthread_mutex_unlock(&proxy->mu);

    if (blocked) {
        free(reply);
        return NULL;
    }

    if (fate == FATE_DROP_REPLY) {
        free(reply);
        return NULL;
    }

    return reply;
}

server_proxy_t *server_proxy_new(network_config_t config) {
    POOL_ENSURE_READY();

    static int seeded = 0;
    if (!seeded) {
        srand48((long)time(NULL) ^ (long)getpid());
        srand((unsigned)(time(NULL) ^ getpid()));
        seeded = 1;
    }

    server_proxy_t *p = calloc(1, sizeof(server_proxy_t));
    pthread_mutex_init(&p->mu, NULL);
    p->config = config;
    p->child_url = NULL;
    memset(p->blocked_clients, 0, sizeof(p->blocked_clients));

    p->listen_port = rpc_server_start(p, proxy_dispatch);

    return p;
}

void server_proxy_free(server_proxy_t *p) {
    if (!p) return;
    pthread_mutex_destroy(&p->mu);
    free(p->child_url);
    free(p);
}

void server_proxy_set_child_url(server_proxy_t *p, const char *url) {
    pthread_mutex_lock(&p->mu);
    free(p->child_url);
    p->child_url = url ? strdup(url) : NULL;
    pthread_mutex_unlock(&p->mu);
}

char *server_proxy_url(server_proxy_t *p) {
    char buf[128];
    snprintf(buf, sizeof(buf), "http://127.0.0.1:%d", p->listen_port);
    return strdup(buf);
}

void server_proxy_block_client(server_proxy_t *p, int client_id) {
    if (client_id < 0 || client_id >= 4096) return;
    pthread_mutex_lock(&p->mu);
    p->blocked_clients[client_id] = true;
    pthread_mutex_unlock(&p->mu);
}

void server_proxy_unblock_client(server_proxy_t *p, int client_id) {
    if (client_id < 0 || client_id >= 4096) return;
    pthread_mutex_lock(&p->mu);
    p->blocked_clients[client_id] = false;
    pthread_mutex_unlock(&p->mu);
}

void server_proxy_set_reliable(server_proxy_t *p) {
    pthread_mutex_lock(&p->mu);
    p->config = network_config_reliable();
    pthread_mutex_unlock(&p->mu);
}
