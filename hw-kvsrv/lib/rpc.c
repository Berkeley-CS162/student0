#include "rpc.h"

#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct {
    int server_fd;
    void *ctx;
    rpc_dispatch_fn dispatch;
} server_args_t;

typedef struct {
    int client_fd;
    void *ctx;
    rpc_dispatch_fn dispatch;
} handler_args_t;

static void *handler_thread(void *arg) {
    handler_args_t *ha = arg;
    int fd = ha->client_fd;
    void *ctx = ha->ctx;
    rpc_dispatch_fn dispatch = ha->dispatch;
    free(ha);

    char buf[65536];
    int total = 0;
    int header_end = -1;
    int content_length = 0;

    while (total < (int)sizeof(buf) - 1) {
        int n = (int)read(fd, buf + total, sizeof(buf) - 1 - total);
        if (n <= 0) break;
        total += n;
        buf[total] = '\0';

        if (header_end < 0) {
            char *sep = strstr(buf, "\r\n\r\n");
            if (sep) {
                header_end = (int)(sep - buf) + 4;
                char *cl = strstr(buf, "Content-Length:");
                if (!cl) cl = strstr(buf, "content-length:");
                if (cl) content_length = atoi(cl + 15);
            }
        }

        if (header_end >= 0 && total >= header_end + content_length)
            break;
    }

    if (header_end < 0) {
        close(fd);
        return NULL;
    }

    char method[256] = {0};
    if (strncmp(buf, "POST /", 6) == 0) {
        char *end = strstr(buf + 6, " ");
        if (end) {
            int len = (int)(end - (buf + 6));
            if (len > (int)sizeof(method) - 1) len = (int)sizeof(method) - 1;
            memcpy(method, buf + 6, len);
            method[len] = '\0';
        }
    }

    int client_id = -1;
    char *cid = strstr(buf, "X-Client-Id:");
    if (!cid) cid = strstr(buf, "x-client-id:");
    if (cid) client_id = atoi(cid + 12);

    char *body = buf + header_end;

    char *resp = dispatch(ctx, method, body, client_id);

    if (!resp) {
        const char *err = "HTTP/1.1 503 Service Unavailable\r\n"
                          "Content-Length: 0\r\nConnection: close\r\n\r\n";
        write(fd, err, strlen(err));
    } else {
        int rlen = (int)strlen(resp);
        char header[256];
        int hlen = snprintf(header, sizeof(header),
            "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", rlen);
        write(fd, header, hlen);
        write(fd, resp, rlen);
        free(resp);
    }

    close(fd);
    return NULL;
}

static void *accept_thread(void *arg) {
    server_args_t *sa = arg;

    while (1) {
        struct sockaddr_in addr;
        socklen_t alen = sizeof(addr);
        int cfd = accept(sa->server_fd, (struct sockaddr *)&addr, &alen);
        if (cfd < 0) break;

        handler_args_t *ha = malloc(sizeof(handler_args_t));
        ha->client_fd = cfd;
        ha->ctx = sa->ctx;
        ha->dispatch = sa->dispatch;

        pthread_t tid;
        pthread_create(&tid, NULL, handler_thread, ha);
        pthread_detach(tid);
    }

    /* sa lives for the process lifetime. */
    return NULL;
}

uint16_t rpc_server_start(void *ctx, rpc_dispatch_fn dispatch) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = 0;

    bind(fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(fd, 128);

    socklen_t alen = sizeof(addr);
    getsockname(fd, (struct sockaddr *)&addr, &alen);
    uint16_t port = ntohs(addr.sin_port);

    server_args_t *sa = malloc(sizeof(server_args_t));
    sa->server_fd = fd;
    sa->ctx = ctx;
    sa->dispatch = dispatch;

    pthread_t tid;
    pthread_create(&tid, NULL, accept_thread, sa);
    pthread_detach(tid);

    return port;
}

rpc_client_t *rpc_client_new(const char *url, int client_id) {
    rpc_client_t *c = malloc(sizeof(rpc_client_t));
    memset(c, 0, sizeof(*c));
    c->client_id = client_id;

    const char *p = url;
    if (strncmp(p, "http://", 7) == 0) p += 7;

    const char *colon = strchr(p, ':');
    if (colon) {
        int hlen = (int)(colon - p);
        if (hlen > (int)sizeof(c->host) - 1) hlen = (int)sizeof(c->host) - 1;
        memcpy(c->host, p, hlen);
        c->host[hlen] = '\0';
        c->port = (uint16_t)atoi(colon + 1);
    } else {
        snprintf(c->host, sizeof(c->host), "%s", p);
        c->port = 80;
    }

    return c;
}

void rpc_client_free(rpc_client_t *c) {
    free(c);
}

char *rpc_call(rpc_client_t *c, const char *method, const char *body) {
    return rpc_call_timeout(c, method, body, 5);
}

char *rpc_call_timeout(rpc_client_t *c, const char *method, const char *body, int timeout_sec) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return NULL;

    if (timeout_sec > 0) {
        struct timeval tv;
        tv.tv_sec = timeout_sec;
        tv.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(c->host);
    addr.sin_port = htons(c->port);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return NULL;
    }

    int body_len = body ? (int)strlen(body) : 0;
    char header[512];
    int hlen = snprintf(header, sizeof(header),
        "POST /%s HTTP/1.1\r\n"
        "Host: %s\r\n"
        "X-Client-Id: %d\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n"
        "\r\n",
        method, c->host, c->client_id, body_len);

    if (write(fd, header, hlen) != hlen) {
        close(fd);
        return NULL;
    }
    if (body_len > 0) {
        if (write(fd, body, body_len) != body_len) {
            close(fd);
            return NULL;
        }
    }

    char buf[65536];
    int total = 0;
    while (total < (int)sizeof(buf) - 1) {
        int n = (int)read(fd, buf + total, sizeof(buf) - 1 - total);
        if (n <= 0) break;
        total += n;
    }
    buf[total] = '\0';
    close(fd);

    if (total == 0) return NULL;

    if (!strstr(buf, "200")) return NULL;

    char *sep = strstr(buf, "\r\n\r\n");
    if (!sep) return NULL;

    return strdup(sep + 4);
}
