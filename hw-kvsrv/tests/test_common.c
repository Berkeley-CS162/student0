#include "tests/test_common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <signal.h>
#include <time.h>
#include <limits.h>
#include <stdatomic.h>
#include <sys/wait.h>
#include <sys/stat.h>

static _Atomic int g_next_client_id = 100;
static const char *g_current_test_name = NULL;
static const char *g_test_filter = NULL;
static int g_tests_ran = 0;

int next_client_id(void) {
    return atomic_fetch_add(&g_next_client_id, 1);
}

void test_common_set_filter(const char *filter) {
    g_test_filter = (filter && filter[0]) ? filter : NULL;
}

bool test_common_should_run(const char *name) {
    if (g_test_filter && !strstr(name, g_test_filter))
        return false;
    g_tests_ran++;
    return true;
}

int test_common_ran_count(void) {
    return g_tests_ran;
}

static void timeout_handler(int sig) {
    (void)sig;
    const char *name = g_current_test_name ? g_current_test_name : "(unknown)";
    fprintf(stderr, "\nFAIL timeout: %s exceeded %d seconds\n",
            name, TEST_TIMEOUT_SEC);
    fflush(stderr);
    _exit(124);
}

void test_common_start_timeout(int seconds, const char *test_name) {
    g_current_test_name = test_name;
    signal(SIGALRM, timeout_handler);
    alarm((unsigned)seconds);
}

void test_common_clear_timeout(void) {
    alarm(0);
    g_current_test_name = NULL;
}

char *rand_string(int n) {
    char *s = malloc(n + 1);
    for (int i = 0; i < n; i++)
        s[i] = 'a' + (rand() % 26);
    s[n] = '\0';
    return s;
}

static pid_t spawn_kv_child(uint16_t *out_port) {
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("pipe");
        exit(1);
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(1);
    }

    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], 3);
        close(pipefd[1]);
        execl("./kv_server", "kv_server", NULL);
        perror("execl kv_server");
        _exit(1);
    }

    close(pipefd[1]);
    char buf[32];
    int n = (int)read(pipefd[0], buf, sizeof(buf) - 1);
    close(pipefd[0]);

    if (n <= 0) {
        fprintf(stderr, "kv_server child died during startup\n");
        waitpid(pid, NULL, 0);
        exit(1);
    }

    buf[n] = '\0';
    *out_port = (uint16_t)atoi(buf);
    return pid;
}

static pid_t spawn_raft_child(int me, int n_peers, const char *persist_path,
                              char **peer_urls, uint16_t *out_port) {
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("pipe");
        exit(1);
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        exit(1);
    }

    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], 3);
        close(pipefd[1]);

        int argc = 4 + n_peers;
        char **argv = malloc((argc + 1) * sizeof(char *));
        argv[0] = "raft_test_server";

        char me_str[16], n_str[16];
        snprintf(me_str, sizeof(me_str), "%d", me);
        snprintf(n_str, sizeof(n_str), "%d", n_peers);

        argv[1] = me_str;
        argv[2] = n_str;
        argv[3] = (char *)persist_path;
        for (int i = 0; i < n_peers; i++)
            argv[4 + i] = peer_urls[i];
        argv[argc] = NULL;

        execv("./raft_test_server", argv);
        perror("execv raft_test_server");
        _exit(1);
    }

    close(pipefd[1]);
    char buf[32];
    int n = (int)read(pipefd[0], buf, sizeof(buf) - 1);
    close(pipefd[0]);

    if (n <= 0) {
        fprintf(stderr, "raft_server child %d died during startup\n", me);
        *out_port = 0;
        return pid;
    }

    buf[n] = '\0';
    *out_port = (uint16_t)atoi(buf);
    return pid;
}

test_kv_t *test_kv_new(network_config_t config) {
    test_kv_t *t = calloc(1, sizeof(test_kv_t));

    t->proxy = server_proxy_new(config);

    uint16_t port;
    t->child_pid = spawn_kv_child(&port);

    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d", port);
    server_proxy_set_child_url(t->proxy, url);

    return t;
}

void test_kv_free(test_kv_t *t) {
    if (!t) return;
    if (t->child_pid > 0) {
        kill(t->child_pid, SIGKILL);
        waitpid(t->child_pid, NULL, 0);
    }
    server_proxy_free(t->proxy);
    free(t);
}

test_cluster_t *test_cluster_new(int n, network_config_t config) {
    test_cluster_t *tc = calloc(1, sizeof(test_cluster_t));
    tc->n = n;
    tc->is_raft_test = true;

    snprintf(tc->persist_dir, sizeof(tc->persist_dir),
             "/tmp/raft-mp-%d-%ld", getpid(), (long)time(NULL));
    mkdir(tc->persist_dir, 0755);

    tc->proxies = malloc(n * sizeof(server_proxy_t *));
    for (int i = 0; i < n; i++)
        tc->proxies[i] = server_proxy_new(config);

    char **proxy_urls = malloc(n * sizeof(char *));
    for (int i = 0; i < n; i++)
        proxy_urls[i] = server_proxy_url(tc->proxies[i]);

    tc->child_pids = malloc(n * sizeof(pid_t));
    tc->child_urls = malloc(n * sizeof(char *));

    for (int i = 0; i < n; i++) {
        char persist_path[512];
        snprintf(persist_path, sizeof(persist_path),
                 "%s/node-%d.json", tc->persist_dir, i);

        uint16_t port;
        tc->child_pids[i] = spawn_raft_child(i, n, persist_path,
                                              proxy_urls, &port);

        char url[128];
        snprintf(url, sizeof(url), "http://127.0.0.1:%d", port);
        tc->child_urls[i] = strdup(url);

        server_proxy_set_child_url(tc->proxies[i], url);
    }

    for (int i = 0; i < n; i++)
        free(proxy_urls[i]);
    free(proxy_urls);

    return tc;
}

void test_cluster_free(test_cluster_t *tc) {
    if (!tc) return;

    for (int i = 0; i < tc->n; i++) {
        if (tc->child_pids[i] > 0) {
            kill(tc->child_pids[i], SIGKILL);
            waitpid(tc->child_pids[i], NULL, 0);
        }
    }

    for (int i = 0; i < tc->n; i++)
        server_proxy_free(tc->proxies[i]);
    free(tc->proxies);

    for (int i = 0; i < tc->n; i++)
        free(tc->child_urls[i]);
    free(tc->child_urls);

    free(tc->child_pids);

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", tc->persist_dir);
    system(cmd);

    free(tc);
}


void test_cluster_disconnect(test_cluster_t *tc, int i) {
    for (int j = 0; j < tc->n; j++) {
        if (j != i) {
            server_proxy_block_client(tc->proxies[j], i);
            server_proxy_block_client(tc->proxies[i], j);
        }
    }
}

void test_cluster_reconnect(test_cluster_t *tc, int i) {
    for (int j = 0; j < tc->n; j++) {
        if (j != i) {
            server_proxy_unblock_client(tc->proxies[j], i);
            server_proxy_unblock_client(tc->proxies[i], j);
        }
    }
}

void test_cluster_partition(test_cluster_t *tc, int *group_a, int na,
                            int *group_b, int nb) {
    for (int ai = 0; ai < na; ai++) {
        for (int bi = 0; bi < nb; bi++) {
            int a = group_a[ai];
            int b = group_b[bi];
            server_proxy_block_client(tc->proxies[b], a);
            server_proxy_block_client(tc->proxies[a], b);
        }
    }
}

void test_cluster_heal(test_cluster_t *tc) {
    for (int i = 0; i < tc->n; i++) {
        for (int j = 0; j < tc->n; j++) {
            server_proxy_unblock_client(tc->proxies[j], i);
        }
    }
}


void test_cluster_kill(test_cluster_t *tc, int i) {
    if (tc->child_pids[i] > 0) {
        kill(tc->child_pids[i], SIGKILL);
        waitpid(tc->child_pids[i], NULL, 0);
    }
    tc->child_pids[i] = -1;

    free(tc->child_urls[i]);
    tc->child_urls[i] = NULL;

    server_proxy_set_child_url(tc->proxies[i], NULL);
    test_cluster_disconnect(tc, i);
}

void test_cluster_restart(test_cluster_t *tc, int i) {
    char persist_path[512];
    snprintf(persist_path, sizeof(persist_path),
             "%s/node-%d.json", tc->persist_dir, i);

    char **proxy_urls = malloc(tc->n * sizeof(char *));
    for (int j = 0; j < tc->n; j++)
        proxy_urls[j] = server_proxy_url(tc->proxies[j]);

    uint16_t port;
    tc->child_pids[i] = spawn_raft_child(i, tc->n, persist_path,
                                          proxy_urls, &port);

    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d", port);

    free(tc->child_urls[i]);
    tc->child_urls[i] = strdup(url);

    server_proxy_set_child_url(tc->proxies[i], url);
    test_cluster_reconnect(tc, i);

    for (int j = 0; j < tc->n; j++)
        free(proxy_urls[j]);
    free(proxy_urls);
}

bool test_cluster_is_alive(test_cluster_t *tc, int i) {
    return tc->child_pids[i] > 0;
}


rpc_client_t *test_cluster_child_endpoint(test_cluster_t *tc, int i) {
    if (!tc->child_urls[i])
        return NULL;
    return rpc_client_new(tc->child_urls[i], INT_MAX);
}

bool test_cluster_get_state_on(test_cluster_t *tc, int i,
                               uint64_t *term, bool *is_leader) {
    rpc_client_t *ep = test_cluster_child_endpoint(tc, i);
    if (!ep)
        return false;

    char *reply = rpc_call(ep, "_test/get_state", "null");
    rpc_client_free(ep);

    if (!reply)
        return false;

    uint64_t t = 0;
    int leader = 0;

    const char *p = reply;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;
    t = (uint64_t)strtoull(p, NULL, 10);
    while (*p && *p != ',') p++;
    if (*p == ',') p++;
    while (*p == ' ') p++;
    leader = (*p == 't' || *p == 'T') ? 1 : 0;

    free(reply);

    *term = t;
    *is_leader = (leader != 0);
    return true;
}
