#include "src/kv_raft/rsm.h"
#include "lib/cjson/cJSON.h"
#include "lib/rpc.h"
#include "lib/persister.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>

typedef struct {
    rsm_t *rsm;
    raft_t *raft;
} raft_test_server_t;

static char *raft_test_dispatch(void *ctx, const char *method,
                                const char *body, int client_id) {
    raft_test_server_t *s = ctx;

    if (strcmp(method, "_test/get_state") == 0) {
        uint64_t term;
        bool is_leader;
        raft_get_state(s->raft, &term, &is_leader);
        char buf[128];
        snprintf(buf, sizeof(buf), "[%llu,%s]",
                 (unsigned long long)term, is_leader ? "true" : "false");
        return strdup(buf);
    }
    if (strcmp(method, "_test/get_committed") == 0) {
        cJSON *j = cJSON_Parse(body);
        int index = j ? j->valueint : 0;
        cJSON_Delete(j);
        char *cmd = raft_get_committed(s->raft, index);
        if (cmd) {
            cJSON *res = cJSON_CreateString(cmd);
            char *reply = cJSON_PrintUnformatted(res);
            cJSON_Delete(res);
            free(cmd);
            return reply;
        }
        return strdup("null");
    }
    if (strcmp(method, "_test/submit") == 0) {
        cJSON *j = cJSON_Parse(body);
        const char *cmd = (j && cJSON_IsString(j)) ? j->valuestring : "";
        uint64_t term;
        bool is_leader;
        int index = raft_submit(s->raft, cmd, &term, &is_leader);
        cJSON_Delete(j);
        if (index == 0 || !is_leader)
            return strdup("null");
        char buf[128];
        snprintf(buf, sizeof(buf), "[%d,%llu]", index, (unsigned long long)term);
        return strdup(buf);
    }

    return rsm_dispatch(s->rsm, method, body, client_id);
}

int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr, "usage: raft_test_server me n_peers persist_path peer_url...\n");
        return 1;
    }

    int me = atoi(argv[1]);
    int n_peers = atoi(argv[2]);
    const char *persist_path = argv[3];
    srand((unsigned)(time(NULL) ^ getpid() ^ (me * 2654435761u)));
    signal(SIGPIPE, SIG_IGN);

    persister_t *persister = malloc(sizeof(persister_t));
    persister_init(persister, persist_path);

    rpc_client_t **peers = malloc(n_peers * sizeof(rpc_client_t *));
    for (int i = 0; i < n_peers; i++)
        peers[i] = rpc_client_new(argv[4 + i], me);

    raft_test_server_t *server = calloc(1, sizeof(raft_test_server_t));
    server->rsm = rsm_new(me, peers, n_peers, persister);
    server->raft = rsm_raft(server->rsm);

    uint16_t port = rpc_server_start(server, raft_test_dispatch);
    dprintf(3, "%d", port);
    close(3);

    while (1) pause();
    return 0;
}
