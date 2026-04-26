#include "src/kv_raft/rsm.h"
#include "lib/rpc.h"
#include "lib/persister.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>

int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr, "usage: raft_server me n_peers persist_path peer_url...\n");
        return 1;
    }

    int me = atoi(argv[1]);
    int n_peers = atoi(argv[2]);
    const char *persist_path = argv[3];
    srand((unsigned)(time(NULL) ^ getpid() ^ (me * 2654435761u)));
    signal(SIGPIPE, SIG_IGN);

    persister_t persister;
    persister_init(&persister, persist_path);

    rpc_client_t **peers = malloc(n_peers * sizeof(rpc_client_t *));
    for (int i = 0; i < n_peers; i++) {
        peers[i] = rpc_client_new(argv[4 + i], me);
    }

    rsm_t *rsm = rsm_new(me, peers, n_peers, &persister);

    uint16_t port = rpc_server_start(rsm, rsm_dispatch);

    dprintf(3, "%d", port);
    close(3);

    while (1) pause();

    rsm_free(rsm);
    for (int i = 0; i < n_peers; i++) rpc_client_free(peers[i]);
    free(peers);
    return 0;
}
