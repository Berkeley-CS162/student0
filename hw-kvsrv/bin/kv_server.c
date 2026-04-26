#include "src/kv_single/server.h"
#include "lib/rpc.h"
#include <stdio.h>
#include <unistd.h>
#include <signal.h>

int main(void) {
    signal(SIGPIPE, SIG_IGN);

    kv_server_t *server = kv_server_new();
    uint16_t port = rpc_server_start(server, kv_server_dispatch);

    dprintf(3, "%d", port);
    close(3);

    while (1) pause();
    return 0;
}
