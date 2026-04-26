CC = cc
CFLAGS = -std=c11 -D_GNU_SOURCE -Wall -Wextra -Wno-unused-parameter -pthread -I. -g -O2
LDFLAGS = -pthread

LIB_SRCS = lib/cjson/cJSON.c lib/list.c lib/hash.c lib/persister.c lib/kv_types.c lib/rpc.c
KV_SINGLE_SRCS = src/kv_single/server.c src/kv_single/client.c
LOCK_SRC = src/lock.c
RAFT_SRCS = src/kv_raft/raft.c src/kv_raft/rsm.c src/kv_raft/client.c
TEST_SRCS = tests/network_proxy.c tests/test_common.c tests/kv_test_helpers.c
KVSRV_TEST_SRCS = tests/kv_single_test_helpers.c tests/lock_test_helpers.c
KVRAFT_TEST_SRCS = tests/kvraft_test_helpers.c tests/lock_test_helpers.c
RAFT_TEST_SRCS = tests/raft_test_helpers.c

ALL_STUDENT = $(KV_SINGLE_SRCS) $(LOCK_SRC) $(RAFT_SRCS)

.PHONY: all clean

all: kv_server raft_server raft_test_server test_kvsrv test_raft test_kvraft

kv_server: bin/kv_server.c $(LIB_SRCS) $(KV_SINGLE_SRCS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

raft_server: bin/raft_server.c $(LIB_SRCS) $(ALL_STUDENT)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

raft_test_server: tests/raft_test_server.c $(LIB_SRCS) $(ALL_STUDENT)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

test_kvsrv: tests/kvsrv_test.c $(LIB_SRCS) $(KV_SINGLE_SRCS) $(LOCK_SRC) $(TEST_SRCS) $(KVSRV_TEST_SRCS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

test_raft: tests/raft_test.c $(LIB_SRCS) $(ALL_STUDENT) $(TEST_SRCS) $(RAFT_TEST_SRCS) | raft_test_server
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

test_kvraft: tests/kvraft_test.c $(LIB_SRCS) $(ALL_STUDENT) $(TEST_SRCS) $(KVRAFT_TEST_SRCS) | raft_test_server
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f kv_server raft_server raft_test_server test_kvsrv test_raft test_kvraft
