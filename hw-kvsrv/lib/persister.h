#ifndef PERSISTER_H
#define PERSISTER_H

/* File-backed persister using atomic rename. */

typedef struct {
    char path[512];
} persister_t;

void persister_init(persister_t *p, const char *path);

/* Replace any previous content. */
void persister_save(persister_t *p, const char *data);

/* Caller frees the returned buffer. */
char *persister_read(persister_t *p);

#endif
